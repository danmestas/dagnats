package natsutil

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// LeaderRebalanceInterval is how often Run checks stream leaders.
// Elections and restarts drift leaders slowly; a minute is soon enough
// to undo one and rare enough to cost nothing.
const LeaderRebalanceInterval = time.Minute

// leaderRebalanceStartDelay is Run's first check after start, once a
// restarted node's streams have elected leaders.
const leaderRebalanceStartDelay = 15 * time.Second

// LeaderMovesPerTickMax bounds the step-downs one Tick sends: each makes
// a stream briefly leaderless, so a cluster whose leaders all drifted is
// moved back a few streams at a time.
const LeaderMovesPerTickMax = 4

// leaderMoveCooldown is how long a stream Tick moved is left alone, so a
// stream whose preferred replica keeps losing its leadership does not
// flap between servers.
const leaderMoveCooldown = 5 * time.Minute

// leaderStepDownTimeout bounds one step-down request.
const leaderStepDownTimeout = 5 * time.Second

// LeaderMove is one step-down Tick sent: a stream's leader, or the
// JetStream meta leader when Meta (Stream is then empty).
type LeaderMove struct {
	Stream, From, To string
	Meta             bool
}

// MetaState is this node's view of the JetStream meta group, read before
// each Tick.
type MetaState struct {
	Leader bool       // this node leads the meta group
	Self   string     // this node's server name
	Peers  []MetaPeer // the other members, as the leader sees them
}

// MetaPeer is one other member of the meta group.
type MetaPeer struct {
	Name             string
	Current, Offline bool
}

// LeaderRebalancer moves replicated streams' leaders onto preferred
// servers. JetStream elects any in-sync replica, so on a cluster with
// one node across a slow link (a laptop 160-700 ms from two nodes 1.4 ms
// apart) leaders drift onto it after restarts and elections, and every
// write to those streams then crosses that link. The meta leader drifts
// the same way, and every stream and consumer create, update and info
// goes through it. It asks the meta leader, then each stream's leader,
// to step down in favour of a current preferred server, which
// nats-server honours. Safe for one goroutine; it acts only on the
// JetStream meta leader, so the cluster has one actor.
type LeaderRebalancer struct {
	nc        *nats.Conn
	system    *nats.Conn // in the system account, for $JS.API.META; nil: streams only
	js        jetstream.JetStream
	preferred []string

	mu          sync.Mutex
	moved       map[string]time.Time // stream -> last move, inside the cooldown
	metaMovedAt time.Time            // last meta move by this node
	next        int                  // rotates moves over the preferred servers
}

// NewLeaderRebalancer returns a rebalancer preferring the given server
// names. With none it never moves anything. system is a connection in
// the system account, which alone may step the meta leader down; nil
// leaves the meta leader where it is.
func NewLeaderRebalancer(nc, system *nats.Conn, preferred []string) *LeaderRebalancer {
	if nc == nil {
		panic("NewLeaderRebalancer: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic("NewLeaderRebalancer: jetstream.New on a live conn: " + err.Error())
	}
	return &LeaderRebalancer{
		nc: nc, system: system, js: js, preferred: slices.Clone(preferred),
		moved: map[string]time.Time{},
	}
}

// Run calls Tick shortly after start and then every
// LeaderRebalanceInterval until ctx ends, reading meta each time.
func (r *LeaderRebalancer) Run(ctx context.Context, meta func() MetaState) {
	if ctx == nil || meta == nil {
		panic("LeaderRebalancer.Run: ctx and meta must be set")
	}
	wait := leaderRebalanceStartDelay
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = LeaderRebalanceInterval
		if _, err := r.Tick(ctx, meta()); err != nil && ctx.Err() == nil {
			slog.Warn("natsutil: stream leader rebalance failed", "error", err)
		}
	}
}

// Tick moves drifted leaders and returns the moves it made. It does
// nothing unless meta.Leader, or with no preferred servers. First the
// meta leader: if this node is not preferred, a preferred peer is
// current and this node has not moved the meta leader within the
// cooldown, it steps down to that peer and the tick ends; the next meta
// leader moves the streams. Otherwise up to LeaderMovesPerTickMax
// streams: one with more than one replica, a leader that is not
// preferred, a current preferred replica and no move within the
// cooldown. A failed step-down is logged and skipped; only a failed
// listing is returned.
func (r *LeaderRebalancer) Tick(ctx context.Context, meta MetaState) ([]LeaderMove, error) {
	if ctx == nil {
		panic("LeaderRebalancer.Tick: ctx must not be nil")
	}
	if !meta.Leader || len(r.preferred) == 0 {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.forgetCooledDown(now)
	if move, ok := r.planMetaMove(meta, now); ok {
		// Stamped before the attempt on purpose: a refused or failed
		// step-down waits out the cooldown too, so a meta leader that
		// cannot move is not asked again every tick.
		r.metaMovedAt = now
		if err := r.stepDownMeta(ctx, move); err != nil {
			slog.Warn("natsutil: meta leader not moved", "from", move.From, "to", move.To,
				"error", err)
		} else {
			slog.Info("natsutil: moved the JetStream meta leader onto a preferred server",
				"from", move.From, "to", move.To)
			r.next++
			return []LeaderMove{move}, nil
		}
	}
	return r.moveStreams(ctx)
}

// moveStreams is Tick's stream pass. Callers hold r.mu.
func (r *LeaderRebalancer) moveStreams(ctx context.Context) ([]LeaderMove, error) {
	if ctx == nil {
		panic("LeaderRebalancer.moveStreams: ctx must not be nil")
	}
	var moves []LeaderMove
	lister := r.js.ListStreams(ctx)
	for info := range lister.Info() {
		if len(moves) >= LeaderMovesPerTickMax {
			continue // drain the lister; its goroutine ends with the listing
		}
		move, ok := r.planMove(info)
		if !ok {
			continue
		}
		if err := r.stepDown(ctx, move); err != nil {
			slog.Warn("natsutil: stream leader not moved", "stream", move.Stream,
				"from", move.From, "to", move.To, "error", err)
			continue
		}
		slog.Info("natsutil: moved stream leader onto a preferred server",
			"stream", move.Stream, "from", move.From, "to", move.To)
		r.moved[move.Stream] = time.Now()
		r.next++
		moves = append(moves, move)
	}
	if err := lister.Err(); err != nil && !errors.Is(err, jetstream.ErrEndOfData) {
		return moves, fmt.Errorf("list streams: %w", err)
	}
	return moves, nil
}

// planMetaMove picks the preferred peer the meta leader (this node)
// should step down to, rotating like planMove. Callers hold r.mu.
func (r *LeaderRebalancer) planMetaMove(meta MetaState, now time.Time) (LeaderMove, bool) {
	if !meta.Leader || r.system == nil || slices.Contains(r.preferred, meta.Self) {
		return LeaderMove{}, false
	}
	if now.Sub(r.metaMovedAt) < leaderMoveCooldown {
		return LeaderMove{}, false
	}
	for i := range r.preferred {
		candidate := r.preferred[(r.next+i)%len(r.preferred)]
		for _, peer := range meta.Peers {
			if peer.Name == candidate && peer.Current && !peer.Offline {
				return LeaderMove{Meta: true, From: meta.Self, To: candidate}, true
			}
		}
	}
	return LeaderMove{}, false
}

// planMove picks the preferred replica info's leader should step down
// to, starting the search at r.next so moves spread over the preferred
// servers. Callers hold r.mu.
func (r *LeaderRebalancer) planMove(info *jetstream.StreamInfo) (LeaderMove, bool) {
	if info == nil || info.Cluster == nil || info.Config.Replicas < 2 {
		return LeaderMove{}, false
	}
	leader := info.Cluster.Leader
	if leader == "" || slices.Contains(r.preferred, leader) {
		return LeaderMove{}, false
	}
	if _, cooling := r.moved[info.Config.Name]; cooling {
		return LeaderMove{}, false
	}
	for i := range r.preferred {
		candidate := r.preferred[(r.next+i)%len(r.preferred)]
		if currentReplica(info.Cluster.Replicas, candidate) {
			return LeaderMove{Stream: info.Config.Name, From: leader, To: candidate}, true
		}
	}
	return LeaderMove{}, false
}

// currentReplica reports whether name is a replica that is online and
// caught up, so it can take the leadership without losing writes.
func currentReplica(replicas []*jetstream.PeerInfo, name string) bool {
	if name == "" {
		panic("currentReplica: name must not be empty")
	}
	for _, peer := range replicas {
		if peer != nil && peer.Name == name {
			return peer.Current && !peer.Offline
		}
	}
	return false
}

// stepDownMeta asks the meta leader to step down in favour of move.To,
// through the system account that owns the meta group's API.
func (r *LeaderRebalancer) stepDownMeta(ctx context.Context, move LeaderMove) error {
	if !move.Meta || move.To == "" || r.system == nil {
		panic("LeaderRebalancer.stepDownMeta: a meta move and a system connection are required")
	}
	return stepDownRequest(ctx, r.system, "$JS.API.META.LEADER.STEPDOWN", move.To)
}

// stepDown asks move.Stream's leader to step down in favour of move.To.
func (r *LeaderRebalancer) stepDown(ctx context.Context, move LeaderMove) error {
	if move.Stream == "" || move.To == "" {
		panic("LeaderRebalancer.stepDown: stream and target must be set")
	}
	return stepDownRequest(ctx, r.nc, "$JS.API.STREAM.LEADER.STEPDOWN."+move.Stream, move.To)
}

// stepDownRequest sends a step-down preferring server on subject and
// reads JetStream's answer.
func stepDownRequest(ctx context.Context, nc *nats.Conn, subject, server string) error {
	if nc == nil || subject == "" || server == "" {
		panic("stepDownRequest: nc, subject and server must be set")
	}
	body, err := json.Marshal(map[string]any{
		"placement": map[string]string{"preferred": server},
	})
	if err != nil {
		panic("stepDownRequest: a placement always encodes")
	}
	ctx, cancel := context.WithTimeout(ctx, leaderStepDownTimeout)
	defer cancel()
	reply, err := nc.RequestWithContext(ctx, subject, body)
	if err != nil {
		return err
	}
	var answer struct {
		Success bool `json:"success"`
		Error   *struct {
			Description string `json:"description"`
		} `json:"error"`
	}
	if err := json.Unmarshal(reply.Data, &answer); err != nil {
		return fmt.Errorf("step-down reply: %w", err)
	}
	if !answer.Success {
		if answer.Error != nil {
			return errors.New(answer.Error.Description)
		}
		return errors.New("step-down not acknowledged")
	}
	return nil
}

// forgetCooledDown drops streams whose cooldown has passed, so the map
// holds only streams moved within leaderMoveCooldown. Callers hold r.mu.
func (r *LeaderRebalancer) forgetCooledDown(now time.Time) {
	for stream, at := range r.moved {
		if now.Sub(at) >= leaderMoveCooldown {
			delete(r.moved, stream)
		}
	}
}
