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

// LeaderMove is one step-down Tick sent.
type LeaderMove struct {
	Stream, From, To string
}

// LeaderRebalancer moves replicated streams' leaders onto preferred
// servers. JetStream elects any in-sync replica, so on a cluster with
// one node across a slow link (a laptop 160-700 ms from two nodes 1.4 ms
// apart) leaders drift onto it after restarts and elections, and every
// write to those streams then crosses that link. It asks a stream's
// leader to step down in favour of a current preferred replica, which
// nats-server honours. Safe for one goroutine; the caller runs it on the
// JetStream meta leader only, so the cluster has one actor.
type LeaderRebalancer struct {
	nc        *nats.Conn
	js        jetstream.JetStream
	preferred []string

	mu    sync.Mutex
	moved map[string]time.Time // stream -> last move, inside the cooldown
	next  int                  // rotates moves over the preferred servers
}

// NewLeaderRebalancer returns a rebalancer preferring the given server
// names. With none it never moves anything.
func NewLeaderRebalancer(nc *nats.Conn, preferred []string) *LeaderRebalancer {
	if nc == nil {
		panic("NewLeaderRebalancer: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic("NewLeaderRebalancer: jetstream.New on a live conn: " + err.Error())
	}
	return &LeaderRebalancer{
		nc: nc, js: js, preferred: slices.Clone(preferred), moved: map[string]time.Time{},
	}
}

// Run calls Tick shortly after start and then every
// LeaderRebalanceInterval until ctx ends, reading isMetaLeader each time.
func (r *LeaderRebalancer) Run(ctx context.Context, isMetaLeader func() bool) {
	if ctx == nil || isMetaLeader == nil {
		panic("LeaderRebalancer.Run: ctx and isMetaLeader must be set")
	}
	wait := leaderRebalanceStartDelay
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
		wait = LeaderRebalanceInterval
		if _, err := r.Tick(ctx, isMetaLeader()); err != nil && ctx.Err() == nil {
			slog.Warn("natsutil: stream leader rebalance failed", "error", err)
		}
	}
}

// Tick moves up to LeaderMovesPerTickMax drifted leaders, and returns
// the moves it made. It does nothing unless metaLeader, or with no
// preferred servers. A stream is moved when it has more than one
// replica, its leader is not preferred, a preferred replica is current,
// and it was not moved within the cooldown. A failed step-down is logged
// and skipped; only a failed listing is returned.
func (r *LeaderRebalancer) Tick(ctx context.Context, metaLeader bool) ([]LeaderMove, error) {
	if ctx == nil {
		panic("LeaderRebalancer.Tick: ctx must not be nil")
	}
	if !metaLeader || len(r.preferred) == 0 {
		return nil, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.forgetCooledDown(time.Now())
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

// stepDown asks move.Stream's leader to step down in favour of move.To.
func (r *LeaderRebalancer) stepDown(ctx context.Context, move LeaderMove) error {
	if move.Stream == "" || move.To == "" {
		panic("LeaderRebalancer.stepDown: stream and target must be set")
	}
	body, err := json.Marshal(map[string]any{
		"placement": map[string]string{"preferred": move.To},
	})
	if err != nil {
		panic("LeaderRebalancer.stepDown: a placement always encodes")
	}
	ctx, cancel := context.WithTimeout(ctx, leaderStepDownTimeout)
	defer cancel()
	reply, err := r.nc.RequestWithContext(ctx, "$JS.API.STREAM.LEADER.STEPDOWN."+move.Stream, body)
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
