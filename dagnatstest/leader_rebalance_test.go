// dagnatstest/leader_rebalance_test.go
// Methodology: the preferred-leader rebalancer on a real 3-node
// in-process cluster. Two servers are preferred; streams' leaders are
// forced onto the third with a step-down that prefers it, as an
// election drifts them there on the mesh. One Tick must move them back
// to a preferred server, at most LeaderMovesPerTickMax per tick, never
// touching an R1 stream, and not at all with no preference or on a node
// that is not the meta leader. Every wait is bounded. Lives here because
// the cluster helper is in this package.
package dagnatstest

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/natsutil"
)

const leaderSettleWait = 20 * time.Second

type leaderCluster struct {
	nc        *nats.Conn
	js        jetstream.JetStream
	preferred []string
	other     string
}

func startLeaderCluster(t *testing.T) leaderCluster {
	t.Helper()
	cluster := startClusterWithRetry(t, minTestClusterNodes, allocateFreePorts)
	t.Cleanup(cluster.shutdown)
	waitForRoutesMeshed(t, cluster.servers, minTestClusterNodes)
	waitForJetStreamLeader(t, cluster.servers)
	nc := connectAllNodes(t, cluster)
	confirmClusterQuorum(t, nc, minTestClusterNodes)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	names := make([]string, 0, len(cluster.servers))
	for _, server := range cluster.servers {
		names = append(names, server.Name())
	}
	return leaderCluster{nc: nc, js: js, preferred: names[:2], other: names[2]}
}

// stream creates an R<replicas> stream and, for R3, forces its leader
// onto the non-preferred server.
func (c leaderCluster) stream(t *testing.T, name string, replicas int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), leaderSettleWait)
	defer cancel()
	_, err := c.js.CreateStream(ctx, jetstream.StreamConfig{
		Name: name, Subjects: []string{name + ".>"}, Replicas: replicas, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatalf("create %s: %v", name, err)
	}
	if replicas > 1 {
		c.forceLeader(t, name, c.other)
	}
}

func (c leaderCluster) leader(t *testing.T, name string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := c.js.Stream(ctx, name)
	if err != nil {
		return ""
	}
	if info := stream.CachedInfo(); info.Cluster != nil {
		return info.Cluster.Leader
	}
	return ""
}

// forceLeader steps name's leader down in favour of server until it
// holds, as an election can leave it there.
func (c leaderCluster) forceLeader(t *testing.T, name, server string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"placement": map[string]string{"preferred": server}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	deadline := time.Now().Add(leaderSettleWait)
	for c.leader(t, name) != server {
		if time.Now().After(deadline) {
			t.Fatalf("%s leader is %q, could not force it onto %s", name, c.leader(t, name), server)
		}
		_, _ = c.nc.Request("$JS.API.STREAM.LEADER.STEPDOWN."+name, body, 2*time.Second)
		time.Sleep(200 * time.Millisecond)
	}
}

// waitPreferred waits for name's leader to be one of the preferred
// servers and returns it.
func (c leaderCluster) waitPreferred(t *testing.T, name string) string {
	t.Helper()
	deadline := time.Now().Add(leaderSettleWait)
	for {
		if leader := c.leader(t, name); slices.Contains(c.preferred, leader) {
			return leader
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s leader stayed %q, want one of %v", name, c.leader(t, name), c.preferred)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func tick(t *testing.T, r *natsutil.LeaderRebalancer, metaLeader bool) []natsutil.LeaderMove {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), leaderSettleWait)
	defer cancel()
	moves, err := r.Tick(ctx, natsutil.MetaState{Leader: metaLeader})
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	return moves
}

func TestLeaderRebalancer_MovesLeadersOntoPreferredServers(t *testing.T) {
	c := startLeaderCluster(t)
	c.stream(t, "DRIFTED", 3)
	c.stream(t, "SINGLE", 1)
	singleLeader := c.leader(t, "SINGLE")
	r := natsutil.NewLeaderRebalancer(c.nc, nil, c.preferred)
	moves := tick(t, r, true)
	// Positive: the drifted leader is moved onto a preferred server.
	if len(moves) != 1 || moves[0].Stream != "DRIFTED" || moves[0].From != c.other ||
		!slices.Contains(c.preferred, moves[0].To) {
		t.Fatalf("moves = %+v, want DRIFTED from %s to a preferred server", moves, c.other)
	}
	if leader := c.waitPreferred(t, "DRIFTED"); leader != moves[0].To {
		t.Fatalf("DRIFTED leader %s, want the server it was moved to, %s", leader, moves[0].To)
	}
	// Negative: an R1 stream is never touched, and a stream already led
	// by a preferred server is left alone on the next tick.
	if leader := c.leader(t, "SINGLE"); leader != singleLeader {
		t.Fatalf("SINGLE leader %s -> %s, want untouched", singleLeader, leader)
	}
	if again := tick(t, r, true); len(again) != 0 {
		t.Fatalf("second tick moved %+v, want nothing", again)
	}
}

func TestLeaderRebalancer_DoesNothingWhenOff(t *testing.T) {
	c := startLeaderCluster(t)
	c.stream(t, "DRIFTED", 3)
	// Negative: no preference, or a node that is not the meta leader.
	if moves := tick(t, natsutil.NewLeaderRebalancer(c.nc, nil, nil), true); len(moves) != 0 {
		t.Fatalf("no preference moved %+v", moves)
	}
	if moves := tick(t, natsutil.NewLeaderRebalancer(c.nc, nil, c.preferred), false); len(moves) != 0 {
		t.Fatalf("a node that is not the meta leader moved %+v", moves)
	}
	time.Sleep(time.Second)
	// Positive: the drifted leader is still where it was forced.
	if leader := c.leader(t, "DRIFTED"); leader != c.other {
		t.Fatalf("DRIFTED leader %s, want it left on %s", leader, c.other)
	}
}

func TestLeaderRebalancer_BoundsMovesPerTickAndCoolsDown(t *testing.T) {
	c := startLeaderCluster(t)
	streams := natsutil.LeaderMovesPerTickMax + 2
	for i := 0; i < streams; i++ {
		c.stream(t, fmt.Sprintf("DRIFT%d", i), 3)
	}
	r := natsutil.NewLeaderRebalancer(c.nc, nil, c.preferred)
	first := tick(t, r, true)
	// Positive: one tick moves no more than its bound.
	if len(first) != natsutil.LeaderMovesPerTickMax {
		t.Fatalf("first tick moved %d, want %d", len(first), natsutil.LeaderMovesPerTickMax)
	}
	targets := map[string]bool{}
	for _, move := range first {
		c.waitPreferred(t, move.Stream)
		targets[move.To] = true
	}
	// The moves are spread over the preferred servers, not piled on one.
	if len(targets) != len(c.preferred) {
		t.Fatalf("first tick moved onto %v, want both preferred servers", targets)
	}
	second := tick(t, r, true)
	if len(second) != streams-len(first) {
		t.Fatalf("second tick moved %d, want the remaining %d", len(second), streams-len(first))
	}
	// Negative: a stream moved moments ago is cooling down, so it is not
	// moved again even when its leader drifts straight back.
	c.forceLeader(t, first[0].Stream, c.other)
	if third := tick(t, r, true); len(third) != 0 {
		t.Fatalf("third tick moved %+v, want nothing inside the cooldown", third)
	}
}
