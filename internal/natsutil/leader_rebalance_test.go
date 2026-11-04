// natsutil/leader_rebalance_test.go
// Tests for which stream a rebalance tick moves, and where. Methodology:
// planMove judged on constructed stream infos, one condition at a time,
// so each is pinned on its own; the cluster behaviour (a real step-down
// moving a real leader) is covered in dagnatstest. Positive and negative
// space per case.
package natsutil

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func driftedInfo(name string, replicas int, peers ...*jetstream.PeerInfo) *jetstream.StreamInfo {
	return &jetstream.StreamInfo{
		Config:  jetstream.StreamConfig{Name: name, Replicas: replicas},
		Cluster: &jetstream.ClusterInfo{Leader: "far", Replicas: peers},
	}
}

func peer(name string, current, offline bool) *jetstream.PeerInfo {
	return &jetstream.PeerInfo{Name: name, Current: current, Offline: offline}
}

func TestPlanMove(t *testing.T) {
	r := &LeaderRebalancer{preferred: []string{"a", "b"}, moved: map[string]time.Time{}}
	healthy := []*jetstream.PeerInfo{peer("a", true, false), peer("b", true, false)}
	settled := driftedInfo("SETTLED", 3, peer("far", true, false), peer("b", true, false))
	settled.Cluster.Leader = "a"
	r.moved["COOLING"] = time.Now()
	for _, c := range []struct {
		name string
		info *jetstream.StreamInfo
	}{
		{"R1", driftedInfo("R1", 1, healthy...)},
		{"leader already preferred", settled},
		{"no preferred replica current", driftedInfo("STALE", 3,
			peer("a", false, false), peer("b", true, true))},
		{"preferred servers not replicas", driftedInfo("ELSEWHERE", 3,
			peer("x", true, false), peer("y", true, false))},
		{"inside the cooldown", driftedInfo("COOLING", 3, healthy...)},
	} {
		// Negative: each condition alone keeps the stream where it is.
		if move, ok := r.planMove(c.info); ok {
			t.Errorf("%s: planned %+v, want no move", c.name, move)
		}
	}
	// Positive: a drifted stream with a current preferred replica moves
	// to it, the next preferred server in rotation first, skipping one
	// that is not current.
	r.next = 1
	move, ok := r.planMove(driftedInfo("DRIFTED", 3, healthy...))
	if !ok || move != (LeaderMove{Stream: "DRIFTED", From: "far", To: "b"}) {
		t.Fatalf("planMove = %+v, %v; want DRIFTED far -> b", move, ok)
	}
	move, ok = r.planMove(driftedInfo("DRIFTED", 3, peer("a", true, false), peer("b", false, false)))
	if !ok || move.To != "a" {
		t.Fatalf("planMove = %+v, %v; want a, b being behind", move, ok)
	}
}

func TestForgetCooledDown(t *testing.T) {
	now := time.Now()
	r := &LeaderRebalancer{moved: map[string]time.Time{
		"OLD": now.Add(-leaderMoveCooldown), "NEW": now.Add(-time.Second),
	}}
	r.forgetCooledDown(now)
	// Positive: a stream past its cooldown may move again.
	if _, held := r.moved["OLD"]; held {
		t.Fatal("OLD still cooling down past leaderMoveCooldown")
	}
	// Negative: one moved a second ago is still held.
	if _, held := r.moved["NEW"]; !held {
		t.Fatal("NEW forgotten inside its cooldown")
	}
}

func TestPlanMetaMove(t *testing.T) {
	r := &LeaderRebalancer{preferred: []string{"a", "b"}, moved: map[string]time.Time{},
		system: &nats.Conn{}}
	now := time.Now()
	farLeader := func(peers ...MetaPeer) MetaState {
		return MetaState{Leader: true, Self: "far", Peers: peers}
	}
	healthy := []MetaPeer{{Name: "a", Current: true}, {Name: "b", Current: true}}
	noSystem := &LeaderRebalancer{preferred: r.preferred, moved: r.moved}
	for _, c := range []struct {
		name  string
		r     *LeaderRebalancer
		state MetaState
	}{
		{"not the meta leader", r, MetaState{Self: "far", Peers: healthy}},
		{"leader already preferred", r, MetaState{Leader: true, Self: "a", Peers: healthy}},
		{"no preferred peer current", r, farLeader(
			MetaPeer{Name: "a"}, MetaPeer{Name: "b", Current: true, Offline: true})},
		{"preferred servers not peers", r, farLeader(MetaPeer{Name: "x", Current: true})},
		{"no system connection", noSystem, farLeader(healthy...)},
	} {
		// Negative: each condition alone keeps the meta leader.
		if move, ok := c.r.planMetaMove(c.state, now); ok {
			t.Errorf("%s: planned %+v, want no move", c.name, move)
		}
	}
	// Positive: a far meta leader moves to the next current preferred peer.
	r.next = 1
	move, ok := r.planMetaMove(farLeader(healthy...), now)
	if !ok || move != (LeaderMove{Meta: true, From: "far", To: "b"}) {
		t.Fatalf("planMetaMove = %+v, %v; want meta far -> b", move, ok)
	}
	// Negative: inside the cooldown after a meta move, it waits.
	r.metaMovedAt = now.Add(-time.Second)
	if move, ok := r.planMetaMove(farLeader(healthy...), now); ok {
		t.Fatalf("planned %+v inside the meta cooldown", move)
	}
	r.metaMovedAt = now.Add(-leaderMoveCooldown)
	if _, ok := r.planMetaMove(farLeader(healthy...), now); !ok {
		t.Fatal("no move once the meta cooldown passed")
	}
}
