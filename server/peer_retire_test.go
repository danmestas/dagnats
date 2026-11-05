// server/peer_retire_test.go
// Tests for removing a retired server from the JetStream meta group.
// Methodology: real NATS servers built from natsOptions, as serve builds
// them (so each has the in-process system user), routed into one
// cluster; the reconciler ticks with the live MetaState of the node it
// runs on, as serve's loop does, with a short offline window. A stopped
// listed node must be removed only after the window and only by the meta
// leader, and every R3 stream must end with three in-sync replicas
// without it; an online listed node is never removed. Every wait is
// bounded.
package server

import (
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/natsutil"
)

const (
	retireOfflineWindow = 2 * time.Second
	retireSettleWait    = 60 * time.Second
	retireStreamCount   = 4
)

// leaderMeta is the live MetaState of whichever running node leads the
// meta group, waiting for one.
func leaderMeta(t *testing.T, nodes []metaNode) (metaNode, natsutil.MetaState) {
	t.Helper()
	deadline := time.Now().Add(metaSettleWait)
	for time.Now().Before(deadline) {
		for _, node := range nodes {
			if node.ns.Running() && node.ns.JetStreamIsLeader() {
				return node, metaStateOf(node.ns)
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("no meta leader among the running nodes")
	return metaNode{}, natsutil.MetaState{}
}

// metaPeerOf finds name in meta by name or peer ID.
func metaPeerOf(meta natsutil.MetaState, name string) (natsutil.MetaPeer, bool) {
	for _, peer := range meta.Peers {
		if peer.Name == name || peer.ID == natsutil.PeerID(name) {
			return peer, true
		}
	}
	return natsutil.MetaPeer{}, false
}

func retireTick(t *testing.T, r *natsutil.PeerRetirer, node metaNode) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), metaSettleWait)
	defer cancel()
	return r.Tick(ctx, metaStateOf(node.ns))
}

func createR3Streams(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), metaSettleWait)
	defer cancel()
	for i := range retireStreamCount {
		name := fmt.Sprintf("RETIRE_%d", i)
		_, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name: name, Subjects: []string{fmt.Sprintf("retire.%d", i)}, Replicas: 3,
		})
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
}

// streamServers is every server holding name: its leader and replicas.
func streamServers(t *testing.T, js jetstream.JetStream, name string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, name)
	if err != nil {
		return nil
	}
	info, err := stream.Info(ctx)
	if err != nil || info.Cluster == nil || info.Cluster.Leader == "" {
		return nil
	}
	servers := []string{info.Cluster.Leader}
	for _, peer := range info.Cluster.Replicas {
		if !peer.Current || peer.Offline {
			return nil
		}
		servers = append(servers, peer.Name)
	}
	return servers
}

func TestPeerRetirer_RemovesAStoppedPeerAndReplacesItsReplicas(t *testing.T) {
	nodes := startMetaClusterOf(t, 4)
	leader, meta := leaderMeta(t, nodes)
	// The hash matches the IDs the meta group itself reports.
	for _, peer := range meta.Peers {
		if peer.ID != natsutil.PeerID(peer.Name) {
			t.Fatalf("peer %s has ID %s, PeerID gives %s",
				peer.Name, peer.ID, natsutil.PeerID(peer.Name))
		}
	}
	js, err := jetstream.New(dialRoot(t, leader))
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	createR3Streams(t, js)
	victim := pickVictim(t, js, leader.ns.Name())
	stopAndAwaitOffline(t, nodes, victim)

	// Negative: a follower never removes, even with no window.
	for _, node := range nodes {
		if node.ns.Running() && !node.ns.JetStreamIsLeader() {
			follower := natsutil.NewPeerRetirer(dialSystem(t, node), []string{victim}, 0)
			if got := retireTick(t, follower, node); got != "" {
				t.Fatalf("follower %s removed %s", node.ns.Name(), got)
			}
		}
	}
	leader, _ = leaderMeta(t, nodes)
	r := natsutil.NewPeerRetirer(dialSystem(t, leader), []string{victim}, retireOfflineWindow)
	firstSeen := time.Now()
	// Negative: first seen offline, inside the window.
	if got := retireTick(t, r, leader); got != "" {
		t.Fatalf("removed %s on first sight", got)
	}
	awaitRemoval(t, r, leader, victim, firstSeen)
	awaitMetaGone(t, nodes, victim)
	awaitStreamsWithout(t, js, victim)
	// Negative: gone, nothing more is sent.
	if got := retireTick(t, r, leader); got != "" {
		t.Fatalf("sent another removal for %s after it left", got)
	}
}

// pickVictim is a non-leader server holding a replica of the first
// stream, so stopping it leaves that stream a replica short.
func pickVictim(t *testing.T, js jetstream.JetStream, leader string) string {
	t.Helper()
	deadline := time.Now().Add(metaSettleWait)
	for time.Now().Before(deadline) {
		for _, name := range streamServers(t, js, "RETIRE_0") {
			if name != leader {
				return name
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("RETIRE_0 never reported its replicas")
	return ""
}

func stopAndAwaitOffline(t *testing.T, nodes []metaNode, victim string) {
	t.Helper()
	for _, node := range nodes {
		if node.ns.Name() == victim {
			node.ns.Shutdown()
		}
	}
	deadline := time.Now().Add(metaSettleWait)
	for time.Now().Before(deadline) {
		_, meta := leaderMeta(t, nodes)
		if peer, ok := metaPeerOf(meta, victim); ok && peer.Offline {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s never showed offline", victim)
}

// awaitRemoval ticks until the removal is sent, which must not be before
// the offline window has passed since firstSeen.
func awaitRemoval(
	t *testing.T, r *natsutil.PeerRetirer, leader metaNode, victim string, firstSeen time.Time,
) {
	t.Helper()
	deadline := time.Now().Add(retireSettleWait)
	for time.Now().Before(deadline) {
		got := retireTick(t, r, leader)
		if got == "" {
			time.Sleep(250 * time.Millisecond)
			continue
		}
		if got != victim {
			t.Fatalf("removed %s, want %s", got, victim)
		}
		if waited := time.Since(firstSeen); waited < retireOfflineWindow {
			t.Fatalf("removed after %v, inside the %v window", waited, retireOfflineWindow)
		}
		return
	}
	t.Fatalf("%s never removed", victim)
}

func awaitMetaGone(t *testing.T, nodes []metaNode, victim string) {
	t.Helper()
	deadline := time.Now().Add(retireSettleWait)
	for time.Now().Before(deadline) {
		_, meta := leaderMeta(t, nodes)
		if _, ok := metaPeerOf(meta, victim); !ok && len(meta.Peers) == 2 {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("%s still in the meta group", victim)
}

func awaitStreamsWithout(t *testing.T, js jetstream.JetStream, victim string) {
	t.Helper()
	deadline := time.Now().Add(retireSettleWait)
	for i := range retireStreamCount {
		name := fmt.Sprintf("RETIRE_%d", i)
		for {
			servers := streamServers(t, js, name)
			if len(servers) == 3 && !slices.Contains(servers, victim) {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s on %v, want 3 in-sync replicas without %s", name, servers, victim)
			}
			time.Sleep(250 * time.Millisecond)
		}
	}
}

func TestPeerRetirer_NeverRemovesAnOnlinePeer(t *testing.T) {
	nodes := startMetaCluster(t)
	leader, meta := leaderMeta(t, nodes)
	if len(meta.Peers) != 2 {
		t.Fatalf("meta peers = %+v, want 2", meta.Peers)
	}
	online := meta.Peers[0].Name
	r := natsutil.NewPeerRetirer(dialSystem(t, leader), []string{online}, 0)
	// Negative: an online listed peer is never removed, even with no window.
	for range 3 {
		if got := retireTick(t, r, leader); got != "" {
			t.Fatalf("removed online peer %s", got)
		}
	}
	// Positive: it is still a member, and online.
	_, meta = leaderMeta(t, nodes)
	if peer, ok := metaPeerOf(meta, online); !ok || peer.Offline {
		t.Fatalf("%s: present %v, %+v; want an online member", online, ok, peer)
	}
}
