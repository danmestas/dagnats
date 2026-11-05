// natsutil/peer_retire_test.go
// Tests for which retired peer a tick removes, and when. Methodology:
// PeerID is checked against a real embedded server's own node hash;
// plan is judged on constructed MetaStates with an explicit clock, one
// condition at a time (the meta-leader gate, the online check, the
// offline window), so a mutation of any one fails a case. A real removal
// from a real cluster is covered in server/peer_retire_test.go.
package natsutil

import (
	"testing"
	"time"
)

func TestPeerID_MatchesTheServersOwnHash(t *testing.T) {
	ns, _ := StartTestServer(t)
	node := ns.Node()
	if node == "" {
		t.Fatal("embedded server reports no node hash")
	}
	// Positive: the hash of the server's name is the hash it reports.
	if got := PeerID(ns.Name()); got != node {
		t.Fatalf("PeerID(%q) = %q, server reports %q", ns.Name(), got, node)
	}
	// Negative: another name hashes elsewhere.
	if got := PeerID(ns.Name() + "-other"); got == node {
		t.Fatalf("PeerID of a different name = %q, the server's own hash", got)
	}
}

func retiring(offlineMin time.Duration, names ...string) *PeerRetirer {
	return &PeerRetirer{
		peers: names, offlineMin: offlineMin,
		offlineSince: map[string]time.Time{}, gone: map[string]bool{},
	}
}

func leading(peers ...MetaPeer) MetaState {
	return MetaState{Leader: true, Self: "a", Peers: peers}
}

func TestPeerRetirerPlan_WaitsOutTheOfflineWindow(t *testing.T) {
	r := retiring(2*time.Minute, "mini")
	start := time.Now()
	offline := leading(MetaPeer{Name: "b", Current: true}, MetaPeer{Name: "mini", Offline: true})
	// Negative: first seen offline, and still inside the window.
	if name, ok := r.plan(offline, start); ok {
		t.Fatalf("planned %s on first sight offline", name)
	}
	if name, ok := r.plan(offline, start.Add(2*time.Minute-time.Second)); ok {
		t.Fatalf("planned %s inside the offline window", name)
	}
	// Positive: offline for the whole window, it is removed.
	name, ok := r.plan(offline, start.Add(2*time.Minute))
	if !ok || name != "mini" {
		t.Fatalf("plan = %q, %v; want mini after the window", name, ok)
	}
	// Negative: the removal restarts the wait, so it is not resent each tick.
	if name, ok := r.plan(offline, start.Add(2*time.Minute+time.Second)); ok {
		t.Fatalf("planned %s again right after a removal", name)
	}
}

func TestPeerRetirerPlan_NeverAnOnlinePeer(t *testing.T) {
	r := retiring(0, "mini")
	start := time.Now()
	online := leading(MetaPeer{Name: "mini", Current: true})
	// Negative: an online peer is never removed, even with no window.
	for i := range 3 {
		if name, ok := r.plan(online, start.Add(time.Duration(i)*time.Hour)); ok {
			t.Fatalf("planned online peer %s", name)
		}
	}
	// Coming back online restarts the window.
	r = retiring(time.Minute, "mini")
	offline := leading(MetaPeer{Name: "mini", Offline: true})
	r.plan(offline, start)
	r.plan(online, start.Add(30*time.Second))
	if name, ok := r.plan(offline, start.Add(time.Minute)); ok {
		t.Fatalf("planned %s although it was online inside the window", name)
	}
	// Positive: the same peer offline with no window is removed.
	if name, ok := retiring(0, "mini").plan(offline, start); !ok || name != "mini" {
		t.Fatalf("plan = %q, %v; want mini", name, ok)
	}
}

func TestPeerRetirerPlan_OnlyOnTheMetaLeader(t *testing.T) {
	r := retiring(0, "mini")
	start := time.Now()
	follower := leading(MetaPeer{Name: "mini", Offline: true})
	follower.Leader = false
	// Negative: a follower plans nothing, even with a listed peer offline.
	if name, ok := r.plan(follower, start); ok {
		t.Fatalf("follower planned %s", name)
	}
	// A spell as follower forgets what was seen before it.
	r = retiring(time.Minute, "mini")
	offline := leading(MetaPeer{Name: "mini", Offline: true})
	r.plan(offline, start)
	r.plan(follower, start.Add(30*time.Second))
	if name, ok := r.plan(offline, start.Add(time.Minute)); ok {
		t.Fatalf("planned %s on a window that spanned a spell as follower", name)
	}
	// Positive: as leader, the window counted from regaining leadership.
	if name, ok := r.plan(offline, start.Add(2*time.Minute)); !ok || name != "mini" {
		t.Fatalf("plan = %q, %v; want mini", name, ok)
	}
}

func TestPeerRetirerPlan_MatchesByPeerIDAndFinishes(t *testing.T) {
	r := retiring(0, "mini", "other")
	start := time.Now()
	// A server the leader has not heard from since its restart shows
	// only its peer ID.
	unknown := leading(MetaPeer{Name: "Server name unknown", ID: PeerID("mini"), Offline: true})
	name, ok := r.plan(unknown, start)
	// Positive: matched by ID.
	if !ok || name != "mini" {
		t.Fatalf("plan = %q, %v; want mini by peer ID", name, ok)
	}
	// Negative: once gone from the meta group, nothing more is planned.
	if name, ok := r.plan(leading(), start.Add(time.Hour)); ok {
		t.Fatalf("planned %s with no listed peer in the meta group", name)
	}
	if !r.gone["mini"] || !r.gone["other"] {
		t.Fatalf("gone = %v, want both listed peers recorded gone", r.gone)
	}
}

func TestPeerRetirerPlan_OneRemovalPerTick(t *testing.T) {
	r := retiring(time.Minute, "x", "y")
	start := time.Now()
	both := leading(MetaPeer{Name: "x", Offline: true}, MetaPeer{Name: "y", Offline: true})
	r.plan(both, start)
	first, ok := r.plan(both, start.Add(time.Minute))
	// Positive: one of them now.
	if !ok || first != "x" {
		t.Fatalf("plan = %q, %v; want x first", first, ok)
	}
	// Positive: the other on the next tick; negative: not the same one twice.
	second, ok := r.plan(both, start.Add(time.Minute+time.Second))
	if !ok || second != "y" {
		t.Fatalf("plan = %q, %v; want y next", second, ok)
	}
}

func TestPeerRetirerPlan_OfflineAndNotCurrent(t *testing.T) {
	start := time.Now()
	for _, c := range []struct {
		name string
		peer MetaPeer
	}{
		{"offline but current", MetaPeer{Name: "mini", Offline: true, Current: true}},
		{"online but behind", MetaPeer{Name: "mini"}},
	} {
		// Negative: either half alone is not enough, even with no window.
		if name, ok := retiring(0, "mini").plan(leading(c.peer), start); ok {
			t.Errorf("%s: planned %s", c.name, name)
		}
	}
	// Positive: offline and not current is removed.
	gone := leading(MetaPeer{Name: "mini", Offline: true})
	if name, ok := retiring(0, "mini").plan(gone, start); !ok || name != "mini" {
		t.Fatalf("plan = %q, %v; want mini", name, ok)
	}
}

func TestPeerRetirerPlan_SkipsWhenPeersUnread(t *testing.T) {
	r := retiring(time.Minute, "mini")
	start := time.Now()
	offline := leading(MetaPeer{Name: "mini", Offline: true})
	unread := MetaState{Leader: true, Self: "a", PeersUnread: true}
	r.plan(offline, start)
	// Negative: an unread view neither plans nor marks the peer gone.
	if name, ok := r.plan(unread, start.Add(30*time.Second)); ok {
		t.Fatalf("planned %s from an unread view", name)
	}
	if r.gone["mini"] {
		t.Fatal("an unread view marked mini gone")
	}
	// Positive: the window kept running across it.
	if name, ok := r.plan(offline, start.Add(time.Minute)); !ok || name != "mini" {
		t.Fatalf("plan = %q, %v; want mini, the window not reset", name, ok)
	}
}
