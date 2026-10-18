// Methodology: pure unit test of buildMembership over hand-built nats-server
// structs (the live path is covered by the multi-process e2e test). Asserts
// the merge rules: self is always first and connected, a peer is route
// connected only if the route table names it, and the meta leader flag
// lands on exactly one member.
package server

import (
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

func TestBuildMembership(t *testing.T) {
	meta := &natsserver.MetaClusterInfo{
		Leader: "n2",
		Replicas: []*natsserver.PeerInfo{
			{Name: "n2", Current: true},
			{Name: "n3", Offline: true, Lag: 4},
		},
	}
	routes := []*natsserver.RouteInfo{{RemoteName: "n2"}}
	view := buildMembership("n1", meta, routes)

	if len(view.Members) != 3 || !view.Members[0].Self {
		t.Fatalf("members = %+v, want self first of 3", view.Members)
	}
	if view.MetaLeader != "n2" || !view.Members[1].MetaLeader {
		t.Errorf("meta leader not marked on n2: %+v", view.Members)
	}
	if view.Members[0].MetaLeader || view.Members[2].MetaLeader {
		t.Errorf("meta leader flag leaked: %+v", view.Members)
	}
	if !view.Members[1].RouteConnected || view.Members[2].RouteConnected {
		t.Errorf("route flags wrong: %+v", view.Members)
	}
	if !view.Members[2].Offline {
		t.Errorf("n3 offline lost: %+v", view.Members[2])
	}
	// Answering node n1 is a follower: its view of caught-up is unreliable,
	// so current/lag must be omitted rather than reported wrong.
	for _, m := range view.Members {
		if m.Current != nil || m.LagOps != 0 {
			t.Errorf("follower view must omit current/lag: %+v", m)
		}
	}
}

// TestBuildMembershipLeaderView verifies the leader's view does report
// current and lag for followers.
func TestBuildMembershipLeaderView(t *testing.T) {
	meta := &natsserver.MetaClusterInfo{
		Leader: "n1",
		Replicas: []*natsserver.PeerInfo{
			{Name: "n2", Current: true},
			{Name: "n3", Current: false, Lag: 9},
		},
	}
	view := buildMembership("n1", meta, nil)
	if c := view.Members[0].Current; c == nil || !*c {
		t.Errorf("leader self current = %v, want true", c)
	}
	if c := view.Members[1].Current; c == nil || !*c {
		t.Errorf("n2 current = %v, want true", c)
	}
	if c := view.Members[2].Current; c == nil || *c || view.Members[2].LagOps != 9 {
		t.Errorf("n3 = %+v, want current=false lag=9", view.Members[2])
	}
}
