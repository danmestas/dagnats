// server/leaders_meta_test.go
// Tests for moving the JetStream meta leader onto a preferred server.
// Methodology: three real NATS servers built from natsOptions, as serve
// builds them (so each has the in-process system user applyAuth adds),
// routed into one cluster. The meta leader is forced onto the third
// server through the system account, as an election can leave it there;
// one Tick on that server, with its live MetaState, must move it to a
// preferred server. Every wait is bounded.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	"github.com/danmestas/dagnats/internal/natsutil"
)

const metaSettleWait = 30 * time.Second

type metaNode struct {
	ns       *natsserver.Server
	password string
	opts     *natsserver.Options // to restart it on the same store
}

func startMetaCluster(t *testing.T) []metaNode {
	t.Helper()
	return startMetaClusterOf(t, 3)
}

// startMetaClusterOf starts size routed servers named n0, n1, ... and
// waits for a meta leader.
func startMetaClusterOf(t *testing.T, size int) []metaNode {
	t.Helper()
	clusterPorts := freeTCPPorts(t, size)
	routes := make([]string, len(clusterPorts))
	for i, port := range clusterPorts {
		routes[i] = fmt.Sprintf("nats://127.0.0.1:%d", port)
	}
	nodes := make([]metaNode, 0, len(clusterPorts))
	for i, port := range clusterPorts {
		cfg := testConfig(t)
		cfg.NATSClusterName = "meta-test"
		cfg.NATSClusterRoutes = routes
		cfg.NATSClusterPort = port
		cfg.NATSClusterHost = "127.0.0.1"
		cfg.NATSServerName = fmt.Sprintf("n%d", i)
		cfg.MaxStoreBytes = 256 << 20
		opts, err := natsOptions(cfg)
		if err != nil {
			t.Fatalf("natsOptions: %v", err)
		}
		ns, err := natsserver.NewServer(opts)
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		ns.Start()
		t.Cleanup(ns.Shutdown)
		nodes = append(nodes, metaNode{ns: ns, password: systemPassword(opts), opts: opts})
	}
	for _, node := range nodes {
		if !node.ns.ReadyForConnections(10 * time.Second) {
			t.Fatalf("%s not ready", node.ns.Name())
		}
	}
	metaLeader(t, nodes)
	return nodes
}

// metaLeader waits for the cluster to agree on a meta leader and
// returns its name.
func metaLeader(t *testing.T, nodes []metaNode) string {
	t.Helper()
	deadline := time.Now().Add(metaSettleWait)
	for {
		for _, node := range nodes {
			if node.ns.JetStreamIsLeader() {
				return node.ns.Name()
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("no meta leader elected")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func dialSystem(t *testing.T, node metaNode) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect("", nats.InProcessServer(node.ns),
		nats.UserInfo(systemUser, node.password))
	if err != nil {
		t.Fatalf("dial %s as the system user: %v", node.ns.Name(), err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func dialRoot(t *testing.T, node metaNode) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect("", nats.InProcessServer(node.ns))
	if err != nil {
		t.Fatalf("dial %s: %v", node.ns.Name(), err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// forceMetaLeader steps the meta leader down in favour of name until it
// holds.
func forceMetaLeader(t *testing.T, nodes []metaNode, name string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"placement": map[string]string{"preferred": name}})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	system := dialSystem(t, nodes[0])
	deadline := time.Now().Add(metaSettleWait)
	for metaLeader(t, nodes) != name {
		if time.Now().After(deadline) {
			t.Fatalf("meta leader stayed %s, could not force it onto %s", metaLeader(t, nodes), name)
		}
		_, _ = system.Request("$JS.API.META.LEADER.STEPDOWN", body, 2*time.Second)
		time.Sleep(500 * time.Millisecond)
	}
}

func metaTick(t *testing.T, r *natsutil.LeaderRebalancer, node metaNode) []natsutil.LeaderMove {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), metaSettleWait)
	defer cancel()
	moves, err := r.Tick(ctx, metaStateOf(node.ns))
	if err != nil {
		t.Fatalf("Tick: %v", err)
	}
	return moves
}

func TestLeaderRebalancer_MovesTheMetaLeaderOntoAPreferredServer(t *testing.T) {
	nodes := startMetaCluster(t)
	preferred := []string{"n0", "n1"}
	far := nodes[2]
	forceMetaLeader(t, nodes, "n2")
	// Negative: no preference moves nothing.
	off := natsutil.NewLeaderRebalancer(dialRoot(t, far), dialSystem(t, far), nil)
	if moves := metaTick(t, off, far); len(moves) != 0 {
		t.Fatalf("no preference moved %+v", moves)
	}
	if leader := metaLeader(t, nodes); leader != "n2" {
		t.Fatalf("meta leader %s, want it left on n2", leader)
	}
	r := natsutil.NewLeaderRebalancer(dialRoot(t, far), dialSystem(t, far), preferred)
	moves := metaTick(t, r, far)
	// Positive: the far meta leader steps down to a preferred server.
	if len(moves) != 1 || !moves[0].Meta || moves[0].From != "n2" ||
		!slices.Contains(preferred, moves[0].To) {
		t.Fatalf("moves = %+v, want the meta leader from n2 to a preferred server", moves)
	}
	deadline := time.Now().Add(metaSettleWait)
	for !slices.Contains(preferred, metaLeader(t, nodes)) {
		if time.Now().After(deadline) {
			t.Fatalf("meta leader stayed %s", metaLeader(t, nodes))
		}
		time.Sleep(100 * time.Millisecond)
	}
	// Negative: back on n2 inside the cooldown, the same rebalancer waits.
	forceMetaLeader(t, nodes, "n2")
	if again := metaTick(t, r, far); len(again) != 0 {
		t.Fatalf("moved %+v inside the meta cooldown", again)
	}
}
