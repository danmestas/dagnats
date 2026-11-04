// region_harness_test.go: two regional clusters for ADR-031 tests. Each
// region is three dagnats NATS servers started through startNATS (so the
// production options, leaf wiring included, are what runs), with its own
// cluster name and JetStream domain. The listening region opens a leaf
// listener on every node; the dialing region holds a leaf remote to every
// one of those listeners on every node. Ports come from freeTCPPorts and
// every wait is bounded.
package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	testRegionNodes    = 3
	testRegionFormWait = 30 * time.Second
	testRegionLinkWait = 20 * time.Second
	// testRegionMaxStoreBytes clears the ceilings SetupAll derives from its
	// default budget, as dagnatstest's cluster does; it is a reservation
	// ceiling, not disk the tests write.
	testRegionMaxStoreBytes int64 = 12 << 30
)

// testRegion is one running regional cluster.
type testRegion struct {
	name      string
	nodes     []*natsserver.Server
	leafPorts []int // bound leaf listener ports; nil for a dialing region
}

// regionOptions shapes a region's leaf side: listen opens a leaf listener
// on every node; dial lists leaf listener ports every node links to.
type regionOptions struct {
	listen bool
	dial   []int
	// mutate, when set, adjusts each node's config before it starts.
	mutate func(*Config)
}

// startRegionPair starts region "eu" with leaf listeners and region "us"
// linked to every eu listener from every node, and waits for the links.
func startRegionPair(t *testing.T, euMutate func(*Config)) (*testRegion, *testRegion) {
	t.Helper()
	eu := startTestRegion(t, "eu", regionOptions{listen: true, mutate: euMutate})
	us := startTestRegion(t, "us", regionOptions{dial: eu.leafPorts})
	waitRegionLinked(t, us, len(eu.leafPorts))
	return eu, us
}

// startTestRegion starts a three-node cluster named and domained after
// name, waits for routes and a meta leader, and registers cleanup.
func startTestRegion(t *testing.T, name string, opts regionOptions) *testRegion {
	t.Helper()
	if name == "" {
		panic("startTestRegion: name is empty")
	}
	if opts.listen && len(opts.dial) > 0 {
		panic("startTestRegion: a test region either listens or dials")
	}
	routePorts := freeTCPPorts(t, testRegionNodes)
	region := &testRegion{name: name, nodes: make([]*natsserver.Server, testRegionNodes)}
	for i := range region.nodes {
		cfg := regionNodeConfig(t, name, i, routePorts, opts)
		node, err := startNATS(cfg)
		if err != nil {
			t.Fatalf("start %s node %d: %v", name, i, err)
		}
		t.Cleanup(node.Shutdown)
		region.nodes[i] = node
		if opts.listen {
			varz, err := node.Varz(nil)
			if err != nil || varz.LeafNode.Port <= 0 {
				t.Fatalf("%s node %d leaf listener not bound: %v", name, i, err)
			}
			region.leafPorts = append(region.leafPorts, varz.LeafNode.Port)
		}
	}
	waitRegionFormed(t, region)
	return region
}

// regionNodeConfig is node i's dagnats config in region name.
func regionNodeConfig(
	t *testing.T, name string, i int, routePorts []int, opts regionOptions,
) Config {
	t.Helper()
	routes := make([]string, 0, len(routePorts)-1)
	for j, port := range routePorts {
		if j != i {
			routes = append(routes, fmt.Sprintf("nats://127.0.0.1:%d", port))
		}
	}
	cfg := Config{
		DataDir: t.TempDir(), NATSPort: -1, NATSHost: "127.0.0.1",
		MaxStoreBytes: testRegionMaxStoreBytes, MaxMemoryBytes: 1 << 28,
		NATSClusterName: name, JetStreamDomain: name, Region: name,
		NATSClusterHost: "127.0.0.1", NATSClusterPort: routePorts[i],
		NATSClusterRoutes: routes, NATSServerName: fmt.Sprintf("%s-%d", name, i),
	}
	if opts.listen {
		cfg.LeafPort, cfg.LeafListenUser = -1, testLeafUser
		cfg.LeafListenPasswordFile = writeSecret(t, testLeafPassword)
	}
	for _, port := range opts.dial {
		cfg.LeafRemotes = append(cfg.LeafRemotes, fmt.Sprintf("nats-leaf://127.0.0.1:%d", port))
	}
	if len(opts.dial) > 0 {
		cfg.LeafUser, cfg.LeafPasswordFile = testLeafUser, writeSecret(t, testLeafPassword)
	}
	if opts.mutate != nil {
		opts.mutate(&cfg)
	}
	validateClusterConfig(&cfg)
	if err := validateLeafConfig(cfg); err != nil {
		t.Fatalf("%s node %d config: %v", name, i, err)
	}
	if err := resolveClientAuth(&cfg); err != nil {
		t.Fatalf("%s node %d auth: %v", name, i, err)
	}
	return cfg
}

// waitRegionFormed waits for a full route mesh and a JetStream API that
// answers on every node.
func waitRegionFormed(t *testing.T, region *testRegion) {
	t.Helper()
	deadline := time.Now().Add(testRegionFormWait)
	for time.Now().Before(deadline) {
		ready := true
		for _, node := range region.nodes {
			ready = ready && node.NumRoutes() >= len(region.nodes)-1 && jetStreamMetaReady(node)
		}
		if ready {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("region %s did not form within %s", region.name, testRegionFormWait)
}

// waitRegionLinked waits until every node of region holds want leaf links.
func waitRegionLinked(t *testing.T, region *testRegion, want int) {
	t.Helper()
	if want < 1 {
		panic("waitRegionLinked: want must be positive")
	}
	waitRegionLeafCount(t, region, func(n int) bool { return n == want },
		fmt.Sprintf("%d leaf links", want))
}

// waitRegionLeafCount waits until every node's leaf count satisfies ok.
func waitRegionLeafCount(t *testing.T, region *testRegion, ok func(int) bool, what string) {
	t.Helper()
	deadline := time.Now().Add(testRegionLinkWait)
	for time.Now().Before(deadline) {
		done := true
		for _, node := range region.nodes {
			done = done && ok(node.NumLeafNodes())
		}
		if done {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	counts := make([]int, 0, len(region.nodes))
	for _, node := range region.nodes {
		counts = append(counts, node.NumLeafNodes())
	}
	t.Fatalf("region %s: leaf counts %v, want %s within %s",
		region.name, counts, what, testRegionLinkWait)
}

// connect opens a client on node i of the region.
func (r *testRegion) connect(t *testing.T, i int) *nats.Conn {
	t.Helper()
	return connectOrFail(t, r.nodes[i].ClientURL())
}

// jetStream opens a JetStream context on node i, by domain when set.
func (r *testRegion) jetStream(t *testing.T, i int, domain string) jetstream.JetStream {
	t.Helper()
	return newJetStream(t, r.connect(t, i), domain)
}

// shutdown stops every node of the region.
func (r *testRegion) shutdown() {
	for _, node := range r.nodes {
		node.Shutdown()
		node.WaitForShutdown()
	}
}

// regionCtx is a bounded context for one test step.
func regionCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}
