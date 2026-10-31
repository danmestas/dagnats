// dagnatstest/cluster.go
// In-process N-node NATS cluster helper for tests. Spins up real
// nats-server instances configured as a cluster, waits for routes
// to mesh and a JetStream meta-leader to be elected, then confirms
// the cluster is API-healthy via the production WaitForClusterQuorum
// helper. Cleanup registered with t.Cleanup so tests stay leak-free.
package dagnatstest

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/natsutil"
)

const (
	minTestClusterNodes    = 3
	maxTestClusterNodes    = 5
	testClusterReadyTime   = 5 * time.Second
	testClusterFormTime    = 15 * time.Second
	testClusterQuorumTTL   = 30 * time.Second
	placementProbeInterval = 250 * time.Millisecond

	// testClusterListenWait bounds how long a started node may take to
	// bind its listeners; a failed bind is reported at once, so the
	// bound only matters on a loaded runner.
	testClusterListenWait = 10 * time.Second
	// testClusterStartAttemptsMax bounds the starts on fresh ports when
	// another process takes an allocated port before its node binds it.
	testClusterStartAttemptsMax = 3
)

// testClusterMaxStoreBytes pins each cluster node's JetStream store budget
// so placement is hermetic rather than a function of host free disk. It
// must clear the ceilings SetupAll derives from the default budget, since
// these tests call SetupAll without an explicit one: 0.80 of 10 GiB is
// 8 GiB of per-peer reservation, so 12 GiB leaves real headroom. The
// budget is a reservation ceiling, not preallocated disk — nothing here
// writes more than a few MiB.
const testClusterMaxStoreBytes int64 = 12 << 30

// StartTestCluster starts n in-process NATS servers configured as a
// cluster, waits for routes to mesh and quorum to form, and returns
// a connection to peer 0. All cleanup is registered with t.Cleanup.
// Panics if n < 3 or n > 5.
func StartTestCluster(t *testing.T, n int) *nats.Conn {
	t.Helper()
	if n < minTestClusterNodes || n > maxTestClusterNodes {
		panic(fmt.Sprintf("StartTestCluster: n=%d out of [%d, %d]",
			n, minTestClusterNodes, maxTestClusterNodes))
	}

	cluster := startClusterWithRetry(t, n, allocateFreePorts)
	t.Cleanup(cluster.shutdown)
	servers := cluster.servers

	for i, ns := range servers {
		if !ns.ReadyForConnections(testClusterReadyTime) {
			t.Fatalf("node %d not ready after %v", i, testClusterReadyTime)
		}
	}

	waitForRoutesMeshed(t, servers, n)
	waitForJetStreamLeader(t, servers)

	nc, err := nats.Connect(servers[0].ClientURL())
	if err != nil {
		t.Fatalf("connect to peer 0: %v", err)
	}
	t.Cleanup(func() { nc.Close() })

	confirmClusterQuorum(t, nc, n)
	return nc
}

// buildClusterRoutes returns the per-node peer route URLs. Node i
// gets routes to all j != i.
func buildClusterRoutes(t *testing.T, clusterPorts []int) [][]*url.URL {
	t.Helper()
	n := len(clusterPorts)
	routes := make([][]*url.URL, n)
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			if i == j {
				continue
			}
			raw := fmt.Sprintf("nats://127.0.0.1:%d", clusterPorts[j])
			u, err := url.Parse(raw)
			if err != nil {
				t.Fatalf("parse route %q: %v", raw, err)
			}
			routes[i] = append(routes[i], u)
		}
	}
	return routes
}

// startClusterWithRetry starts n nodes on freshly allocated ports and
// retries on new ports when a node fails to bind one. The allocator is
// injected so a test can hand it a port that is already taken.
func startClusterWithRetry(
	t *testing.T, n int, allocate func(*testing.T, int) []int,
) *testCluster {
	t.Helper()
	if allocate == nil {
		panic("startClusterWithRetry: allocate must not be nil")
	}
	var cluster *testCluster
	var err error
	for attempt := 1; attempt <= testClusterStartAttemptsMax; attempt++ {
		cluster, err = tryStartCluster(t, n, allocate)
		if err == nil {
			return cluster
		}
		t.Logf("cluster start attempt %d: %v", attempt, err)
	}
	t.Fatalf("cluster never started in %d attempts: %v", testClusterStartAttemptsMax, err)
	return nil
}

// tryStartCluster starts the nodes and waits for each to bind its
// client and route listeners. On failure it shuts every node down so
// the caller can retry on new ports.
func tryStartCluster(
	t *testing.T, n int, allocate func(*testing.T, int) []int,
) (*testCluster, error) {
	t.Helper()
	// Allocate all 2n ports in a single batch: allocateFreePorts holds
	// every listener in the batch open until all ports are chosen, so
	// one batch guarantees client and cluster ports never collide. Two
	// separate batches let the OS hand the second call a port the
	// first call had just closed and freed, silently overlapping a
	// node's client port with another node's cluster port.
	allPorts := allocate(t, 2*n)
	if len(allPorts) != 2*n {
		panic(fmt.Sprintf("tryStartCluster: allocated %d ports, want %d", len(allPorts), 2*n))
	}
	clientPorts := allPorts[:n]
	clusterPorts := allPorts[n:]
	routesByNode := buildClusterRoutes(t, clusterPorts)
	cluster, fatals := startClusterNodes(t, clientPorts, clusterPorts, routesByNode)
	for i, server := range cluster.servers {
		if err := waitListening(server, fatals[i]); err != nil {
			cluster.shutdown()
			return nil, err
		}
	}
	return cluster, nil
}

// testCluster is a running cluster and the options each node started
// from, so a test can restart a node on the same ports and store.
type testCluster struct {
	servers []*natsserver.Server
	options []*natsserver.Options
}

func (c *testCluster) shutdown() { shutdownClusterServers(c.servers) }

// restartNode shuts node i down and starts it again from its options,
// keeping its JetStream store, then waits until it serves clients and
// has rejoined the routes.
func (c *testCluster) restartNode(t *testing.T, i int) {
	t.Helper()
	if i < 0 || i >= len(c.servers) {
		panic(fmt.Sprintf("restartNode: node %d out of [0, %d)", i, len(c.servers)))
	}
	if len(c.options) != len(c.servers) {
		panic("restartNode: every node must keep its options")
	}
	c.servers[i].Shutdown()
	c.servers[i].WaitForShutdown()
	ns, err := natsserver.NewServer(c.options[i])
	if err != nil {
		t.Fatalf("restart node %d: %v", i, err)
	}
	fatal := &fatalLog{messages: make(chan string, 1)}
	ns.SetLogger(fatal, false, false)
	ns.Start()
	c.servers[i] = ns
	if err := waitListening(ns, fatal); err != nil {
		t.Fatalf("restart node %d: %v", i, err)
	}
	if !ns.ReadyForConnections(testClusterReadyTime) {
		t.Fatalf("restarted node %d not ready after %v", i, testClusterReadyTime)
	}
	waitForRoutesMeshed(t, c.servers, len(c.servers))
}

// fatalLog is a node's logger. Logging stays off except for Fatalf,
// which nats-server calls when it cannot bind a listener and then
// carries on half started; with no logger the failure is invisible.
type fatalLog struct{ messages chan string }

func (l *fatalLog) Fatalf(format string, v ...any) {
	select {
	case l.messages <- fmt.Sprintf(format, v...):
	default: // the first failure is the one that matters
	}
}

func (*fatalLog) Noticef(string, ...any) {}
func (*fatalLog) Warnf(string, ...any)   {}
func (*fatalLog) Errorf(string, ...any)  {}
func (*fatalLog) Debugf(string, ...any)  {}
func (*fatalLog) Tracef(string, ...any)  {}

// waitListening returns once the node has bound its client and route
// listeners, or with the failure it logged, within testClusterListenWait.
func waitListening(server *natsserver.Server, fatal *fatalLog) error {
	if server == nil || fatal == nil {
		panic("waitListening: server and fatal must not be nil")
	}
	deadline := time.Now().Add(testClusterListenWait)
	for time.Now().Before(deadline) {
		select {
		case message := <-fatal.messages:
			return fmt.Errorf("server %s: %s", server.Name(), message)
		default:
		}
		if server.Addr() != nil && server.ClusterAddr() != nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("server %s did not bind its listeners within %s",
		server.Name(), testClusterListenWait)
}

// startClusterNodes constructs and starts each NATS server in the
// cluster. Routes are placed on Options (not ClusterOpts) per the
// nats-server v2.12 API, matching server/nats.go startNATS.
func startClusterNodes(
	t *testing.T,
	clientPorts, clusterPorts []int,
	routesByNode [][]*url.URL,
) (*testCluster, []*fatalLog) {
	t.Helper()
	n := len(clientPorts)
	cluster := &testCluster{
		servers: make([]*natsserver.Server, n),
		options: make([]*natsserver.Options, n),
	}
	fatals := make([]*fatalLog, n)
	for i := 0; i < n; i++ {
		opts := &natsserver.Options{
			Host:       "127.0.0.1",
			Port:       clientPorts[i],
			ServerName: fmt.Sprintf("dagnats-test-%d", i),
			JetStream:  true,
			// Same reason as natstest.StartServer: an unset
			// budget is sized from the host's free disk, so a nearly
			// full disk starves peer selection and R3 stream
			// placement fails with "no suitable peers". The nodes
			// only ever hold reservations, never bytes.
			JetStreamMaxStore: testClusterMaxStoreBytes,
			StoreDir:          t.TempDir(),
			Cluster: natsserver.ClusterOpts{
				Name: "dagnats-test",
				Host: "127.0.0.1",
				Port: clusterPorts[i],
			},
			Routes: routesByNode[i],
			NoLog:  true,
			NoSigs: true,
		}
		ns, err := natsserver.NewServer(opts)
		if err != nil {
			t.Fatalf("NewServer node %d: %v", i, err)
		}
		fatals[i] = &fatalLog{messages: make(chan string, 1)}
		ns.SetLogger(fatals[i], false, false)
		ns.Start()
		cluster.servers[i] = ns
		cluster.options[i] = opts
	}
	return cluster, fatals
}

// shutdownClusterServers stops servers in reverse start order so
// peer routes drain cleanly.
func shutdownClusterServers(servers []*natsserver.Server) {
	for i := len(servers) - 1; i >= 0; i-- {
		if servers[i] != nil {
			servers[i].Shutdown()
			servers[i].WaitForShutdown()
		}
	}
}

// waitForRoutesMeshed polls until each server has connected to all
// (n-1) peers via cluster routes, or fails the test on timeout.
func waitForRoutesMeshed(t *testing.T, servers []*natsserver.Server, n int) {
	t.Helper()
	deadline := time.Now().Add(testClusterFormTime)
	for time.Now().Before(deadline) {
		allReady := true
		for _, ns := range servers {
			if ns.NumRoutes() < n-1 {
				allReady = false
				break
			}
		}
		if allReady {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	for i, ns := range servers {
		if ns.NumRoutes() < n-1 {
			t.Fatalf("node %d: NumRoutes=%d, want %d",
				i, ns.NumRoutes(), n-1)
		}
	}
}

// waitForJetStreamLeader polls until one server reports it is the
// JetStream meta-leader, or fails the test on timeout.
func waitForJetStreamLeader(t *testing.T, servers []*natsserver.Server) {
	t.Helper()
	deadline := time.Now().Add(testClusterFormTime)
	for time.Now().Before(deadline) {
		for _, ns := range servers {
			if ns.JetStreamIsLeader() {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("StartTestCluster: no JetStream meta-leader elected")
}

// confirmClusterQuorum verifies the cluster is API-healthy by calling
// AccountInfo via the production WaitForClusterQuorum helper, then
// probes stream placement at R=n by creating and deleting a throwaway
// stream. The placement probe ensures all n peers are actually ready
// to host stream replicas — AccountInfo alone can return success
// before all peers' JS subsystems are fully synced for placement.
func confirmClusterQuorum(t *testing.T, nc *nats.Conn, n int) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testClusterQuorumTTL)
	defer cancel()
	if _, err := natsutil.WaitForClusterQuorum(ctx, js, n); err != nil {
		t.Fatalf("cluster did not form: %v", err)
	}
	probeStreamPlacement(ctx, t, js, n)
}

// probeStreamPlacement creates and deletes a throwaway R=n stream,
// retrying on "peer offline" errors that occur when peers' JS API is
// up but their meta state hasn't fully synced for placement. Returns
// once placement succeeds; fails the test when ctx is cancelled.
//
// The retry interval is bounded by placementProbeInterval and the
// total wait by the caller's ctx — there is no separate attempt cap.
func probeStreamPlacement(
	ctx context.Context, t *testing.T, js jetstream.JetStream, n int,
) {
	if t == nil {
		panic("probeStreamPlacement: t is nil")
	}
	if js == nil {
		panic("probeStreamPlacement: js is nil")
	}
	if n < 1 {
		panic(fmt.Sprintf("probeStreamPlacement: n=%d", n))
	}
	t.Helper()

	const probeName = "dagnats_test_placement_probe"
	cfg := jetstream.StreamConfig{
		Name:     probeName,
		Subjects: []string{"_dagnats.test.probe"},
		Replicas: n,
	}

	var lastErr error
	for {
		_, err := js.CreateOrUpdateStream(ctx, cfg)
		if err == nil {
			if delErr := js.DeleteStream(ctx, probeName); delErr != nil {
				t.Fatalf("probeStreamPlacement: cleanup: %v", delErr)
			}
			return
		}
		lastErr = err

		select {
		case <-ctx.Done():
			t.Fatalf("probeStreamPlacement: ctx done before R=%d placement (last err: %v)",
				n, lastErr)
			return
		case <-time.After(placementProbeInterval):
			// retry
		}
	}
}

// allocateFreePorts finds count available TCP ports by binding 127.0.0.1:0
// then closing. Hands the freed ports to nats-server. Subject to a small
// race window, but matches the supercluster harness's approach.
func allocateFreePorts(t *testing.T, count int) []int {
	t.Helper()
	if count < 1 {
		panic(fmt.Sprintf("allocateFreePorts: count=%d", count))
	}
	ports := make([]int, count)
	listeners := make([]net.Listener, count)
	for i := 0; i < count; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("net.Listen: %v", err)
		}
		listeners[i] = l
		ports[i] = l.Addr().(*net.TCPAddr).Port
	}
	for _, l := range listeners {
		_ = l.Close()
	}
	return ports
}
