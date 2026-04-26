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
)

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

	clientPorts := allocateFreePorts(t, n)
	clusterPorts := allocateFreePorts(t, n)
	routesByNode := buildClusterRoutes(t, clusterPorts)
	servers := startClusterNodes(t, clientPorts, clusterPorts, routesByNode)

	t.Cleanup(func() { shutdownClusterServers(servers) })

	for i, ns := range servers {
		if !ns.ReadyForConnections(testClusterReadyTime) {
			t.Fatalf("node %d not ready after %v", i, testClusterReadyTime)
		}
	}

	waitForRoutesMeshed(t, servers, n)
	waitForJetStreamLeader(t, servers)

	url := fmt.Sprintf("nats://127.0.0.1:%d", clientPorts[0])
	nc, err := nats.Connect(url)
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

// startClusterNodes constructs and starts each NATS server in the
// cluster. Routes are placed on Options (not ClusterOpts) per the
// nats-server v2.12 API, matching server/nats.go startNATS.
func startClusterNodes(
	t *testing.T,
	clientPorts, clusterPorts []int,
	routesByNode [][]*url.URL,
) []*natsserver.Server {
	t.Helper()
	n := len(clientPorts)
	servers := make([]*natsserver.Server, n)
	for i := 0; i < n; i++ {
		opts := &natsserver.Options{
			Host:       "127.0.0.1",
			Port:       clientPorts[i],
			ServerName: fmt.Sprintf("dagnats-test-%d", i),
			JetStream:  true,
			StoreDir:   t.TempDir(),
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
		ns.Start()
		servers[i] = ns
	}
	return servers
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
	t.Helper()
	if t == nil {
		panic("probeStreamPlacement: t is nil")
	}
	if js == nil {
		panic("probeStreamPlacement: js is nil")
	}
	if n < 1 {
		panic(fmt.Sprintf("probeStreamPlacement: n=%d", n))
	}

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
