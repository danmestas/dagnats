// The cluster helper allocates its ports by binding and closing them,
// so another process can take one before the node binds it (any
// outgoing connection may draw it as its ephemeral source port). The
// node then serves clients without a route listener, and nats-server,
// with logging off, reports the bind failure nowhere.
//
// Methodology: the first allocation swaps one route port for a port a
// live listener holds, which is the race lost deterministically.
// Positive: the helper returns a started cluster, fast. Negative: the
// held port never becomes a node's route port.
package dagnatstest

import (
	"net"
	"testing"
	"time"
)

func TestStartClusterWithRetry_RecoversWhenARoutePortIsTaken(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hold a port: %v", err)
	}
	defer held.Close()
	heldPort := held.Addr().(*net.TCPAddr).Port
	const nodeCount = minTestClusterNodes
	allocations := 0
	allocate := func(t *testing.T, count int) []int {
		allocations++
		ports := allocateFreePorts(t, count)
		if allocations == 1 {
			ports[nodeCount] = heldPort // the first route port
		}
		return ports
	}
	start := time.Now()
	servers := startClusterWithRetry(t, nodeCount, allocate)
	t.Cleanup(func() { shutdownClusterServers(servers) })
	if len(servers) != nodeCount {
		t.Fatalf("got %d servers, want %d", len(servers), nodeCount)
	}
	if elapsed := time.Since(start); elapsed > testClusterListenWait {
		t.Fatalf("start took %s: the taken port was waited on, not detected", elapsed)
	}
	if allocations < 2 {
		t.Fatalf("allocated %d times: the taken port was never replaced", allocations)
	}
	for i, server := range servers {
		if port := server.ClusterAddr().Port; port == heldPort {
			t.Fatalf("node %d routes on the held port %d", i, port)
		}
	}
}
