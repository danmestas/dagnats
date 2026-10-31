// The race fixture reserves its route ports by binding and closing
// them, so another process can take one before the node binds it. The
// node then serves clients without a route listener and, with logging
// off, nats-server reports the bind failure nowhere.
//
// Methodology: the first port the fixture reserves is held by a live
// listener for the whole test, which is the race lost deterministically.
// Positive: the fixture starts every node, fast. Negative: the held
// port never becomes a node's route port.
package natsutil

import (
	"net"
	"testing"
	"time"
)

func TestStartRaceNodes_RetriesWhenARoutePortIsTaken(t *testing.T) {
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hold a port: %v", err)
	}
	defer held.Close()
	heldPort := held.Addr().(*net.TCPAddr).Port
	const nodeCount = 3
	reserved := 0
	reserve := func(t *testing.T) int {
		reserved++
		if reserved == 1 {
			return heldPort
		}
		return reservePort(t)
	}
	start := time.Now()
	servers := startRaceNodesWith(t, nodeCount, reserve)
	if len(servers) != nodeCount {
		t.Fatalf("got %d servers, want %d", len(servers), nodeCount)
	}
	if elapsed := time.Since(start); elapsed > raceListenWait {
		t.Fatalf("start took %s: the taken port was waited on, not detected", elapsed)
	}
	if reserved <= nodeCount {
		t.Fatalf("reserved %d ports: the taken one was never replaced", reserved)
	}
	for i, server := range servers {
		if port := server.ClusterAddr().Port; port == heldPort {
			t.Fatalf("node %d routes on the held port %d", i, port)
		}
	}
}
