// journal/cluster_start_test.go
// The cluster fixture reserves its route ports by binding and closing
// them, so another process can take one before the node binds it (any
// outgoing connection may draw it as its ephemeral source port). The
// node then serves clients without a route listener, and nats-server,
// with logging off, reports the bind failure nowhere.
//
// Methodology: the first port the fixture reserves is held by a live
// listener for the whole test, which is the race lost deterministically.
// Positive: the fixture still returns a quorate cluster, fast. Negative:
// the held port never becomes a node's route port.
package journal

import (
	"net"
	"testing"
	"time"
)

func TestStartCluster_RetriesWhenARoutePortIsTaken(t *testing.T) {
	if testing.Short() {
		t.Skip("three-node cluster skipped in -short mode")
	}
	held, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("hold a port: %v", err)
	}
	defer held.Close()
	heldPort := held.Addr().(*net.TCPAddr).Port
	reserved := 0
	reserve := func(t *testing.T) int {
		reserved++
		if reserved == 1 {
			return heldPort
		}
		return reservePort(t)
	}
	start := time.Now()
	nc := startClusterWith(t, reserve)
	if !nc.IsConnected() {
		t.Fatal("cluster connection is not connected")
	}
	if elapsed := time.Since(start); elapsed > clusterListenWait {
		t.Fatalf("cluster took %s: the taken port was waited on, not detected", elapsed)
	}
	if reserved <= clusterNodes {
		t.Fatalf("reserved %d ports: the taken one was never replaced", reserved)
	}
}
