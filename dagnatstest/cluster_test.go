package dagnatstest

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// TestAllocateFreePorts_BatchIsUnique guards the single-batch uniqueness
// invariant that StartTestCluster's port allocation relies on. The
// CI flake this regresses against was NOT intra-batch duplication —
// allocateFreePorts already holds every listener in a batch open until
// all `count` ports are chosen, so a single call never repeats a port.
// The flake was CROSS-call reuse: StartTestCluster used to call
// allocateFreePorts twice (once for client ports, once for cluster
// ports), and each call closes its listeners before returning, so the
// OS was free to hand the second call a port the first call had just
// freed. That let a node's client port collide with another node's
// cluster port, breaking bind and producing an intermittently broken
// cluster. Allocating all 2n ports in one batch (see StartTestCluster)
// prevents that reuse; this test locks in that a large single batch
// never contains a duplicate.
func TestAllocateFreePorts_BatchIsUnique(t *testing.T) {
	const count = 2 * maxTestClusterNodes
	ports := allocateFreePorts(t, count)

	if len(ports) != count {
		t.Fatalf("allocateFreePorts returned %d ports, want %d", len(ports), count)
	}

	seen := make(map[int]bool, count)
	for i, port := range ports {
		if port == 0 {
			t.Fatalf("ports[%d] is 0, want a real allocated port", i)
		}
		if seen[port] {
			t.Fatalf("duplicate port %d in single batch of %d", port, count)
		}
		seen[port] = true
	}
	if len(seen) != count {
		t.Fatalf("got %d unique ports, want %d", len(seen), count)
	}
}

// TestStartTestCluster_3Nodes verifies that StartTestCluster brings up
// a 3-node cluster whose JetStream API responds to AccountInfo. We do
// not assert info.API.Errors == 0: that field is a monotonic lifetime
// counter, not a current-health signal, and the placement probe in
// confirmClusterQuorum may bump it during retries while peers settle.
// The semantic check is that AccountInfo succeeds.
func TestStartTestCluster_3Nodes(t *testing.T) {
	nc := StartTestCluster(t, 3)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	info, err := js.AccountInfo(ctx)
	if err != nil {
		t.Fatalf("AccountInfo: %v", err)
	}
	if info == nil {
		t.Fatal("AccountInfo nil")
	}
}
