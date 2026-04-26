package dagnatstest

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

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
