// Methodology: integration tests against real in-process NATS clusters
// (dagnatstest.StartTestCluster) for the replica-count rules a caller that
// passes no cluster settings depends on (#784 item 13). The split binaries
// (dagnats-engine, dagnats-api) call SetupAll with no routes, so SetupAll
// must derive the factor from the cluster it is connected to, and no
// provisioning call may shrink a stream or bucket that already has more
// replicas than it asked for. No mocks: every assertion reads the replica
// count JetStream reports for the live resource.
package server_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/natsutil"
)

// TestCluster_SetupAllWithoutRoutesDerivesR3 is the split-binary path: no
// routes and no override, connected to a 3-node cluster.
func TestCluster_SetupAllWithoutRoutesDerivesR3(t *testing.T) {
	nc := dagnatstest.StartTestCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	assertAllProvisionedReplicated(t, ctx, js, 3)
	assertNoProbeStreamsLeft(t, ctx, js)
}

// TestCluster_SetupAllWithoutRoutesDerivesR5 checks the top of the mapping
// (5+ nodes -> 5) the same way.
func TestCluster_SetupAllWithoutRoutesDerivesR5(t *testing.T) {
	nc := dagnatstest.StartTestCluster(t, 5)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	assertAllProvisionedReplicated(t, ctx, js, 5)
	assertNoProbeStreamsLeft(t, ctx, js)
}

// TestCluster_LowerReplicaRequestKeepsExisting provisions at R3, then asks
// for one replica three ways (the per-resource setup calls, EnsureKeyValue,
// and SetupAll with an override of 1). None of them may shrink anything.
func TestCluster_LowerReplicaRequestKeepsExisting(t *testing.T) {
	nc := dagnatstest.StartTestCluster(t, 3)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := natsutil.SetupAll(nc, natsutil.WithCluster(
		natsutil.ClusterOptions{Routes: []string{"a", "b"}},
	)); err != nil {
		t.Fatalf("SetupAll R3: %v", err)
	}
	assertAllProvisionedReplicated(t, ctx, js, 3)

	if err := natsutil.SetupStreams(js, 1, 0); err != nil {
		t.Fatalf("SetupStreams R1: %v", err)
	}
	if err := natsutil.SetupKVBuckets(js, 1); err != nil {
		t.Fatalf("SetupKVBuckets R1: %v", err)
	}
	_, err = natsutil.EnsureKeyValue(ctx, js, jetstream.KeyValueConfig{
		Bucket: "workflow_defs", Replicas: 1,
	})
	if err != nil {
		t.Fatalf("EnsureKeyValue R1: %v", err)
	}
	if err := natsutil.SetupAll(nc, natsutil.WithCluster(
		natsutil.ClusterOptions{ReplicasOverride: 1},
	)); err != nil {
		t.Fatalf("SetupAll override 1: %v", err)
	}
	assertAllProvisionedReplicated(t, ctx, js, 3)
}

// assertNoProbeStreamsLeft fails if replica detection left a throwaway
// stream behind: it would hold a store reservation forever.
func assertNoProbeStreamsLeft(
	t *testing.T, ctx context.Context, js jetstream.JetStream,
) {
	t.Helper()
	names := js.StreamNames(ctx)
	count := 0
	for name := range names.Name() {
		count++
		if strings.HasPrefix(name, natsutil.ReplicaProbeStreamPrefix) {
			t.Errorf("probe stream %s left behind", name)
		}
	}
	if err := names.Err(); err != nil {
		t.Fatalf("StreamNames: %v", err)
	}
	if count == 0 {
		t.Fatal("StreamNames listed nothing; SetupAll created no streams")
	}
}
