// Methodology: end-to-end integration tests against a real in-process
// NATS cluster (via dagnatstest.StartTestCluster). These exercise the
// real failure modes the cluster-mode spec calls out — cold-start
// stream creation at R=3, in-place migration of pre-existing R=1
// streams to R=3, the explicit override path, and bootstrap timing
// on a healthy cluster. No mocks; tests fail if the actual JetStream
// replica counts on the running cluster don't match expectations.
package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/natsutil"
)

// TestCluster_FreshClusterStreamsAtR3 verifies that SetupAll on a
// 3-node test cluster creates streams at R=3.
func TestCluster_FreshClusterStreamsAtR3(t *testing.T) {
	nc := dagnatstest.StartTestCluster(t, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	routes := []string{"a", "b"} // simulated peers — only len matters for R-derivation
	if err := natsutil.SetupAll(nc,
		natsutil.WithCluster(natsutil.ClusterOptions{Routes: routes}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	s, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if got := s.CachedInfo().Config.Replicas; got != 3 {
		t.Errorf("Replicas = %d, want 3", got)
	}
}

// TestCluster_MigrateR1ToR3 verifies that an existing R=1 stream
// upgrades to R=3 when SetupAll is re-run with cluster routes set.
func TestCluster_MigrateR1ToR3(t *testing.T) {
	nc := dagnatstest.StartTestCluster(t, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// First: pretend this was a single-binary deployment — create at R=1.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := natsutil.SetupStreams(js, 1, 0); err != nil {
		t.Fatalf("SetupStreams R=1: %v", err)
	}
	if err := natsutil.SetupKVBuckets(js, 1); err != nil {
		t.Fatalf("SetupKVBuckets R=1: %v", err)
	}

	// Now run SetupAll with cluster routes; should upgrade R=1 -> R=3.
	routes := []string{"a", "b"}
	if err := natsutil.SetupAll(nc,
		natsutil.WithCluster(natsutil.ClusterOptions{Routes: routes}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	s, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if got := s.CachedInfo().Config.Replicas; got != 3 {
		t.Errorf("Replicas = %d, want 3 after upgrade", got)
	}
}

// TestCluster_OverrideHonored verifies that ReplicasOverride beats
// auto-derive even on a 5-node cluster.
func TestCluster_OverrideHonored(t *testing.T) {
	nc := dagnatstest.StartTestCluster(t, 5)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	routes := []string{"a", "b", "c", "d"}
	if err := natsutil.SetupAll(nc,
		natsutil.WithCluster(natsutil.ClusterOptions{
			Routes:           routes,
			ReplicasOverride: 3,
		}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	s, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	if got := s.CachedInfo().Config.Replicas; got != 3 {
		t.Errorf("Replicas = %d, want 3 (override)", got)
	}
}

// setupSanityBudget is the wall-clock ceiling for SetupAll on a healthy
// 3-node cluster. This test is a regression sanity check against a genuine
// setup-path hang (quorum never forming, the historical client/cluster port
// collision, streams never placing) — SetupAll would then run into its
// internal 60s quorum timeout. It is NOT a performance benchmark, so the
// ceiling is chosen to catch a real hang while tolerating scheduler
// contention on a loaded CI runner running the suite at `-p 4`.
//
// Measured (14-core dev host, SetupAll wall time creating the R=3 streams):
//   - isolated:                    ~2.1s (stable across 10 runs)
//   - 6 concurrent NATS load loops: ~3s
//   - 24 concurrent load loops:     ~6s
//   - 48 load loops (3.4x CPU over-subscription, worse than real `-p 4`):
//     13-20s, which is where the old 10s ceiling flaked (11/12 samples over).
//
// The real full-suite `-p 4` flake in issue #588 measured 12.15s. 30s gives
// ~2.5x margin over that observed worst case and ~1.5x over the pathological
// 48-loop max, while sitting at exactly half SetupAll's internal 60s quorum
// timeout — so a genuine hang forming still trips it well before the internal
// bound, and mere contention never does.
const setupSanityBudget = 30 * time.Second

// TestCluster_HealthyClusterFastSetup verifies that SetupAll completes
// well under SetupAll's internal 60s quorum timeout on a healthy 3-node
// cluster — a sanity check on bootstrap timing, not a performance benchmark.
func TestCluster_HealthyClusterFastSetup(t *testing.T) {
	nc := dagnatstest.StartTestCluster(t, 3)

	routes := []string{"a", "b"}
	start := time.Now()
	if err := natsutil.SetupAll(nc,
		natsutil.WithCluster(natsutil.ClusterOptions{Routes: routes}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	elapsed := time.Since(start)
	t.Logf("SetupAll on healthy 3-node cluster took %v (budget %v)",
		elapsed, setupSanityBudget)
	if elapsed > setupSanityBudget {
		t.Errorf("SetupAll took %v on healthy 3-node cluster; expected <%v",
			elapsed, setupSanityBudget)
	}
}
