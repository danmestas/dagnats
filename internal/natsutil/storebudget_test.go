// natsutil/storebudget_test.go
// Tests for store-budget resolution. A caller that does not declare a
// budget must size its proportional stream ceilings against the budget the
// CONNECTED server actually reports (jetstream.AccountInfo Limits.MaxStore)
// rather than the hardcoded defaultMaxStoreBytes guess — the guess exceeded
// the embedded test server's disk-derived budget on any host with < ~10 GiB
// free, failing every JetStream test with err 10047. Methodology: assert the
// pure limit→budget mapping in isolation (unlimited/fallback cases), then
// assert end-to-end against a real embedded server that SetupAll's ceilings
// track the server's bounded budget. Bounded 5-second timeout on all ops.
package natsutil

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// TestStoreBudgetFromLimit covers the pure mapping: a positive server limit
// is used verbatim; NATS's "unlimited" sentinel (<= 0) falls back to the
// default so fractions never derive a ceiling from an unbounded budget.
func TestStoreBudgetFromLimit(t *testing.T) {
	const serverLimit = int64(1 << 30) // 1 GiB
	if got := storeBudgetFromLimit(serverLimit); got != serverLimit {
		t.Fatalf("storeBudgetFromLimit(%d) = %d, want %d",
			serverLimit, got, serverLimit)
	}
	// Negative space: unlimited (0 and -1) must NOT pass through as a
	// <= 0 budget, which would silently disable every ceiling.
	for _, unlimited := range []int64{0, -1} {
		got := storeBudgetFromLimit(unlimited)
		if got != defaultMaxStoreBytes {
			t.Fatalf("storeBudgetFromLimit(%d) = %d, want default %d",
				unlimited, got, defaultMaxStoreBytes)
		}
		if got <= 0 {
			t.Fatalf("storeBudgetFromLimit(%d) = %d, want positive",
				unlimited, got)
		}
	}
}

// TestStartTestServerBoundsStore guards hermeticity: the embedded test
// server must carry an explicit, modest JetStreamMaxStore instead of
// inheriting whatever free disk the host happens to have.
func TestStartTestServerBoundsStore(t *testing.T) {
	ns, _ := StartTestServer(t)
	cfg := ns.JetStreamConfig()
	if cfg == nil {
		t.Fatal("JetStreamConfig() = nil, want JetStream enabled")
	}
	want := testServerMaxStoreBytes + testServerMaxStoreHeadroomBytes
	if cfg.MaxStore != want {
		t.Fatalf("JetStreamMaxStore = %d, want %d", cfg.MaxStore, want)
	}
	// Negative space: the budget must be genuinely modest, so the suite
	// runs on a nearly-full disk that cannot satisfy the 10 GiB default.
	if cfg.MaxStore >= defaultMaxStoreBytes {
		t.Fatalf("JetStreamMaxStore = %d, want well under default %d",
			cfg.MaxStore, defaultMaxStoreBytes)
	}
}

// TestResolveStoreBudgetReadsServerLimit asserts the resolver reports the
// connected server's real limit, not the compiled-in default.
func TestResolveStoreBudgetReadsServerLimit(t *testing.T) {
	_, nc := StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	budget, err := resolveStoreBudget(ctx, js)
	if err != nil {
		t.Fatalf("resolveStoreBudget: %v", err)
	}
	if budget != testServerMaxStoreBytes {
		t.Fatalf("resolveStoreBudget = %d, want server limit %d",
			budget, testServerMaxStoreBytes)
	}
	if budget == defaultMaxStoreBytes {
		t.Fatalf("resolveStoreBudget = %d, want the server limit, "+
			"not the hardcoded default", budget)
	}
}

// TestSetupAllSizesCeilingsFromServer is the regression guard for the
// original defect: SetupAll with no WithStoreBudget must succeed against a
// server whose budget is far below defaultMaxStoreBytes, and must size its
// ceilings from that server's budget.
func TestSetupAllSizesCeilingsFromServer(t *testing.T) {
	_, nc := StartTestServer(t)
	if err := SetupAll(nc); err != nil {
		t.Fatalf("SetupAll with no explicit budget: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream(WORKFLOW_HISTORY): %v", err)
	}

	want := proportionalMaxBytes(
		testServerMaxStoreBytes, fractionWorkflowHistory,
	)
	got := stream.CachedInfo().Config.MaxBytes
	if got != want {
		t.Fatalf("WORKFLOW_HISTORY MaxBytes = %d, want %d", got, want)
	}
	// Negative space: the ceiling must not have been derived from the
	// 10 GiB default, which is what overran the server's budget.
	fromDefault := proportionalMaxBytes(
		defaultMaxStoreBytes, fractionWorkflowHistory,
	)
	if got == fromDefault {
		t.Fatalf("WORKFLOW_HISTORY MaxBytes = %d, derived from the "+
			"default budget instead of the server's", got)
	}
}

// TestSetupAllExplicitBudgetWins asserts WithStoreBudget still overrides
// the server-derived budget unchanged.
func TestSetupAllExplicitBudgetWins(t *testing.T) {
	const explicit = int64(512 * 1024 * 1024)
	_, nc := StartTestServer(t)
	if err := SetupAll(nc, WithStoreBudget(explicit)); err != nil {
		t.Fatalf("SetupAll with explicit budget: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream(WORKFLOW_HISTORY): %v", err)
	}

	want := proportionalMaxBytes(explicit, fractionWorkflowHistory)
	got := stream.CachedInfo().Config.MaxBytes
	if got != want {
		t.Fatalf("WORKFLOW_HISTORY MaxBytes = %d, want explicit-derived %d",
			got, want)
	}
	// Negative space: the explicit budget must beat the server's limit.
	fromServer := proportionalMaxBytes(
		testServerMaxStoreBytes, fractionWorkflowHistory,
	)
	if got == fromServer {
		t.Fatalf("WORKFLOW_HISTORY MaxBytes = %d, explicit budget was "+
			"overridden by the server limit", got)
	}
}
