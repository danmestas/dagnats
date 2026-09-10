// Test methodology: integration tests with real embedded NATS servers,
// reopening the SAME store directory under different max_store_bytes
// values exactly like an operator's `systemctl edit dagnats` + restart.
// Bounded timeouts on every JetStream round trip; positive and negative
// space.
package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/natsutil"
)

func setupOptsForBudget(budget int64) []natsutil.SetupOption {
	return []natsutil.SetupOption{natsutil.WithStoreBudget(budget)}
}

// TestStartNATSAndSetupAll_BudgetThatFitsStartsOnce is the (d) case: when
// cfg.MaxStoreBytes already covers what is reserved (a fresh store, here),
// the effective JetStreamMaxStore must equal the configured value exactly
// -- no shrink pass, no inflation.
func TestStartNATSAndSetupAll_BudgetThatFitsStartsOnce(t *testing.T) {
	const budget int64 = 1 << 30
	cfg := Config{DataDir: t.TempDir(), NATSPort: -1, MaxStoreBytes: budget}

	ns, nc, err := startNATSAndSetupAll(cfg, setupOptsForBudget(budget), nil)
	if err != nil {
		t.Fatalf("startNATSAndSetupAll: %v", err)
	}
	t.Cleanup(func() { nc.Close(); ns.Shutdown(); ns.WaitForShutdown() })

	jsCfg := ns.JetStreamConfig()
	if jsCfg == nil {
		t.Fatal("JetStreamConfig() = nil, want JetStream enabled")
	}
	if jsCfg.MaxStore != budget {
		t.Fatalf("JetStreamMaxStore = %d, want exactly the configured "+
			"budget %d (no shrink pass needed)", jsCfg.MaxStore, budget)
	}
}

// TestStartNATSAndSetupAll_AutoShrinksLoweredBudget is the (a)+(b) red test
// replacing #686's refusal: lowering max_store_bytes below what an
// established store already reserves must now SUCCEED in one restart,
// shrinking every stream's MaxBytes to the new budget's fractional
// ceilings, and a FOLLOWING restart at that same budget must enforce it
// exactly with no lingering inflation.
func TestStartNATSAndSetupAll_AutoShrinksLoweredBudget(t *testing.T) {
	dataDir := t.TempDir()
	const bigBudget int64 = 1 << 30
	const smallBudget int64 = 1 << 27

	ns1, nc1, err := startNATSAndSetupAll(
		Config{DataDir: dataDir, NATSPort: -1, MaxStoreBytes: bigBudget},
		setupOptsForBudget(bigBudget), nil,
	)
	if err != nil {
		t.Fatalf("initial startNATSAndSetupAll under the large budget: %v", err)
	}
	nc1.Close()
	ns1.Shutdown()
	ns1.WaitForShutdown()

	ns2, nc2, err := startNATSAndSetupAll(
		Config{DataDir: dataDir, NATSPort: -1, MaxStoreBytes: smallBudget},
		setupOptsForBudget(smallBudget), nil,
	)
	if err != nil {
		t.Fatalf("startNATSAndSetupAll with a lowered budget must "+
			"auto-recover, not refuse: %v", err)
	}
	// (c): THIS boot is the one-time recovery restart, so its JetStream-
	// MaxStore is deliberately the temporarily inflated effective limit,
	// not the configured budget -- the configured value takes full effect
	// only on the FOLLOWING restart (checked further down).
	jsCfg := ns2.JetStreamConfig()
	if jsCfg == nil {
		t.Fatal("JetStreamConfig() = nil, want JetStream enabled")
	}
	if jsCfg.MaxStore <= smallBudget {
		t.Fatalf("JetStreamMaxStore = %d, want temporarily ABOVE the "+
			"configured budget %d on the recovery boot (it must admit "+
			"what is already reserved)", jsCfg.MaxStore, smallBudget)
	}

	// (a): streams' MaxBytes now equal the NEW budget's fractional
	// ceilings, proving the shrink pass actually applied.
	js, err := jetstream.New(nc2)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream(WORKFLOW_HISTORY): %v", err)
	}
	const fractionWorkflowHistory = 0.25 // mirrors natsutil's unexported fraction
	want := int64(float64(smallBudget) * fractionWorkflowHistory)
	got := stream.CachedInfo().Config.MaxBytes
	if got != want {
		t.Fatalf("WORKFLOW_HISTORY MaxBytes = %d, want %d (the small "+
			"budget's ceiling)", got, want)
	}
	bigCeiling := int64(float64(bigBudget) * fractionWorkflowHistory)
	if got >= bigCeiling {
		t.Fatalf("WORKFLOW_HISTORY MaxBytes = %d, still at (or above) the "+
			"big-budget ceiling %d; the shrink did not apply", got, bigCeiling)
	}

	// A further restart at the SAME (already-shrunk) budget needs no
	// recovery and must succeed cleanly.
	nc2.Close()
	ns2.Shutdown()
	ns2.WaitForShutdown()
	ns3, nc3, err := startNATSAndSetupAll(
		Config{DataDir: dataDir, NATSPort: -1, MaxStoreBytes: smallBudget},
		setupOptsForBudget(smallBudget), nil,
	)
	if err != nil {
		t.Fatalf("restart at the already-shrunk configured budget must "+
			"not be refused: %v", err)
	}
	// (b): this second restart is the one that carries the operator's
	// configured budget with no lingering inflation -- nothing was left to
	// shrink, so there was no recovery restart to inflate it.
	jsCfg3 := ns3.JetStreamConfig()
	if jsCfg3 == nil {
		t.Fatal("JetStreamConfig() = nil, want JetStream enabled")
	}
	if jsCfg3.MaxStore != smallBudget {
		t.Fatalf("second restart JetStreamMaxStore = %d, want exactly "+
			"the configured budget %d, no lingering inflation",
			jsCfg3.MaxStore, smallBudget)
	}
	nc3.Close()
	ns3.Shutdown()
	ns3.WaitForShutdown()
}

// TestEffectiveStoreBudget covers effectiveStoreBudget's two branches
// directly: (c) reserved+headroom exactly, when configured is too small;
// (d) configured verbatim, when it already covers reserved.
func TestEffectiveStoreBudget(t *testing.T) {
	const configured = int64(1) << 30
	const smallReserved = int64(1) << 20
	if got := effectiveStoreBudget(configured, smallReserved); got != configured {
		t.Fatalf("effectiveStoreBudget(configured >= reserved) = %d, "+
			"want configured %d unchanged", got, configured)
	}

	const smallConfigured = int64(1) << 27
	const bigReserved = int64(819) << 20
	want := bigReserved + 64<<20 // pinned literal: the headroom constant itself, not derived from it
	got := effectiveStoreBudget(smallConfigured, bigReserved)
	if got != want {
		t.Fatalf("effectiveStoreBudget(configured < reserved) = %d, "+
			"want reserved+headroom exactly (%d)", got, want)
	}
	if got < bigReserved {
		t.Fatalf("effectiveStoreBudget = %d, must be >= reserved %d "+
			"(the process must admit what is already on disk)", got, bigReserved)
	}
}

// TestStartNATSAndSetupAll_NonConvergentShrinkReturnsActionableError covers
// the case runShrinkPass's recheck exists for: a stream SetupAll does not
// manage (operator-created here, standing in for a mirror or an orphan)
// still holds the aggregate over budget after every MANAGED stream has
// shrunk. Without the recheck this would report success and silently
// re-enter recovery, inflated, on every future boot; it must instead return
// the same actionable *natsutil.StoreBudgetExceededError #686 produces,
// naming the unmanaged stream as one of the largest reservations.
func TestStartNATSAndSetupAll_NonConvergentShrinkReturnsActionableError(t *testing.T) {
	dataDir := t.TempDir()
	const bigBudget int64 = 1 << 30
	const smallBudget int64 = 1 << 27
	const unmanagedBytes int64 = 150 << 20

	ns1, nc1, err := startNATSAndSetupAll(
		Config{DataDir: dataDir, NATSPort: -1, MaxStoreBytes: bigBudget},
		setupOptsForBudget(bigBudget), nil,
	)
	if err != nil {
		t.Fatalf("initial startNATSAndSetupAll under the large budget: %v", err)
	}

	js, err := jetstream.New(nc1)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	createCtx, createCancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, err = js.CreateStream(createCtx, jetstream.StreamConfig{
		Name:     "UNMANAGED_STREAM",
		Subjects: []string{"unmanaged.>"},
		Storage:  jetstream.FileStorage,
		MaxBytes: unmanagedBytes,
	})
	createCancel()
	if err != nil {
		t.Fatalf("create unmanaged stream: %v", err)
	}
	nc1.Close()
	ns1.Shutdown()
	ns1.WaitForShutdown()

	_, _, err = startNATSAndSetupAll(
		Config{DataDir: dataDir, NATSPort: -1, MaxStoreBytes: smallBudget},
		setupOptsForBudget(smallBudget), nil,
	)
	if err == nil {
		t.Fatal("startNATSAndSetupAll succeeded despite an unmanaged " +
			"stream still holding the aggregate over budget; want the " +
			"actionable error, not a false success")
	}

	var budgetErr *natsutil.StoreBudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("error is not a *natsutil.StoreBudgetExceededError: %v", err)
	}
	if budgetErr.Budget != smallBudget {
		t.Errorf("budgetErr.Budget = %d, want %d", budgetErr.Budget, smallBudget)
	}
	named := false
	for _, r := range budgetErr.Largest {
		if r.Name == "UNMANAGED_STREAM" {
			named = true
		}
	}
	if !named {
		t.Errorf("error does not name UNMANAGED_STREAM among the largest "+
			"reservations, so the operator has nothing to act on: %+v",
			budgetErr.Largest)
	}
}
