package natsutil

import (
	"context"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// startServerOnDir starts an embedded JetStream server over an EXISTING store
// directory with an explicit MaxStore budget.
//
// No account-limit mirroring here, deliberately: server.go passes the budget
// to SetupAll explicitly via WithStoreBudget, so nothing resolves it from
// AccountInfo and the server-level JetStreamMaxStore is what enforces. These
// tests take the same path production does.
//
// StartTestServer pins MaxStore to a constant and owns a fresh directory, so
// it cannot express the scenario this file is about: the SAME store, reopened
// under a SMALLER budget. That is the production shape, because server.go
// starts the embedded server with JetStreamMaxStore: cfg.MaxStoreBytes — the
// very value that also sizes the stream ceilings.
func startServerOnDir(t *testing.T, storeDir string, maxStore int64) *nats.Conn {
	t.Helper()
	ns, err := natsserver.NewServer(&natsserver.Options{
		Host:              "127.0.0.1",
		Port:              -1,
		JetStream:         true,
		JetStreamMaxStore: maxStore,
		StoreDir:          storeDir,
	})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("server not ready after 5s")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		ns.Shutdown()
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	return nc
}

// TestSetupAll_LoweredBudgetOverExistingStoreIsActionable is the demonstrated
// failing case for #685, in the shape production hits it against a single
// connection.
//
// A store is established under a large budget, then reopened under a small
// one, exactly as `systemctl edit dagnats` + restart does. Every stream
// assertion is then refused: JetStream admits a stream only if its MaxBytes
// fits the budget remaining after every OTHER stream's, and the streams
// already on disk reserve more than the whole new budget. That includes the
// updates that would have shrunk the ceilings, so a single SetupAll call
// cannot lower its own budget -- doing that requires restarting the NATS
// process at a temporarily larger limit, which only an embedded-server
// caller that owns the process can do (see server.startNATSAndSetupAll and
// TestSetupAll_LoweredBudgetRecoversAfterRestart below, which drives that
// exact restart through these same natsutil primitives). This test is the
// natsutil-level contract that recovery is built on: SetupAll must still
// refuse a bare reopen, and must refuse with a typed, actionable error.
//
// Before the fix this surfaced as a bare "insufficient storage resources
// available", naming neither the budget nor the aggregate, and dagnats exited
// into a permanent restart loop. The assertion is about the text the operator
// sees (and, for #688, the fields a recovering caller reads), because that is
// the fix: the failure itself is legitimate.
func TestSetupAll_LoweredBudgetOverExistingStoreIsActionable(t *testing.T) {
	storeDir, err := os.MkdirTemp("", "dagnats-budget-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(storeDir) })

	const bigBudget int64 = 1 << 30   // 1 GiB, the established store
	const smallBudget int64 = 1 << 27 // 128 MiB, the operator's new setting

	// Establish the store under the large budget.
	nc := startServerOnDir(t, storeDir, bigBudget)
	if err := SetupAll(nc, WithStoreBudget(bigBudget)); err != nil {
		t.Fatalf("initial SetupAll under the large budget: %v", err)
	}
	nc.Close()

	// Reopen the same store under the smaller budget.
	nc2 := startServerOnDir(t, storeDir, smallBudget)
	err = SetupAll(nc2, WithStoreBudget(smallBudget))
	if err == nil {
		t.Fatal("SetupAll succeeded reopening under a budget below existing reservations; want an actionable error")
	}

	var budgetErr *StoreBudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("SetupAll error is not a *StoreBudgetExceededError: %v", err)
	}
	if budgetErr.Budget != smallBudget {
		t.Errorf("budgetErr.Budget = %d, want %d", budgetErr.Budget, smallBudget)
	}
	if budgetErr.Reserved <= smallBudget {
		t.Errorf("budgetErr.Reserved = %d, want > smallBudget %d",
			budgetErr.Reserved, smallBudget)
	}
	if budgetErr.Reserved > bigBudget {
		t.Errorf("budgetErr.Reserved = %d, want <= what the store was "+
			"established with (%d)", budgetErr.Reserved, bigBudget)
	}

	msg := err.Error()
	if strings.Contains(msg, "insufficient storage resources") {
		t.Errorf("error still surfaces the opaque JetStream text:\n  %s", msg)
	}
	for _, want := range []string{"max_store_bytes", "reserved"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error missing %q, so the operator cannot act on it:\n  %s", want, msg)
		}
	}
	t.Logf("operator-visible error:\n  %s", msg)
}

// TestSetupAll_LoweredBudgetRecoversAfterRestart proves the mechanism #688's
// auto-shrink depends on: once the embedded server is restarted at a limit
// that covers what StoreBudgetExceededError.Reserved reports, a second
// SetupAll at the ORIGINAL (small) budget succeeds and shrinks every
// stream's MaxBytes down to that budget's fractional ceilings -- CreateOr-
// UpdateStream applies a smaller MaxBytes to an already-existing stream, it
// is not create-only. server.startNATSAndSetupAll drives production through
// this exact sequence; this test drives it directly against natsutil so the
// mechanism is verified independent of server.Config.
func TestSetupAll_LoweredBudgetRecoversAfterRestart(t *testing.T) {
	storeDir, err := os.MkdirTemp("", "dagnats-budget-recover-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(storeDir) })

	const bigBudget int64 = 1 << 30   // 1 GiB, the established store
	const smallBudget int64 = 1 << 27 // 128 MiB, the operator's new setting

	nc := startServerOnDir(t, storeDir, bigBudget)
	if err := SetupAll(nc, WithStoreBudget(bigBudget)); err != nil {
		t.Fatalf("initial SetupAll under the large budget: %v", err)
	}
	nc.Close()

	nc2 := startServerOnDir(t, storeDir, smallBudget)
	err = SetupAll(nc2, WithStoreBudget(smallBudget))
	var budgetErr *StoreBudgetExceededError
	if !errors.As(err, &budgetErr) {
		t.Fatalf("reopen under smallBudget did not fail as expected: %v", err)
	}
	nc2.Close()

	// Restart at an effective limit that covers what is already reserved,
	// exactly what server.startNATSAndSetupAll does on this error. The
	// process's real JetStreamMaxStore is now `effective`, so SetupAll must
	// be told that via WithStoreAdmissionCeiling -- WithStoreBudget alone
	// stays at the operator's configured smallBudget so the ceilings this
	// call sizes still shrink to it.
	effective := budgetErr.Reserved + 64<<20
	nc3 := startServerOnDir(t, storeDir, effective)
	err = SetupAll(nc3,
		WithStoreBudget(smallBudget),
		WithStoreAdmissionCeiling(effective),
	)
	if err != nil {
		t.Fatalf("SetupAll at the original small budget, after restarting "+
			"at the effective limit, must succeed: %v", err)
	}

	js, err := jetstream.New(nc3)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream(WORKFLOW_HISTORY): %v", err)
	}
	want := proportionalMaxBytes(smallBudget, fractionWorkflowHistory)
	got := stream.CachedInfo().Config.MaxBytes
	if got != want {
		t.Fatalf("WORKFLOW_HISTORY MaxBytes = %d after shrink, want %d "+
			"(the small budget's ceiling)", got, want)
	}
	if got >= proportionalMaxBytes(bigBudget, fractionWorkflowHistory) {
		t.Fatalf("WORKFLOW_HISTORY MaxBytes = %d, still at the big-budget "+
			"ceiling; the shrink did not apply", got)
	}

	// A second restart AT the configured small budget (no more headroom
	// needed, since streams already fit) must also succeed.
	nc3.Close()
	nc4 := startServerOnDir(t, storeDir, smallBudget)
	if err := SetupAll(nc4, WithStoreBudget(smallBudget)); err != nil {
		t.Fatalf("restart at the configured budget after the shrink pass "+
			"must not be refused: %v", err)
	}
}

// TestSetupAll_BudgetThatFitsProceeds guards the other side: the preflight
// must refuse only the misconfigured case, never an ordinary startup.
func TestSetupAll_BudgetThatFitsProceeds(t *testing.T) {
	storeDir, err := os.MkdirTemp("", "dagnats-budget-ok-*")
	if err != nil {
		t.Fatalf("temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(storeDir) })

	const budget int64 = 1 << 30
	nc := startServerOnDir(t, storeDir, budget)
	if err := SetupAll(nc, WithStoreBudget(budget)); err != nil {
		t.Fatalf("first SetupAll: %v", err)
	}
	// Re-running at the same budget is the ordinary restart path.
	if err := SetupAll(nc, WithStoreBudget(budget)); err != nil {
		t.Fatalf("restart at an unchanged budget must not be refused: %v", err)
	}
}
