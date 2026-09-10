package natsutil

import (
	"os"
	"strings"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
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
// failing case for #685, in the shape production hits it.
//
// A store is established under a large budget, then reopened under a small
// one, exactly as `systemctl edit dagnats` + restart does. Every stream
// assertion is then refused: JetStream admits a stream only if its MaxBytes
// fits the budget remaining after every OTHER stream's, and the streams
// already on disk reserve more than the whole new budget. That includes the
// updates that would have shrunk the ceilings, so dagnats cannot lower its
// own budget.
//
// Before the fix this surfaced as a bare "insufficient storage resources
// available", naming neither the budget nor the aggregate, and dagnats exited
// into a permanent restart loop. The assertion is about the text the operator
// sees, because the text is the fix: the failure itself is legitimate.
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
