// api/controlplane_budgets_batch_test.go
// Integration test for BudgetsForRoots — the batched budget read that backs
// the /console/agents list page (fix/console-agents-nplus1). The per-root
// Budget did THREE store ops per root (a full run scan among them), making
// the agents page O(roots x runs). BudgetsForRoots computes every root's
// budget from the runs the caller already holds plus ONE def-key scan, so the
// whole page costs O(runs + defkeys).
//
// Methodology: fresh embedded NATS + orchestrator + api per test; seed two
// roots (one busy with children + defs, one idle) directly so the counts are
// deterministic; assert the batched result AGREES with the per-root Budget for
// at least one root (the parity anchor) plus explicit counts. Bounded 10s
// waits inherited from the harness helpers.
package api

import (
	"context"
	"testing"

	"github.com/danmestas/dagnats/dag"
)

// TestBudgetsForRoots_ParityWithPerRootBudget proves the batched read reports
// the SAME ActiveRuns/RegisteredDefs/maxima as the per-root Budget for a busy
// root, that a lone idle root is counted independently, and that a root with
// no runs in the input is simply absent (negative space).
func TestBudgetsForRoots_ParityWithPerRootBudget(t *testing.T) {
	limits := RuntimeLimits{MaxActiveRunsPerRoot: 50, MaxDefsPerRoot: 50}
	h := newCPHarnessWithLimits(t, true, limits)

	// Root "busy": 1 root run + 2 active children => 3 active; plus 2 defs.
	seedActiveRunUnderRoot(t, h.svc, "busy", "busy")
	seedActiveRunUnderRoot(t, h.svc, "busy-kid-1", "busy")
	seedActiveRunUnderRoot(t, h.svc, "busy-kid-2", "busy")
	for _, name := range []string{"tool-a", "tool-b"} {
		def := childDef()
		def.Name = name
		if _, _, err := h.svc.RegisterRuntimeWorkflow(
			context.Background(), def, "busy", false,
		); err != nil {
			t.Fatalf("register %q under busy: %v", name, err)
		}
	}
	// Root "idle": just its own root run, no defs.
	seedRootRun(t, h.svc, "idle")

	runs, err := h.svc.ScanRuns(context.Background(), RunsFilter{}, 10_000)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}

	budgets, err := h.svc.BudgetsForRoots(context.Background(), runs)
	if err != nil {
		t.Fatalf("BudgetsForRoots: %v", err)
	}

	busy, ok := budgets["busy"]
	if !ok {
		t.Fatalf("busy root absent from batched budgets")
	}
	if busy.ActiveRuns != 3 {
		t.Errorf("busy ActiveRuns = %d, want 3", busy.ActiveRuns)
	}
	if busy.RegisteredDefs != 2 {
		t.Errorf("busy RegisteredDefs = %d, want 2", busy.RegisteredDefs)
	}
	if busy.MaxActiveRuns != 50 || busy.MaxRegisteredDefs != 50 {
		t.Errorf("busy maxima = (%d, %d), want (50, 50)",
			busy.MaxActiveRuns, busy.MaxRegisteredDefs)
	}

	// Parity anchor: the batched budget must equal the per-root Budget.
	want, _, err := h.svc.Budget(context.Background(), "busy")
	if err != nil {
		t.Fatalf("per-root Budget(busy): %v", err)
	}
	if busy != want {
		t.Errorf("batched busy budget = %+v, per-root Budget = %+v", busy, want)
	}

	idle, ok := budgets["idle"]
	if !ok {
		t.Fatalf("idle root absent from batched budgets")
	}
	if idle.ActiveRuns != 1 { // the root run itself
		t.Errorf("idle ActiveRuns = %d, want 1", idle.ActiveRuns)
	}
	if idle.RegisteredDefs != 0 {
		t.Errorf("idle RegisteredDefs = %d, want 0", idle.RegisteredDefs)
	}

	// Negative space: a root not present in the input runs is absent.
	if _, present := budgets["ghost"]; present {
		t.Errorf("ghost root should be absent from batched budgets")
	}
}

// TestBudgetsForRoots_TerminalRunsExcluded proves a terminal child drops out
// of the active count (the same IsTerminal filter the per-root scan uses),
// and that an empty input yields an empty (non-nil) map without a scan error.
func TestBudgetsForRoots_TerminalRunsExcluded(t *testing.T) {
	h := newCPHarnessWithLimits(t, true,
		RuntimeLimits{MaxActiveRunsPerRoot: 50, MaxDefsPerRoot: 50})

	seedActiveRunUnderRoot(t, h.svc, "r", "r")
	seedActiveRunUnderRoot(t, h.svc, "r-kid", "r")
	markRunTerminal(t, h.svc, "r-kid")

	runs, err := h.svc.ScanRuns(context.Background(), RunsFilter{}, 10_000)
	if err != nil {
		t.Fatalf("list runs: %v", err)
	}
	budgets, err := h.svc.BudgetsForRoots(context.Background(), runs)
	if err != nil {
		t.Fatalf("BudgetsForRoots: %v", err)
	}
	if budgets["r"].ActiveRuns != 1 {
		t.Errorf("r ActiveRuns = %d, want 1 (terminal kid excluded)",
			budgets["r"].ActiveRuns)
	}

	empty, err := h.svc.BudgetsForRoots(context.Background(), []dag.WorkflowRun{})
	if err != nil {
		t.Fatalf("BudgetsForRoots(empty): %v", err)
	}
	if empty == nil {
		t.Errorf("empty input must yield a non-nil map")
	}
	if len(empty) != 0 {
		t.Errorf("empty input map len = %d, want 0", len(empty))
	}
}
