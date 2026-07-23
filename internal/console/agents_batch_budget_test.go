// agents_batch_budget_test.go proves the /console/agents LIST path computes
// budgets in ONE batched call (BudgetsForRoots) instead of a per-root Budget
// scan — the fix for the ~20s agents-page load with many runtimes.
//
// Methodology:
//   - A countingBudgetSource stub stands in for *api.Service. It records how
//     many times BudgetsForRoots vs the per-root Budget are called, so the
//     test can assert the list path is batched (BudgetsForRoots == 1,
//     Budget == 0) rather than N+1.
//   - The stub returns real in-memory runs + a budget map so the produced
//     rows carry correct Budget / BudgetOK. Positive space: rows render with
//     the seeded budget. Negative space: when the batched call errors, EVERY
//     row degrades to BudgetOK=false (never a fabricated 0/0).
//   - No NATS: the list logic under test is pure over the stub. Bounded by
//     the fixed seed size.
package console

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/worker"
)

// testLogger returns a slog logger writing to the test log — used by the
// list-path tests that build an apiServiceAdapter directly (no full Config).
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(testLogWriter(t), nil))
}

// countingBudgetSource is an agentBudgetSource stub that counts calls and
// serves canned runs + budgets. budgetsErr, when set, makes BudgetsForRoots
// fail so the degrade path is exercised.
type countingBudgetSource struct {
	runs         []dag.WorkflowRun
	budgets      map[string]worker.RuntimeBudget
	budgetsErr   error
	budgetsCalls int
	perRootCalls int
}

func (c *countingBudgetSource) ScanRuns(
	_ context.Context, _ api.RunsFilter, _ int,
) ([]dag.WorkflowRun, error) {
	return c.runs, nil
}

func (c *countingBudgetSource) Budget(
	_ context.Context, _ string,
) (worker.RuntimeBudget, string, error) {
	c.perRootCalls++
	return worker.RuntimeBudget{}, "", nil
}

func (c *countingBudgetSource) BudgetsForRoots(
	_ context.Context, _ []dag.WorkflowRun,
) (map[string]worker.RuntimeBudget, error) {
	c.budgetsCalls++
	if c.budgetsErr != nil {
		return nil, c.budgetsErr
	}
	return c.budgets, nil
}

// twoRootRuns builds two independent 2-node runtime trees (root + child).
func twoRootRuns() []dag.WorkflowRun {
	return []dag.WorkflowRun{
		agentRun("root-a", "agent.plan", "root-a", "", dag.RunStatusRunning),
		agentRun("kid-a", "agent.impl", "root-a", "root-a", dag.RunStatusRunning),
		agentRun("root-b", "agent.plan", "root-b", "", dag.RunStatusRunning),
		agentRun("kid-b", "agent.impl", "root-b", "root-b", dag.RunStatusRunning),
	}
}

// TestListAgentRuntimes_UsesBatchedBudget proves the list path calls
// BudgetsForRoots exactly once and the per-root Budget ZERO times, and that
// each rendered row carries the batched budget with BudgetOK=true.
func TestListAgentRuntimes_UsesBatchedBudget(t *testing.T) {
	src := &countingBudgetSource{
		runs: twoRootRuns(),
		budgets: map[string]worker.RuntimeBudget{
			"root-a": {ActiveRuns: 2, MaxActiveRuns: 50,
				RegisteredDefs: 1, MaxRegisteredDefs: 50},
			"root-b": {ActiveRuns: 2, MaxActiveRuns: 50,
				RegisteredDefs: 0, MaxRegisteredDefs: 50},
		},
	}
	a := &apiServiceAdapter{logger: testLogger(t)}

	rows, err := a.listAgentRuntimes(context.Background(), src, nil, 200)
	if err != nil {
		t.Fatalf("listAgentRuntimes: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if src.budgetsCalls != 1 {
		t.Errorf("BudgetsForRoots calls = %d, want 1", src.budgetsCalls)
	}
	if src.perRootCalls != 0 {
		t.Errorf("per-root Budget calls = %d, want 0 on the list path",
			src.perRootCalls)
	}
	for i := range rows {
		if !rows[i].BudgetOK {
			t.Errorf("row %q BudgetOK = false, want true",
				rows[i].RootRunID)
		}
		want := src.budgets[rows[i].RootRunID]
		if rows[i].Budget != want {
			t.Errorf("row %q Budget = %+v, want %+v",
				rows[i].RootRunID, rows[i].Budget, want)
		}
	}
}

// TestListAgentRuntimes_BudgetErrorDegradesAllRows proves a batched-budget
// failure degrades EVERY row to BudgetOK=false (never a fabricated 0/0) and
// still returns the rows (the page renders without the budget block), never
// falling back to per-root Budget calls.
func TestListAgentRuntimes_BudgetErrorDegradesAllRows(t *testing.T) {
	src := &countingBudgetSource{
		runs:       twoRootRuns(),
		budgetsErr: errors.New("defKV down"),
	}
	a := &apiServiceAdapter{logger: testLogger(t)}

	rows, err := a.listAgentRuntimes(context.Background(), src, nil, 200)
	if err != nil {
		t.Fatalf("listAgentRuntimes should degrade, not error: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if src.perRootCalls != 0 {
		t.Errorf("per-root Budget calls = %d, want 0 even on degrade",
			src.perRootCalls)
	}
	for i := range rows {
		if rows[i].BudgetOK {
			t.Errorf("row %q BudgetOK = true, want false on degrade",
				rows[i].RootRunID)
		}
		if rows[i].Budget != (worker.RuntimeBudget{}) {
			t.Errorf("row %q Budget = %+v, want zero on degrade",
				rows[i].RootRunID, rows[i].Budget)
		}
	}
}
