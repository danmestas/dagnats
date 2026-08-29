// api/run_scan_order_test.go
// End-to-end reproduction of #659: on a store with a large run
// population, GET /runs (ScanRuns) and POST /runs/cancel (bulk cancel)
// must find a run started AFTER that population exists, regardless of
// which label/status/workflow filter is used to find it. Before the
// creation-ordered index, the underlying key scan sampled an arbitrary
// (not newest-first) window, so a filtered query silently missed the
// exact runs labels exist to find.
// Methodology: real embedded NATS server + orchestrator. The bulk
// population is seeded by writing snapshots directly via the store
// (svc.store.Save) -- going through the full publish/dispatch pipeline
// for 1700+ runs would make this test far too slow for what it's
// proving. The single new run under test IS started through the real
// StartRunWithLabels -> orchestrator -> snapshot path.
package api

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
)

const scanOrderSeedPopulation = 1700

// seedOldRuns writes `total` Completed runs directly via the store,
// oldest first, with CreatedAt strictly in the past (so a genuinely
// new run started afterward is always newer than all of them). Then
// re-Saves the oldest seedTouchCount of them -- proving a later
// revision write does not move a run's position in the creation-order
// index, the exact bias #659 fixed.
func seedOldRuns(
	t *testing.T, svc *Service, workflowID string, total int,
) {
	t.Helper()
	if total <= 0 {
		panic("seedOldRuns: total must be positive")
	}
	base := time.Now().Add(-24 * time.Hour)
	ctx := context.Background()
	for i := 0; i < total; i++ {
		run := dag.WorkflowRun{
			RunID:      fmt.Sprintf("seed-%05d", i),
			WorkflowID: workflowID,
			Status:     dag.RunStatusCompleted,
			Steps:      map[string]dag.StepState{},
			CreatedAt:  base.Add(time.Duration(i) * time.Millisecond),
		}
		if err := svc.store.Save(ctx, run); err != nil {
			t.Fatalf("seed run %d: %v", i, err)
		}
	}
	const seedTouchCount = 50
	for i := 0; i < seedTouchCount; i++ {
		run, err := svc.store.Load(ctx, fmt.Sprintf("seed-%05d", i))
		if err != nil {
			t.Fatalf("load seed run %d for touch: %v", i, err)
		}
		if err := svc.store.Save(ctx, run); err != nil {
			t.Fatalf("touch (re-save) seed run %d: %v", i, err)
		}
	}
}

// newScanOrderFixture spins an embedded server + orchestrator + service
// and registers a workflow with a task type NO worker handles, so a
// started run stays non-terminal (Pending/Running) long enough for the
// filter/cancel assertions.
func newScanOrderFixture(t *testing.T, workflowName string) *Service {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)

	svc := NewService(nc)
	wb := dag.NewWorkflow(workflowName)
	wb.Task("unstaffed", "no-such-worker-task")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	return svc
}

// waitRunNonTerminal polls GetRun until the run exists and has left
// its zero-value / not-found state, then returns it. Bounded by a 10s
// deadline (Running/Pending is reached fast -- no worker is needed to
// get this far, only to advance further).
func waitRunNonTerminal(t *testing.T, svc *Service, runID string) dag.WorkflowRun {
	t.Helper()
	deadline := time.After(10 * time.Second)
	for {
		run, err := svc.GetRun(context.Background(), runID)
		if err == nil && run.RunID == runID {
			return run
		}
		select {
		case <-deadline:
			t.Fatalf("run %s not visible within 10s (last err %v)", runID, err)
		case <-time.After(20 * time.Millisecond):
		}
	}
}

// TestScanRunsFindsNewLabeledRunAmongManyOldRuns reproduces #659's
// exact symptom: a run started with labels, on a store with 1700+
// older runs, must be findable via GET /runs?label=... at the DEFAULT
// limit -- not just with limit=10000.
func TestScanRunsFindsNewLabeledRunAmongManyOldRuns(t *testing.T) {
	const wf = "scan-order-label-wf"
	svc := newScanOrderFixture(t, wf)
	seedOldRuns(t, svc, wf, scanOrderSeedPopulation)

	labels := map[string]string{"repo": "demo", "branch": "trunk"}
	runID, err := svc.StartRunWithLabels(
		context.Background(), wf, nil, labels,
	)
	if err != nil {
		t.Fatalf("StartRunWithLabels: %v", err)
	}
	waitRunNonTerminal(t, svc, runID)

	// Positive: default limit (0 -> DefaultRunsLimit) still finds it.
	runs, err := svc.ScanRuns(
		context.Background(), RunsFilter{Labels: labels}, 0,
	)
	if err != nil {
		t.Fatalf("ScanRuns: %v", err)
	}
	if len(runs) != 1 || runs[0].RunID != runID {
		t.Fatalf("ScanRuns(label filter) = %+v, want exactly [%s]",
			runIDsOf(runs), runID)
	}
	// Negative: a non-matching label finds nothing (not a false
	// positive from the seeded population).
	none, err := svc.ScanRuns(
		context.Background(), RunsFilter{Labels: map[string]string{
			"repo": "demo", "branch": "nope",
		}}, 0,
	)
	if err != nil {
		t.Fatalf("ScanRuns (non-matching): %v", err)
	}
	if len(none) != 0 {
		t.Fatalf("ScanRuns(non-matching label) = %d runs, want 0", len(none))
	}
}

// TestScanRunsFindsNewestByStatusAndWorkflow proves the same fix for
// the status and workflow filters (not just labels): both find the
// newest matching run at the front of the result.
func TestScanRunsFindsNewestByStatusAndWorkflow(t *testing.T) {
	const wf = "scan-order-status-wf"
	svc := newScanOrderFixture(t, wf)
	seedOldRuns(t, svc, wf, scanOrderSeedPopulation)

	runID, err := svc.StartRun(context.Background(), wf, nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	run := waitRunNonTerminal(t, svc, runID)

	// ?workflow= : the newest match is first in a newest-first scan.
	byWorkflow, err := svc.ScanRuns(
		context.Background(), RunsFilter{Workflow: wf}, 0,
	)
	if err != nil {
		t.Fatalf("ScanRuns(workflow): %v", err)
	}
	if len(byWorkflow) == 0 || byWorkflow[0].RunID != runID {
		t.Fatalf("ScanRuns(workflow) first result = %q, want %q",
			firstRunID(byWorkflow), runID)
	}

	// ?status= : only the new run carries a non-terminal status (every
	// seeded run is Completed), so this also proves it's found at all.
	byStatus, err := svc.ScanRuns(
		context.Background(), RunsFilter{State: &run.Status}, 0,
	)
	if err != nil {
		t.Fatalf("ScanRuns(status): %v", err)
	}
	found := false
	for _, r := range byStatus {
		if r.RunID == runID {
			found = true
		}
	}
	if !found {
		t.Fatalf("ScanRuns(status=%v) = %v, missing %q",
			run.Status, runIDsOf(byStatus), runID)
	}
}

// TestBulkCancelFindsNewLabeledRunAmongManyOldRuns reproduces #659's
// bulk-cancel symptom: POST /runs/cancel with labels must cancel the
// one matching run on a large store, not silently no-op.
func TestBulkCancelFindsNewLabeledRunAmongManyOldRuns(t *testing.T) {
	const wf = "scan-order-cancel-wf"
	svc := newScanOrderFixture(t, wf)
	seedOldRuns(t, svc, wf, scanOrderSeedPopulation)

	labels := map[string]string{"batch": "release-42"}
	runID, err := svc.StartRunWithLabels(
		context.Background(), wf, nil, labels,
	)
	if err != nil {
		t.Fatalf("StartRunWithLabels: %v", err)
	}
	waitRunNonTerminal(t, svc, runID)

	resp, err := svc.BulkCancelRuns(context.Background(), BulkCancelRequest{
		WorkflowID: wf,
		Labels:     labels,
	})
	if err != nil {
		t.Fatalf("BulkCancelRuns: %v", err)
	}
	// Positive: exactly the one labeled run was cancelled.
	if len(resp.Cancelled) != 1 || resp.Cancelled[0] != runID {
		t.Fatalf("Cancelled = %v, want exactly [%s]", resp.Cancelled, runID)
	}
	got := waitRunStatus(t, svc, runID, dag.RunStatusCancelled)
	if got.RunID != runID {
		t.Fatalf("run id = %q, want %q", got.RunID, runID)
	}
	// Negative: none of the 1700+ seeded (already-Completed, unlabeled)
	// runs were touched.
	if len(resp.Skipped) != 0 {
		t.Fatalf("Skipped = %v, want empty (seeded runs don't match)",
			resp.Skipped)
	}
}

func runIDsOf(runs []dag.WorkflowRun) []string {
	if len(runs) > scanOrderSeedPopulation+10 {
		panic("runIDsOf: runs exceeds bound")
	}
	out := make([]string, 0, len(runs))
	for _, r := range runs {
		out = append(out, r.RunID)
	}
	return out
}

func firstRunID(runs []dag.WorkflowRun) string {
	if len(runs) == 0 {
		return "<empty>"
	}
	return runs[0].RunID
}
