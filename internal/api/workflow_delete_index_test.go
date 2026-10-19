// api/workflow_delete_index_test.go
// Methodology: exercise DeleteWorkflow's non-terminal-run guard when the
// two run indexes disagree (#784 item 14), against a real embedded NATS
// server. A run is seeded through SnapshotStore.SaveInitial (snapshot,
// runactive marker, runidx entry), then its runactive.<runID> marker is
// deleted straight from the workflow_runs bucket -- the state the
// pre-#768 lost-update bug left behind. The guard must still find the run
// through the run index, and a run-index scan that hits its cap must
// refuse rather than report a clean workflow.
package api

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
)

// deleteActiveMarker removes runID's runactive marker, leaving the
// snapshot and the run index entry in place.
func deleteActiveMarker(t *testing.T, svc *Service, runID string) {
	t.Helper()
	ctx := context.Background()
	kv, err := svc.js.KeyValue(ctx, "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue workflow_runs: %v", err)
	}
	if err := kv.Delete(ctx, "runactive."+runID); err != nil {
		t.Fatalf("delete runactive marker: %v", err)
	}
}

func TestDeleteWorkflow_RefusesRunMissingActiveMarker(t *testing.T) {
	svc, store := newNonTerminalGuardTestService(t)
	ctx := context.Background()
	seedRun(t, store, "run-marked", "wf-guard", dag.RunStatusRunning)
	seedRun(t, store, "run-unmarked-1", "wf-guard", dag.RunStatusRunning)
	seedRun(t, store, "run-unmarked-2", "wf-guard", dag.RunStatusPending)
	deleteActiveMarker(t, svc, "run-unmarked-1")
	deleteActiveMarker(t, svc, "run-unmarked-2")

	err := svc.DeleteWorkflow(ctx, "wf-guard", false)
	var refusal *ErrWorkflowHasNonTerminalRuns
	if !errors.As(err, &refusal) {
		t.Fatalf("want *ErrWorkflowHasNonTerminalRuns, got: %v", err)
	}
	// Positive: every non-terminal run is counted once, marker or not.
	if refusal.Total != 3 {
		t.Fatalf("Total = %d, want 3 (RunIDs=%v)", refusal.Total, refusal.RunIDs)
	}
	listed := strings.Join(refusal.RunIDs, ",")
	for _, id := range []string{"run-marked", "run-unmarked-1", "run-unmarked-2"} {
		if !strings.Contains(listed, id) {
			t.Errorf("refusal should name %s, got %v", id, refusal.RunIDs)
		}
	}
	// Negative: the definition survives the refusal.
	if _, gerr := svc.GetWorkflow("wf-guard"); gerr != nil {
		t.Fatal("workflow must survive a refused delete")
	}
}

func TestDeleteWorkflow_UnmarkedRunAloneStillRefuses(t *testing.T) {
	svc, store := newNonTerminalGuardTestService(t)
	ctx := context.Background()
	seedRun(t, store, "run-unmarked", "wf-guard", dag.RunStatusRunning)
	seedRun(t, store, "run-done", "wf-guard", dag.RunStatusCompleted)
	deleteActiveMarker(t, svc, "run-unmarked")

	err := svc.DeleteWorkflow(ctx, "wf-guard", false)
	var refusal *ErrWorkflowHasNonTerminalRuns
	if !errors.As(err, &refusal) {
		t.Fatalf("want *ErrWorkflowHasNonTerminalRuns, got: %v", err)
	}
	if refusal.Total != 1 || refusal.RunIDs[0] != "run-unmarked" {
		t.Fatalf("refusal = %+v, want only run-unmarked", refusal)
	}
	if refusal.AtLeast {
		t.Fatal("a complete scan must report an exact count")
	}
	// Force still deletes.
	if err := svc.DeleteWorkflow(ctx, "wf-guard", true); err != nil {
		t.Fatalf("force delete: %v", err)
	}
	if _, gerr := svc.GetWorkflow("wf-guard"); gerr == nil {
		t.Fatal("workflow should be gone after force delete")
	}
}

// TestDeleteWorkflow_IndexScanCapRefuses shrinks the run-index scan cap
// below the number of stored runs: the guard cannot see every run, so it
// must refuse (reporting a lower bound) instead of deleting.
func TestDeleteWorkflow_IndexScanCapRefuses(t *testing.T) {
	svc, store := newNonTerminalGuardTestService(t)
	ctx := context.Background()
	prev := deleteGuardIndexFetchMax
	deleteGuardIndexFetchMax = 3
	t.Cleanup(func() { deleteGuardIndexFetchMax = prev })
	for i := 0; i < 6; i++ {
		seedRun(t, store, fmt.Sprintf("run-done-%d", i), "wf-guard",
			dag.RunStatusCompleted)
	}

	// Nothing non-terminal in the newest 3 runs, but 3 more were never
	// scanned: the guard cannot call the workflow clean.
	err := svc.DeleteWorkflow(ctx, "wf-guard", false)
	var refusal *ErrWorkflowHasNonTerminalRuns
	if !errors.As(err, &refusal) {
		t.Fatalf("want *ErrWorkflowHasNonTerminalRuns, got: %v", err)
	}
	if !refusal.AtLeast || refusal.Total != 0 {
		t.Fatalf("refusal = %+v, want AtLeast with Total 0", refusal)
	}
	if !strings.Contains(refusal.Error(), "could not confirm") {
		t.Fatalf("message should say the scan was incomplete: %q", refusal.Error())
	}

	// A marker-less live run found inside the capped window: the count is
	// reported as a lower bound.
	seedRun(t, store, "run-live", "wf-guard", dag.RunStatusRunning)
	deleteActiveMarker(t, svc, "run-live")
	err = svc.DeleteWorkflow(ctx, "wf-guard", false)
	if !errors.As(err, &refusal) {
		t.Fatalf("want *ErrWorkflowHasNonTerminalRuns, got: %v", err)
	}
	if !refusal.AtLeast || refusal.Total != 1 {
		t.Fatalf("refusal = %+v, want AtLeast with Total 1", refusal)
	}
	if !strings.Contains(refusal.Error(), "at least 1 non-terminal") {
		t.Fatalf("message should say the count is a lower bound: %q", refusal.Error())
	}

	// Negative: once the live run is gone, a complete scan deletes cleanly.
	deleteGuardIndexFetchMax = engine.ScanFetchMax
	if err := store.Delete(ctx, "run-live"); err != nil {
		t.Fatalf("store.Delete(run-live): %v", err)
	}
	if err := svc.DeleteWorkflow(ctx, "wf-guard", false); err != nil {
		t.Fatalf("delete with a full scan: %v", err)
	}
}
