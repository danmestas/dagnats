// api/workflow_delete_nonterminal_test.go
// Methodology: exercise Service.DeleteWorkflow's non-terminal-run guard
// (#682) against a real embedded NATS server. Runs are seeded directly
// via engine.SnapshotStore (no orchestrator running) so the guard's
// behavior is isolated from run-lifecycle machinery. Unless force is
// passed, delete must refuse while any run for the workflow is
// non-terminal, naming the offending run IDs; force must bypass the
// refusal without touching the runs themselves.
package api

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func newNonTerminalGuardTestService(
	t *testing.T,
) (*Service, *engine.SnapshotStore) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := engine.NewSnapshotStore(js)

	def := dag.WorkflowDef{
		Name:  "wf-guard",
		Steps: []dag.StepDef{{ID: "a", Task: "task-a"}},
	}
	if err := svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	return svc, store
}

// seedRun persists a run snapshot via SaveInitial rather than Save so
// a non-terminal run also gets the runactive.<runID> liveness marker
// ListActive (and therefore nonTerminalRunIDsForWorkflow) reads --
// Save alone only writes the snapshot, not the active-run index.
func seedRun(
	t *testing.T, store *engine.SnapshotStore,
	runID, workflowID string, status dag.RunStatus,
) {
	t.Helper()
	run := dag.WorkflowRun{
		RunID:      runID,
		WorkflowID: workflowID,
		Status:     status,
		CreatedAt:  time.Now(),
	}
	if err := store.SaveInitial(context.Background(), run); err != nil {
		t.Fatalf("store.SaveInitial(%s): %v", runID, err)
	}
}

func TestDeleteWorkflow_RefusesWithNonTerminalRun(t *testing.T) {
	svc, store := newNonTerminalGuardTestService(t)
	ctx := context.Background()
	seedRun(t, store, "run-active-1", "wf-guard", dag.RunStatusRunning)

	err := svc.DeleteWorkflow(ctx, "wf-guard", false)
	// Positive: delete is refused while a run is non-terminal.
	if err == nil {
		t.Fatal("delete should be refused with a non-terminal run")
	}
	var refusal *ErrWorkflowHasNonTerminalRuns
	if !errors.As(err, &refusal) {
		t.Fatalf("error should be *ErrWorkflowHasNonTerminalRuns, got: %v (%T)", err, err)
	}
	// Negative: the error names the offending run.
	found := false
	for _, id := range refusal.RunIDs {
		if id == "run-active-1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("error should name run-active-1, got RunIDs=%v", refusal.RunIDs)
	}
	if refusal.Total != 1 {
		t.Fatalf("refusal.Total = %d, want 1", refusal.Total)
	}
	// Negative: the workflow must NOT have been deleted.
	if _, gerr := svc.GetWorkflow("wf-guard"); gerr != nil {
		t.Fatal("workflow must survive a refused delete")
	}
}

func TestDeleteWorkflow_ForceOverridesNonTerminalRun(t *testing.T) {
	svc, store := newNonTerminalGuardTestService(t)
	ctx := context.Background()
	seedRun(t, store, "run-active-1", "wf-guard", dag.RunStatusRunning)

	// Positive: force deletes despite the non-terminal run.
	if err := svc.DeleteWorkflow(ctx, "wf-guard", true); err != nil {
		t.Fatalf("force delete: %v", err)
	}
	if _, gerr := svc.GetWorkflow("wf-guard"); gerr == nil {
		t.Fatal("workflow should be deleted with force")
	}
	// Negative: force does not touch the run itself.
	got, err := store.Load(ctx, "run-active-1")
	if err != nil {
		t.Fatalf("run should survive force delete: %v", err)
	}
	if got.Status != dag.RunStatusRunning {
		t.Fatalf("run status changed by force delete: %v", got.Status)
	}
}

func TestDeleteWorkflow_TerminalRunsOnlySucceedsWithoutForce(t *testing.T) {
	svc, store := newNonTerminalGuardTestService(t)
	ctx := context.Background()
	seedRun(t, store, "run-done-1", "wf-guard", dag.RunStatusCompleted)
	seedRun(t, store, "run-done-2", "wf-guard", dag.RunStatusFailed)

	// Positive: only-terminal runs never trip the guard.
	if err := svc.DeleteWorkflow(ctx, "wf-guard", false); err != nil {
		t.Fatalf("delete with only terminal runs should succeed: %v", err)
	}
	// Negative: the terminal run history still reads back fine.
	got, err := store.Load(ctx, "run-done-1")
	if err != nil {
		t.Fatalf("terminal run history destroyed by delete: %v", err)
	}
	if got.WorkflowID != "wf-guard" {
		t.Fatalf("run snapshot corrupted: %#v", got)
	}
}

func TestDeleteWorkflow_NonTerminalRunListIsCappedWithHonestTotal(t *testing.T) {
	svc, store := newNonTerminalGuardTestService(t)
	ctx := context.Background()
	const seeded = nonTerminalRunListCap + 5
	for i := 0; i < seeded; i++ {
		seedRun(t, store,
			fmt.Sprintf("run-active-%d", i), "wf-guard",
			dag.RunStatusRunning,
		)
	}

	err := svc.DeleteWorkflow(ctx, "wf-guard", false)
	var refusal *ErrWorkflowHasNonTerminalRuns
	if !errors.As(err, &refusal) {
		t.Fatalf("error should be *ErrWorkflowHasNonTerminalRuns, got: %v", err)
	}
	// Positive: the listed IDs are capped.
	if len(refusal.RunIDs) != nonTerminalRunListCap {
		t.Fatalf("len(RunIDs) = %d, want %d", len(refusal.RunIDs), nonTerminalRunListCap)
	}
	// Negative: the true total is not silently truncated to the cap.
	if refusal.Total != seeded {
		t.Fatalf("refusal.Total = %d, want %d", refusal.Total, seeded)
	}
}
