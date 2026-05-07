// reconciler_test.go
// Tests for the reconciliation janitor that recovers wedged
// runs (RunStatusRunning with no in-flight work and no path
// to terminal state). Methodology: real embedded NATS, real
// KV; bypass orchestrator.Start to avoid the history consumer
// and call reconcileRunningRuns directly.
package engine

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
)

func TestReconciler_CompletesRunWhenStepsAllDone(t *testing.T) {
	// The production case from #185: a run is left at
	// RunStatusRunning even though every step is Completed.
	// IsComplete returns true; the janitor must promote the
	// run to RunStatusCompleted on its next sweep.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "reconciler-complete", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "a", Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	seedWorkflowDef(t, nc, wfDef)

	orch := NewOrchestrator(nc)

	wedged := dag.WorkflowRun{
		RunID:      "wedged-complete",
		WorkflowID: wfDef.Name,
		Status:     dag.RunStatusRunning,
		CreatedAt: time.Now().UTC().
			Add(-(reconcileMinAge + time.Minute)),
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted},
		},
	}
	ctx := context.Background()
	if err := orch.store.Save(ctx, wedged); err != nil {
		t.Fatalf("seed wedged run: %v", err)
	}

	orch.reconcileRunningRuns(ctx)

	after, err := orch.store.Load(ctx, wedged.RunID)
	if err != nil {
		t.Fatalf("load post-reconcile: %v", err)
	}
	if after.Status != dag.RunStatusCompleted {
		t.Errorf(
			"Status = %v, want Completed", after.Status,
		)
	}
}

func TestReconciler_FailsRunWhenWedgedNoWork(t *testing.T) {
	// Defensive case: a run is RunStatusRunning, no step is
	// in flight (Pending/Queued/Running), and IsComplete is
	// false because some step never finished. There is no
	// path forward; the janitor must mark it Failed so
	// operators see the wedge instead of letting the entry
	// linger in workflow_runs forever.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "reconciler-wedge", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "a", Task: "task-a",
				Type: dag.StepTypeNormal,
			},
			{
				ID: "b", Task: "task-b",
				DependsOn: []string{"a"},
				Type:      dag.StepTypeNormal,
			},
		},
	}
	seedWorkflowDef(t, nc, wfDef)

	orch := NewOrchestrator(nc)

	// `a` failed earlier; `b` was never dispatched. No step
	// is in flight; no further events will arrive.
	wedged := dag.WorkflowRun{
		RunID:      "wedged-no-path",
		WorkflowID: wfDef.Name,
		Status:     dag.RunStatusRunning,
		CreatedAt: time.Now().UTC().
			Add(-(reconcileMinAge + time.Minute)),
		Steps: map[string]dag.StepState{
			"a": {
				Status: dag.StepStatusFailed,
				Error:  "earlier failure",
			},
			"b": {Status: dag.StepStatusPending},
		},
	}
	// hasInFlightStep counts Pending as in-flight — replace b
	// with a non-in-flight terminal-ish state to actually
	// trigger the wedged-no-work branch. Use Cancelled so the
	// run is unambiguously stuck (not waiting for dispatch).
	wedged.Steps["b"] = dag.StepState{
		Status: dag.StepStatusCancelled,
	}
	ctx := context.Background()
	if err := orch.store.Save(ctx, wedged); err != nil {
		t.Fatalf("seed wedged run: %v", err)
	}

	orch.reconcileRunningRuns(ctx)

	after, err := orch.store.Load(ctx, wedged.RunID)
	if err != nil {
		t.Fatalf("load post-reconcile: %v", err)
	}
	if after.Status != dag.RunStatusFailed {
		t.Errorf(
			"Status = %v, want Failed", after.Status,
		)
	}
}

func TestReconciler_SkipsRecentlyCreatedRun(t *testing.T) {
	// Safety guard: a run created moments ago may still be
	// mid-dispatch with no Steps yet populated. The janitor
	// must not race with dispatch; runs younger than the
	// minimum age are left alone.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name: "reconciler-recent", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "a", Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	seedWorkflowDef(t, nc, wfDef)

	orch := NewOrchestrator(nc)

	young := dag.WorkflowRun{
		RunID:      "too-young",
		WorkflowID: wfDef.Name,
		Status:     dag.RunStatusRunning,
		CreatedAt:  time.Now().UTC(),
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted},
		},
	}
	ctx := context.Background()
	if err := orch.store.Save(ctx, young); err != nil {
		t.Fatalf("seed young run: %v", err)
	}

	orch.reconcileRunningRuns(ctx)

	after, err := orch.store.Load(ctx, young.RunID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if after.Status != dag.RunStatusRunning {
		t.Errorf(
			"young run was modified; Status = %v, "+
				"want Running",
			after.Status,
		)
	}
}

func TestReconciler_SkipsRunWithInFlightStep(t *testing.T) {
	// A run with any step in Pending/Queued/Running is
	// genuinely active: a worker may complete the step at
	// any moment. The janitor must not touch it.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name: "reconciler-active", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "a", Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	seedWorkflowDef(t, nc, wfDef)

	orch := NewOrchestrator(nc)

	active := dag.WorkflowRun{
		RunID:      "actively-running",
		WorkflowID: wfDef.Name,
		Status:     dag.RunStatusRunning,
		CreatedAt: time.Now().UTC().
			Add(-(reconcileMinAge + time.Minute)),
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusQueued},
		},
	}
	ctx := context.Background()
	if err := orch.store.Save(ctx, active); err != nil {
		t.Fatalf("seed active run: %v", err)
	}

	orch.reconcileRunningRuns(ctx)

	after, err := orch.store.Load(ctx, active.RunID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if after.Status != dag.RunStatusRunning {
		t.Errorf(
			"active run was modified; Status = %v, "+
				"want Running",
			after.Status,
		)
	}
}

func TestReconciler_LeavesTerminalRunsAlone(t *testing.T) {
	// Runs already in a terminal state must never be
	// touched by the janitor — re-completing or re-failing
	// would double-decrement runsActive metrics and re-emit
	// terminal events.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name: "reconciler-terminal", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "a", Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	seedWorkflowDef(t, nc, wfDef)

	orch := NewOrchestrator(nc)
	ctx := context.Background()

	for _, status := range []dag.RunStatus{
		dag.RunStatusCompleted,
		dag.RunStatusFailed,
		dag.RunStatusCancelled,
	} {
		run := dag.WorkflowRun{
			RunID:      "terminal-" + status.String(),
			WorkflowID: wfDef.Name,
			Status:     status,
			CreatedAt: time.Now().UTC().
				Add(-(reconcileMinAge + time.Minute)),
			Steps: map[string]dag.StepState{
				"a": {Status: dag.StepStatusCompleted},
			},
		}
		if err := orch.store.Save(ctx, run); err != nil {
			t.Fatalf("seed %v: %v", status, err)
		}
	}

	orch.reconcileRunningRuns(ctx)

	for _, status := range []dag.RunStatus{
		dag.RunStatusCompleted,
		dag.RunStatusFailed,
		dag.RunStatusCancelled,
	} {
		after, err := orch.store.Load(
			ctx, "terminal-"+status.String(),
		)
		if err != nil {
			t.Fatalf("load %v: %v", status, err)
		}
		if after.Status != status {
			t.Errorf(
				"terminal %v was modified; "+
					"Status now %v",
				status, after.Status,
			)
		}
	}
}

// seedWorkflowDef writes a WorkflowDef into the workflow_defs
// KV bucket so loadRunAndDef can resolve it during reconcile.
func seedWorkflowDef(
	t *testing.T, nc *nats.Conn, wfDef dag.WorkflowDef,
) {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	defKV, err := js.KeyValue("workflow_defs")
	if err != nil {
		t.Fatalf("workflow_defs KV: %v", err)
	}
	mustPut(t, defKV, wfDef.Name, mustMarshal(t, wfDef))
}
