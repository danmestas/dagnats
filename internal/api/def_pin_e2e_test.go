// api/def_pin_e2e_test.go
//
// End-to-end tests for run-def pinning (#637): a POST /workflows
// re-register mid-flight must never change what a running run sees
// on its next advance, and a run started after the re-register must
// use the new def. The legacy (pre-#637, DefHash=="") and missing-
// pinned-version fallback paths are covered as white-box unit tests
// in internal/engine/def_pin_test.go, where loadRunAndDef itself is
// directly callable.
// Methodology: real embedded NATS server + orchestrator + worker,
// driven end to end through svc.StartRun / RegisterWorkflow and real
// task completion -- no mocking of the KV or the advance path. Every
// test asserts positive AND negative space. Bounded <=10s waits.
package api

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/worker"
)

// waitForRunStatus polls GetRun until status is reached or the
// bounded deadline elapses.
func waitForRunStatus(
	t *testing.T, svc *Service, runID string, status dag.RunStatus,
) dag.WorkflowRun {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		run, err := svc.GetRun(context.Background(), runID)
		if err == nil && run.Status == status {
			return run
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("run %q did not reach status %v within deadline", runID, status)
	return dag.WorkflowRun{}
}

// twoStepDef builds a two-step def named name: "a" -> secondID
// (task secondTask), for constructing a v1/v2 pair that renames the
// second step.
func twoStepDef(name, secondID, secondTask string) dag.WorkflowDef {
	if name == "" || secondID == "" || secondTask == "" {
		panic("twoStepDef: all arguments must be non-empty")
	}
	return dag.WorkflowDef{
		Name: name, Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
			{
				ID: secondID, Task: secondTask, Type: dag.StepTypeNormal,
				DependsOn: []string{"a"},
			},
		},
	}
}

// TestRunPinnedToStartDefSurvivesReRegister is the core #637
// scenario: a run started under v1 must complete under v1 even
// though a re-register mid-flight moves the name -> latest pointer
// to v2 (which renamed the second step). A run started AFTER the
// re-register must use v2.
func TestRunPinnedToStartDefSurvivesReRegister(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(orch.Stop)

	const wfName = "pin-test-wf"
	v1 := twoStepDef(wfName, "b", "task-b")
	v2 := twoStepDef(wfName, "b-v2", "task-b-v2")

	// gateA holds task-a's completion until the test has re-registered
	// v2, so the race the bug describes -- re-register landing between
	// task-a's completion event and the orchestrator's next advance
	// -- is deterministic instead of best-effort timing.
	gateA := make(chan struct{})
	enteredA := make(chan struct{}, 1)
	w := worker.NewWorker(nc)
	w.Handle("task-a", func(ctx worker.TaskContext) error {
		enteredA <- struct{}{}
		<-gateA
		return ctx.Complete(nil)
	})
	w.Handle("task-b", func(ctx worker.TaskContext) error {
		return ctx.Complete(nil)
	})
	w.Handle("task-b-v2", func(ctx worker.TaskContext) error {
		return ctx.Complete(nil)
	})
	w.Start()
	t.Cleanup(w.Stop)

	svc := NewService(nc)
	if err := svc.RegisterWorkflow(context.Background(), v1); err != nil {
		t.Fatalf("RegisterWorkflow v1: %v", err)
	}

	runID, err := svc.StartRun(context.Background(), wfName, nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	select {
	case <-enteredA:
	case <-time.After(10 * time.Second):
		t.Fatal("task-a handler never entered")
	}

	// Re-register v2 WHILE task-a is still in flight -- exactly the
	// hazard #637 describes.
	if err := svc.RegisterWorkflow(context.Background(), v2); err != nil {
		t.Fatalf("RegisterWorkflow v2: %v", err)
	}
	close(gateA)

	run := waitForRunStatus(t, svc, runID, dag.RunStatusCompleted)
	// Positive: the run completed under v1's step shape.
	if _, ok := run.Steps["b"]; !ok {
		t.Fatalf("run.Steps missing v1's step %q: %+v", "b", run.Steps)
	}
	// Negative: it never saw v2's renamed step.
	if _, ok := run.Steps["b-v2"]; ok {
		t.Fatalf("run.Steps must not contain v2's step %q", "b-v2")
	}
	if run.DefHash != dag.DefHash(v1) {
		t.Fatalf("run.DefHash = %q, want v1 hash %q",
			run.DefHash, dag.DefHash(v1))
	}

	// A run started AFTER the re-register must use v2.
	runID2, err := svc.StartRun(context.Background(), wfName, nil)
	if err != nil {
		t.Fatalf("StartRun (post re-register): %v", err)
	}
	run2 := waitForRunStatus(t, svc, runID2, dag.RunStatusCompleted)
	if _, ok := run2.Steps["b-v2"]; !ok {
		t.Fatalf("post-re-register run.Steps missing v2's step %q: %+v",
			"b-v2", run2.Steps)
	}
	if run2.DefHash != dag.DefHash(v2) {
		t.Fatalf("run2.DefHash = %q, want v2 hash %q",
			run2.DefHash, dag.DefHash(v2))
	}
}
