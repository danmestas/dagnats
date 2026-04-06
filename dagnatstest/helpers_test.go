// dagnatstest/helpers_test.go
// Tests for RunAndWait and WaitForStatus helpers. Methodology:
// integration tests with real embedded NATS — verify that helpers
// correctly poll and return when workflow reaches target status.
package dagnatstest

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
)

func TestRunAndWait_Completed(t *testing.T) {
	nc := Server(t)
	tel := observe.NewNoopTelemetry()

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(func() { orch.Stop() })

	w := worker.NewWorker(nc, tel)
	w.Handle("echo", func(tc worker.TaskContext) error {
		return tc.Complete(tc.Input())
	})
	w.Start()
	t.Cleanup(func() { w.Stop() })

	svc := api.NewService(nc)
	wb := dag.NewWorkflow("run-and-wait-test")
	wb.Task("step1", "echo")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	err = svc.RegisterWorkflow(context.Background(), wfDef)
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	run := RunAndWait(
		t, svc, "run-and-wait-test",
		[]byte(`"hello"`), 10*time.Second,
	)

	// Positive: status is completed.
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"expected Completed, got %s", run.Status,
		)
	}

	// Positive: RunID is set.
	if run.RunID == "" {
		t.Fatal("expected non-empty RunID")
	}
}

func TestWaitForStatus_SpecificStatus(t *testing.T) {
	nc := Server(t)

	orch := engine.NewOrchestrator(nc)
	orch.Start()
	t.Cleanup(func() { orch.Stop() })

	// No worker — run will stay Pending or move to Running
	// but never complete.
	svc := api.NewService(nc)
	wb := dag.NewWorkflow("wait-status-test")
	wb.Task("step1", "noop")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	err = svc.RegisterWorkflow(context.Background(), wfDef)
	if err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	runID, err := svc.StartRun(
		context.Background(), "wait-status-test", nil,
	)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}

	run := WaitForStatus(
		t, svc, runID, 10*time.Second,
		dag.RunStatusPending, dag.RunStatusRunning,
	)

	// Positive: status matches one of the targets.
	if run.Status != dag.RunStatusPending &&
		run.Status != dag.RunStatusRunning {
		t.Fatalf(
			"expected Pending or Running, got %s",
			run.Status,
		)
	}

	// Negative: RunID matches what we started.
	if run.RunID != runID {
		t.Fatalf(
			"expected RunID %s, got %s", runID, run.RunID,
		)
	}
}
