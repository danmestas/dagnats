// api/service_test.go
// Tests for the control plane service: register workflows, start runs, get status.
// Methodology: real embedded NATS. Verify KV state after each operation.
package api

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestServiceRegisterWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopLogger())
	wfDef, err := dag.NewWorkflow("test-wf").Task("a", "task-a").Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	err = svc.RegisterWorkflow(wfDef)
	if err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}
	got, err := svc.GetWorkflow("test-wf")
	if err != nil {
		t.Fatalf("GetWorkflow failed: %v", err)
	}
	if got.Name != "test-wf" {
		t.Fatalf("Name = %q, want %q", got.Name, "test-wf")
	}
}

func TestServiceStartRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopLogger())
	wfDef, _ := dag.NewWorkflow("test-wf").Task("a", "task-a").Build()
	svc.RegisterWorkflow(wfDef)
	runID, err := svc.StartRun("test-wf", []byte(`"input"`))
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}
	if runID == "" {
		t.Fatal("runID must not be empty")
	}
}

func TestServiceGetRunStatus(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	// The orchestrator is the sole owner of run state — start it so the
	// WorkflowStarted event is processed and the snapshot is created.
	orch := engine.NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
	orch.Start()
	defer orch.Stop()

	svc := NewService(nc, observe.NewNoopLogger())
	wfDef, _ := dag.NewWorkflow("test-wf").Task("a", "task-a").Build()
	svc.RegisterWorkflow(wfDef)
	runID, _ := svc.StartRun("test-wf", nil)

	// Poll for the snapshot to appear; the orchestrator processes the event
	// asynchronously so a brief wait is required (bounded to 5s).
	var run dag.WorkflowRun
	deadline := time.After(5 * time.Second)
	for {
		run, err = svc.GetRun(runID)
		if err == nil {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("run snapshot did not appear within 5s: %v", err)
		case <-time.After(10 * time.Millisecond):
		}
	}

	if run.RunID != runID {
		t.Fatalf("RunID = %q, want %q", run.RunID, runID)
	}
	if run.Status != dag.RunStatusPending && run.Status != dag.RunStatusRunning {
		t.Fatalf("Status = %v, want Pending or Running", run.Status)
	}
}

func TestServiceGetRunNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	svc := NewService(nc, observe.NewNoopLogger())
	_, err = svc.GetRun("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent run")
	}
}
