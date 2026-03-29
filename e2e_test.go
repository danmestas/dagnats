// e2e_test.go
// End-to-end test: register a workflow, start a run, workers execute all steps,
// verify workflow completes with correct state in KV and event history.
// Methodology: real NATS server, real orchestrator, real workers. No mocks.
package dagnats_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestE2ELinearWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	// Start orchestrator
	orch := engine.NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
	orch.Start()
	defer orch.Stop()

	// Register workers
	w := worker.NewWorker(nc, observe.NewNoopLogger())
	w.Handle("task-a", func(ctx worker.TaskContext) error {
		return ctx.Complete([]byte(`"a-output"`))
	})
	w.Handle("task-b", func(ctx worker.TaskContext) error {
		return ctx.Complete([]byte(`"b-output"`))
	})
	w.Start()
	defer w.Stop()

	// Register workflow and start run via service
	svc := api.NewService(nc, observe.NewNoopLogger())
	wfDef, err := dag.NewWorkflow("e2e-linear").
		Task("a", "task-a").
		Task("b", "task-b").DependsOn("a").
		Build()
	if err != nil {
		t.Fatalf("Build failed: %v", err)
	}
	if err := svc.RegisterWorkflow(wfDef); err != nil {
		t.Fatalf("RegisterWorkflow failed: %v", err)
	}
	runID, err := svc.StartRun("e2e-linear", nil)
	if err != nil {
		t.Fatalf("StartRun failed: %v", err)
	}

	// Poll for workflow completion (bounded timeout)
	deadline := time.After(10 * time.Second)
	for {
		run, err := svc.GetRun(runID)
		if err != nil {
			t.Fatalf("GetRun failed: %v", err)
		}
		if run.Status == dag.RunStatusCompleted {
			if run.Steps["a"].Status != dag.StepStatusCompleted {
				t.Fatalf("step-a status = %v, want Completed", run.Steps["a"].Status)
			}
			if run.Steps["b"].Status != dag.StepStatusCompleted {
				t.Fatalf("step-b status = %v, want Completed", run.Steps["b"].Status)
			}
			break
		}
		if run.Status == dag.RunStatusFailed {
			t.Fatal("workflow failed unexpectedly")
		}
		select {
		case <-deadline:
			t.Fatalf("workflow did not complete within 10s, status: %v", run.Status)
		case <-time.After(100 * time.Millisecond):
		}
	}

	// Paired assertion: verify history stream has correct events
	js, _ := nc.JetStream()
	sub, _ := js.SubscribeSync("history."+runID, nats.DeliverAll())
	var eventTypes []string
	for {
		msg, err := sub.NextMsg(1 * time.Second)
		if err != nil {
			break
		}
		var evt engine.Event
		json.Unmarshal(msg.Data, &evt)
		eventTypes = append(eventTypes, string(evt.Type))
	}

	foundStart := false
	foundEnd := false
	completedCount := 0
	for _, et := range eventTypes {
		if et == "workflow.started" {
			foundStart = true
		}
		if et == "workflow.completed" {
			foundEnd = true
		}
		if et == "step.completed" {
			completedCount++
		}
	}
	if !foundStart {
		t.Fatal("history missing workflow.started event")
	}
	if !foundEnd {
		t.Fatal("history missing workflow.completed event")
	}
	if completedCount < 2 {
		t.Fatalf("expected at least 2 step.completed events, got %d", completedCount)
	}
}
