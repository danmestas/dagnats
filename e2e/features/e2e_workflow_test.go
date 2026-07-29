// e2e/features/e2e_workflow_test.go
// End-to-end workflow tests migrated from the repo-root e2e_test.go so
// they run against every enabled topology via RunE2E. These read the
// raw history.<runID> JetStream stream and count event types directly
// (a distinct assertion path from AssertHistoryContains's service API).
// Methodology: real NATS server, real orchestrator, real workers. No
// mocks.
package features

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// historyEventsMax bounds the drain loop in collectHistoryEventTypes so
// it can never spin unboundedly (TigerStyle: fixed loop upper bounds).
const historyEventsMax = 1024

// waitForRunCompleted polls the run to completion, failing fast the
// instant it observes a failed status (preserving the root test's
// early-failure assertion) and tolerating the not-yet-materialized
// snapshot window (engine.ErrRunNotFound). Bounded by timeout.
func waitForRunCompleted(
	t *testing.T, svc *api.Service,
	runID string, timeout time.Duration,
) dag.WorkflowRun {
	t.Helper()
	if svc == nil {
		panic("waitForRunCompleted: svc must not be nil")
	}
	if runID == "" {
		panic("waitForRunCompleted: runID must not be empty")
	}
	ctx := context.Background()
	deadline := time.After(timeout)
	for {
		run, err := svc.GetRun(ctx, runID)
		if err == engine.ErrRunNotFound {
			select {
			case <-deadline:
				t.Fatalf("run snapshot did not appear within %s", timeout)
			case <-time.After(10 * time.Millisecond):
			}
			continue
		}
		if err != nil {
			t.Fatalf("GetRun failed: %v", err)
		}
		if run.Status == dag.RunStatusCompleted {
			return run
		}
		if run.Status == dag.RunStatusFailed {
			t.Fatal("workflow failed unexpectedly")
		}
		select {
		case <-deadline:
			t.Fatalf("run did not complete within %s, status: %v",
				timeout, run.Status)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// collectHistoryEventTypes drains the run's history stream and returns
// each event's type as a string. Bounded on both message wait and
// iteration count.
func collectHistoryEventTypes(
	t *testing.T, nc *nats.Conn, runID string,
) []string {
	t.Helper()
	if nc == nil {
		panic("collectHistoryEventTypes: nc must not be nil")
	}
	if runID == "" {
		panic("collectHistoryEventTypes: runID must not be empty")
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.SubscribeSync("history."+runID, nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync history: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()
	types := make([]string, 0, 8)
	for i := 0; i < historyEventsMax; i++ {
		msg, err := sub.NextMsg(1 * time.Second)
		if err != nil {
			break
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("Unmarshal failed: %v", err)
		}
		types = append(types, string(evt.Type))
	}
	return types
}

func TestE2ELinearWorkflow(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "task-a",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"a-output"`))
			})
		harness.SubscribeWorker(t, nc, "task-b",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"b-output"`))
			})

		svc := harness.NewTestService(t, nc)
		wb := dag.NewWorkflow(harness.UniqueName(t, "e2e-linear"))
		a := wb.Task("a", "task-a")
		wb.Task("b", "task-b").After(a)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: workflow completes with both steps completed.
		run := waitForRunCompleted(t, svc, runID, 15*time.Second)
		if run.Steps["a"].Status != dag.StepStatusCompleted {
			t.Fatalf("step-a status = %v, want Completed",
				run.Steps["a"].Status)
		}
		if run.Steps["b"].Status != dag.StepStatusCompleted {
			t.Fatalf("step-b status = %v, want Completed",
				run.Steps["b"].Status)
		}

		// Paired assertion: history stream has the expected events.
		foundStart, foundEnd, completedCount := false, false, 0
		for _, et := range collectHistoryEventTypes(t, nc, runID) {
			switch et {
			case "workflow.started":
				foundStart = true
			case "workflow.completed":
				foundEnd = true
			case "step.completed":
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
			t.Fatalf("expected at least 2 step.completed events, got %d",
				completedCount)
		}
	})
}

func TestE2EAgentLoop(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Worker that loops twice (Continue) then completes.
		iteration := 0
		harness.SubscribeWorker(t, nc, "looper",
			func(tc worker.TaskContext) error {
				iteration++
				if iteration < 3 {
					return tc.Continue(
						[]byte(fmt.Sprintf(`"iteration-%d"`, iteration)),
					)
				}
				return tc.Complete([]byte(`"done after 3"`))
			})

		svc := harness.NewTestService(t, nc)
		wb := dag.NewWorkflow(harness.UniqueName(t, "e2e-loop"))
		wb.AgentLoop("loop", "looper").WithMaxIterations(10)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build failed: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: the agent loop reaches completion.
		waitForRunCompleted(t, svc, runID, 15*time.Second)

		// Paired assertion: history contains the continue events.
		continueCount := 0
		for _, et := range collectHistoryEventTypes(t, nc, runID) {
			if et == string(protocol.EventStepContinue) {
				continueCount++
			}
		}
		if continueCount < 2 {
			t.Fatalf("expected at least 2 continue events, got %d",
				continueCount)
		}
	})
}
