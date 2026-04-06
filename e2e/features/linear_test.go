// e2e/features/linear_test.go
// Tests a basic sequential workflow A→B through the full stack.
// Methodology: register workflow, start run, workers execute steps,
// verify completion status and event history. Runs against all
// enabled topologies via RunE2E.
package features

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestLinearWorkflow(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {

		// Start orchestrator.
		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Register workers.
		harness.SubscribeWorker(
			t, nc, "task-a",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"a-done"`))
			},
		)
		harness.SubscribeWorker(
			t, nc, "task-b",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"b-done"`))
			},
		)

		// Build and run workflow.
		svc := harness.NewTestService(t, nc)
		name := harness.UniqueName(t, "linear")
		wb := dag.NewWorkflow(name)
		a := wb.Task("a", "task-a")
		wb.Task("b", "task-b").After(a)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: workflow completes.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 15*time.Second,
		)
		if run.Status != dag.RunStatusCompleted {
			t.Fatalf("expected completed, got %s", run.Status)
		}

		// Positive: both steps completed.
		if run.Steps["a"].Status != dag.StepStatusCompleted {
			t.Fatalf("step a: %s", run.Steps["a"].Status)
		}
		if run.Steps["b"].Status != dag.StepStatusCompleted {
			t.Fatalf("step b: %s", run.Steps["b"].Status)
		}

		// Positive: correct event sequence in history.
		harness.AssertHistoryContains(t, svc, runID,
			protocol.EventWorkflowStarted,
			protocol.EventStepCompleted,
			protocol.EventStepCompleted,
			protocol.EventWorkflowCompleted,
		)

		// Negative: step outputs are preserved.
		if string(run.Steps["a"].Output) != `"a-done"` {
			t.Fatalf(
				"step a output: %s", string(run.Steps["a"].Output),
			)
		}
	})
}
