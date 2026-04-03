// e2e/features/fanout_test.go
// Tests parallel fan-out and join. Methodology: workflow A→(B,C,D)→E
// where B,C,D run in parallel and E waits for all three. Verify all
// 5 steps complete and E runs last.
package features

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestParallelFanOut(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Workers for all 5 steps.
		for _, name := range []string{
			"entry", "p1", "p2", "p3", "join",
		} {
			name := name
			harness.SubscribeWorker(t, nc, name,
				func(tc worker.TaskContext) error {
					return tc.Complete(
						[]byte(`"` + name + `-done"`),
					)
				},
			)
		}

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "fanout")
		wb := dag.NewWorkflow(wfName)
		entry := wb.Task("entry", "entry")
		p1 := wb.Task("p1", "p1").After(entry)
		p2 := wb.Task("p2", "p2").After(entry)
		p3 := wb.Task("p3", "p3").After(entry)
		wb.Task("join", "join").After(p1, p2, p3)
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

		// Positive: all 5 steps completed.
		for _, id := range []string{
			"entry", "p1", "p2", "p3", "join",
		} {
			if run.Steps[id].Status != dag.StepStatusCompleted {
				t.Fatalf("step %s: %s", id, run.Steps[id].Status)
			}
		}

		// Negative: join ran after all parallel steps.
		harness.AssertHistoryContains(t, svc, runID,
			protocol.EventWorkflowStarted,
			protocol.EventWorkflowCompleted,
		)

		// Negative: join output confirms it ran.
		if string(run.Steps["join"].Output) != `"join-done"` {
			t.Fatalf("join output: %s",
				string(run.Steps["join"].Output))
		}
	})
}
