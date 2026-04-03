// e2e/features/conditional_test.go
// Tests conditional step skipping. Methodology: A→B→C where B has
// SkipIf condition on A's output. When A outputs the skip value,
// B is skipped and C still runs.
package features

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestConditionalSkip(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "check",
			func(tc worker.TaskContext) error {
				return tc.Complete(
					[]byte(`{"action":"skip"}`),
				)
			},
		)
		harness.SubscribeWorker(t, nc, "process",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"processed"`))
			},
		)
		harness.SubscribeWorker(t, nc, "finalize",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"finalized"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "conditional")
		wb := dag.NewWorkflow(wfName)
		check := wb.Task("check", "check")
		process := wb.Task("process", "process").
			After(check).
			SkipIf(dag.SkipIfOutput(
				check, "action", "==", "skip",
			))
		wb.Task("finalize", "finalize").After(process)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Positive: process was skipped.
		if run.Steps["process"].Status != dag.StepStatusSkipped {
			t.Fatalf("process: expected skipped, got %s",
				run.Steps["process"].Status)
		}

		// Negative: finalize still ran (skipped counts as satisfied).
		if run.Steps["finalize"].Status != dag.StepStatusCompleted {
			t.Fatalf("finalize: expected completed, got %s",
				run.Steps["finalize"].Status)
		}
	})
}
