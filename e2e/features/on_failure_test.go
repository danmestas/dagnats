// e2e/features/on_failure_test.go
// Tests on-failure handler execution. Methodology: main step fails,
// fallback step executes with error context in its input, workflow
// completes with main step Recovered.
package features

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

// onFailureCompleteCeiling bounds the wait for the fallback step to
// run and the workflow to settle. Generous by design: under CPU
// contention the fallback simply takes longer, and the old fixed 5s
// sleep turned that into "fallback: expected completed, got running"
// — a false claim about behavior (#558).
const onFailureCompleteCeiling = 60 * time.Second

func TestOnFailureHandler(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "risky",
			func(tc worker.TaskContext) error {
				return worker.NewNonRetryableError(
					fmt.Errorf("disk full"),
				)
			},
		)

		var fallbackInput json.RawMessage
		harness.SubscribeWorker(t, nc, "recover",
			func(tc worker.TaskContext) error {
				fallbackInput = tc.Input()
				return tc.Complete([]byte(`"recovered"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "on-failure")
		wb := dag.NewWorkflow(wfName)
		main := wb.Task("main", "risky")
		fallback := wb.Task("fallback", "recover")
		main.OnFailure(fallback)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}

		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: the run reaches Completed, not Failed — the whole
		// point of the on-failure handler. Polled rather than slept
		// on, so contention costs wall-clock instead of reporting a
		// still-running fallback as a wrong step status.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, onFailureCompleteCeiling,
		)

		// Positive: fallback step was executed
		if run.Steps["fallback"].Status !=
			dag.StepStatusCompleted {
			t.Fatalf("fallback: expected completed, got %s",
				run.Steps["fallback"].Status)
		}

		// Positive: main step is recovered
		if run.Steps["main"].Status !=
			dag.StepStatusRecovered {
			t.Fatalf("main: expected recovered, got %s",
				run.Steps["main"].Status)
		}

		// Negative: fallback received error context
		if len(fallbackInput) == 0 {
			t.Fatal("fallback received no input")
		}
	})
}
