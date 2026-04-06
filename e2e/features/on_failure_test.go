// e2e/features/on_failure_test.go
// Tests on-failure handler execution. Methodology: main step fails,
// fallback step executes with error context in its input, workflow
// completes with main step Recovered.
package features

import (
	"context"
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

		time.Sleep(5 * time.Second)

		run, _ := svc.GetRun(
			context.Background(), runID,
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

		// Positive: workflow completed (not failed)
		if run.Status != dag.RunStatusCompleted {
			t.Fatalf("run: expected completed, got %s",
				run.Status)
		}

		// Negative: fallback received error context
		if len(fallbackInput) == 0 {
			t.Fatal("fallback received no input")
		}
	})
}
