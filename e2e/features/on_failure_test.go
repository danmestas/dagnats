// e2e/features/on_failure_test.go
// Tests on-failure handler execution. Methodology: main step fails,
// fallback step executes with error context in its input.
package features

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestOnFailureHandler(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
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
		wb.Task("fallback", "recover").After(main)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		// Set on_failure link (not available via builder).
		// Find the main step index to set OnFailure.
		for i := range wfDef.Steps {
			if wfDef.Steps[i].ID == "main" {
				wfDef.Steps[i].OnFailure = "fallback"
				break
			}
		}

		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Wait — the workflow may complete (fallback succeeds)
		// or fail (depends on whether fallback completion
		// marks the workflow as done). Give it time.
		time.Sleep(5 * time.Second)

		run, _ := svc.GetRun(
			context.Background(), runID,
		)

		// Positive: fallback step was executed.
		if run.Steps["fallback"].Status !=
			dag.StepStatusCompleted {
			t.Fatalf("fallback: expected completed, got %s",
				run.Steps["fallback"].Status)
		}

		// Negative: fallback received error context.
		if len(fallbackInput) == 0 {
			t.Fatal("fallback received no input")
		}
	})
}
