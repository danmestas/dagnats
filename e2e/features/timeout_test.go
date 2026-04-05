// e2e/features/timeout_test.go
// Tests workflow timeout enforcement. Methodology: workflow with 2s
// timeout, agent loop step that continues every 500ms. Verify the
// orchestrator cancels the workflow on next event after deadline.
package features

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestWorkflowTimeout(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Agent loop that never completes — continues with delay.
		harness.SubscribeWorker(t, nc, "infinite",
			func(tc worker.TaskContext) error {
				time.Sleep(200 * time.Millisecond)
				return tc.Continue([]byte(`"tick"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "timeout")
		wb := dag.NewWorkflow(wfName)
		wb.AgentLoop("loop", "infinite").
			WithMaxIterations(1000)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		wfDef.Timeout = 2 * time.Second

		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: workflow gets cancelled due to timeout.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCancelled, 15*time.Second,
		)

		// Negative: it didn't complete successfully.
		if run.Status == dag.RunStatusCompleted {
			t.Fatal("expected cancelled, got completed")
		}
	})
}
