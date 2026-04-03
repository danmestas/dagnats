// e2e/features/retry_dlq_test.go
// Tests step failure propagation and non-retryable immediate failure.
// Methodology: handler explicitly fails (tc.Fail) with no retry policy,
// verify workflow fails. For non-retryable, handler returns
// NonRetryableError wrapper, verify the worker framework calls Fail
// and workflow fails immediately.
package features

import (
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

func TestRetryExhaustion(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Handler explicitly fails via tc.Fail(). No retry policy
		// configured, so the orchestrator treats the first failure
		// as permanent (retries exhausted immediately).
		harness.SubscribeWorker(t, nc, "flaky",
			func(tc worker.TaskContext) error {
				tc.Fail(fmt.Errorf("transient failure"))
				return nil
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "retry-exhaust")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "flaky")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: workflow fails after step failure.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusFailed, 15*time.Second,
		)

		// Positive: step is marked failed.
		if run.Steps["step"].Status != dag.StepStatusFailed {
			t.Fatalf("step status: %s", run.Steps["step"].Status)
		}

		// Negative: step was attempted exactly once (no retries).
		if run.Steps["step"].Attempts != 1 {
			t.Fatalf("expected 1 attempt, got %d",
				run.Steps["step"].Attempts)
		}
	})
}

func TestNonRetryableError(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Handler returns NonRetryableError — the worker framework
		// calls tc.Fail() and acks the message (no NAK/redelivery).
		harness.SubscribeWorker(t, nc, "fatal",
			func(tc worker.TaskContext) error {
				return worker.NewNonRetryableError(
					fmt.Errorf("config missing"),
				)
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "non-retryable")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "fatal")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: workflow fails immediately.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusFailed, 15*time.Second,
		)

		// Positive: step is marked failed.
		if run.Steps["step"].Status != dag.StepStatusFailed {
			t.Fatalf("step status: %s", run.Steps["step"].Status)
		}

		// Negative: only 1 attempt — no retries used.
		if run.Steps["step"].Attempts != 1 {
			t.Fatalf("expected 1 attempt (non-retryable), got %d",
				run.Steps["step"].Attempts)
		}
	})
}
