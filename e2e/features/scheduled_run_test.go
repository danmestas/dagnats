// e2e/features/scheduled_run_test.go
// Tests full scheduled run lifecycle: schedule -> timer fires ->
// worker executes -> workflow completes.
// Methodology: real embedded NATS, real orchestrator, real worker.
package features

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestScheduledRunE2E(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {

		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Register a simple worker.
		harness.SubscribeWorker(t, nc, "echo",
			func(tc worker.TaskContext) error {
				return tc.Complete(
					[]byte(`"scheduled-ok"`),
				)
			},
		)

		svc := harness.NewTestService(t, nc)

		// Register workflow.
		wb := dag.NewWorkflow("sched-e2e")
		wb.Task("echo-step", "echo")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		err = svc.RegisterWorkflow(
			context.Background(), wfDef,
		)
		if err != nil {
			t.Fatalf("Register: %v", err)
		}

		// Schedule 2 seconds from now.
		runAt := time.Now().Add(2 * time.Second)
		runID, err := svc.ScheduleRun(
			context.Background(), "sched-e2e", nil, runAt,
		)
		if err != nil {
			t.Fatalf("ScheduleRun: %v", err)
		}

		// Start timer consumer.
		timer := api.NewTimerConsumer(svc)
		if err := timer.Start(); err != nil {
			t.Fatalf("timer.Start: %v", err)
		}
		t.Cleanup(func() { timer.Stop() })

		// Poll with bounded deadline for run completion.
		deadline := time.Now().Add(15 * time.Second)
		var run dag.WorkflowRun
		for time.Now().Before(deadline) {
			run, err = svc.GetRun(
				context.Background(), runID,
			)
			if err == nil &&
				run.Status == dag.RunStatusCompleted {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if err != nil {
			t.Fatalf("GetRun: %v", err)
		}

		// Positive: run completed.
		if run.Status != dag.RunStatusCompleted {
			t.Fatalf("Status = %s, want completed",
				run.Status)
		}

		// Positive: echo step output is correct.
		if string(run.Steps["echo-step"].Output) !=
			`"scheduled-ok"` {
			t.Fatalf("output = %s, want scheduled-ok",
				run.Steps["echo-step"].Output)
		}

		// Negative: scheduled_runs entry cleaned up.
		_, err = svc.GetScheduledRun(runID)
		if err == nil {
			t.Fatal("scheduled entry should be deleted")
		}
	})
}
