// e2e/features/signals_test.go
// Tests cross-step signal coordination. Methodology: step blocks on
// WaitForSignal, external SendSignal unblocks it, workflow completes.
package features

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestSignalWait(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {

		// Signals require the "signals" KV bucket.
		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStream: %v", err)
		}
		_, err = js.CreateKeyValue(
			&nats.KeyValueConfig{Bucket: "signals"},
		)
		if err != nil {
			t.Fatalf("CreateKeyValue signals: %v", err)
		}

		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Step waits for "approval" signal.
		harness.SubscribeWorker(t, nc, "wait-for-approval",
			func(tc worker.TaskContext) error {
				data, err := tc.WaitForSignal(
					"approval", 30*time.Second,
				)
				if err != nil {
					return err
				}
				return tc.Complete(data)
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "signal")
		wb := dag.NewWorkflow(wfName)
		wb.Task("wait", "wait-for-approval")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		ctx := context.Background()

		// Signalling before the worker has claimed the step races the
		// step's own KV watcher, and the miss then looks like a
		// signal-delivery bug rather than a slow worker start.
		harness.WaitForPrecondition(t,
			"worker started the \"wait\" step and is blocking on "+
				"the approval signal",
			stepRunningCeiling,
			func() bool {
				run, err := svc.GetRun(ctx, runID)
				if err != nil {
					return false
				}
				return run.Steps["wait"].Status ==
					dag.StepStatusRunning
			},
		)

		// Send the signal via API.
		err = svc.SendSignal(
			ctx, runID, "approval", []byte(`"approved"`),
		)
		if err != nil {
			t.Fatalf("SendSignal: %v", err)
		}

		// Positive: workflow completes.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Negative: step output contains the signal data.
		if string(run.Steps["wait"].Output) != `"approved"` {
			t.Fatalf("output: %s",
				string(run.Steps["wait"].Output))
		}
	})
}
