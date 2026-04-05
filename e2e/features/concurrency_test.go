// e2e/features/concurrency_test.go
// Tests workflow concurrency limits. Methodology: register workflow
// with max_runs=1, start 2 runs, verify first runs while second is
// pending, complete first, verify second auto-starts.
package features

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestConcurrencyLimit(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		// Create concurrency_runs KV bucket (not in default setup).
		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStream: %v", err)
		}
		_, err = js.CreateKeyValue(
			&nats.KeyValueConfig{Bucket: "concurrency_runs"},
		)
		if err != nil {
			t.Fatalf("CreateKeyValue: %v", err)
		}

		tel := observe.NewNoopTelemetry()
		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Worker that completes on command via a channel.
		gate := make(chan struct{}, 1)
		var taskCount atomic.Int32
		harness.SubscribeWorker(t, nc, "gated",
			func(tc worker.TaskContext) error {
				taskCount.Add(1)
				<-gate
				return tc.Complete([]byte(`"done"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		ctx := context.Background()
		wfName := harness.UniqueName(t, "concurrency")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "gated")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		wfDef.Concurrency = &dag.ConcurrencyLimit{MaxRuns: 1}

		err = svc.RegisterWorkflow(ctx, wfDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		// Start 2 runs.
		runID1, err := svc.StartRun(ctx, wfName, nil)
		if err != nil {
			t.Fatalf("StartRun 1: %v", err)
		}
		runID2, err := svc.StartRun(ctx, wfName, nil)
		if err != nil {
			t.Fatalf("StartRun 2: %v", err)
		}

		// Wait for run 1 to be running.
		harness.WaitForRunStatus(
			t, svc, runID1,
			dag.RunStatusRunning, 10*time.Second,
		)

		// Positive: run 2 is pending (concurrency limit).
		time.Sleep(1 * time.Second)
		run2, err := svc.GetRun(ctx, runID2)
		if err != nil {
			t.Fatalf("GetRun 2: %v", err)
		}
		if run2.Status != dag.RunStatusPending {
			t.Fatalf("run 2: expected pending, got %s",
				run2.Status)
		}

		// Complete run 1.
		gate <- struct{}{}
		harness.WaitForRunStatus(
			t, svc, runID1,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Negative: run 2 auto-starts and completes.
		gate <- struct{}{}
		harness.WaitForRunStatus(
			t, svc, runID2,
			dag.RunStatusCompleted, 15*time.Second,
		)
	})
}
