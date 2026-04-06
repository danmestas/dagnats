// e2e/features/worker_groups_test.go
// Tests worker group routing. Methodology: step has WorkerGroup="gpu",
// two workers subscribed (one with gpu group, one without). Verify
// only the gpu worker receives the task.
package features

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestWorkerGroups(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		var gpuCalled atomic.Bool
		var defaultCalled atomic.Bool

		// GPU worker — subscribes to gpu group.
		gpuWorker := worker.NewWorker(
			nc, worker.WithGroups("gpu"),
		)
		gpuWorker.Handle("render", func(tc worker.TaskContext) error {
			gpuCalled.Store(true)
			return tc.Complete([]byte(`"gpu-done"`))
		})
		gpuWorker.Start()
		t.Cleanup(func() { gpuWorker.Stop() })

		// CPU worker — subscribes to cpu group (not gpu).
		defaultWorker := worker.NewWorker(
			nc, worker.WithGroups("cpu"),
		)
		defaultWorker.Handle("render",
			func(tc worker.TaskContext) error {
				defaultCalled.Store(true)
				return tc.Complete([]byte(`"default-done"`))
			},
		)
		defaultWorker.Start()
		t.Cleanup(func() { defaultWorker.Stop() })

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "worker-groups")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "render")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		wfDef.Steps[0].WorkerGroup = "gpu"

		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Positive: GPU worker handled the task.
		if !gpuCalled.Load() {
			t.Fatal("GPU worker was not called")
		}

		// Negative: cpu worker did NOT handle it.
		if defaultCalled.Load() {
			t.Fatal("cpu worker should not have been called")
		}
	})
}
