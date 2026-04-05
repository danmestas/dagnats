// e2e/features/agent_loop_test.go
// Tests agent loop with checkpoints. Methodology: step calls Continue()
// 3 times with Checkpoint() each iteration, then Complete(). Verify
// iteration count and checkpoint persistence.
package features

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestAgentLoop(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		tel := observe.NewNoopTelemetry()

		// Agent loop needs "checkpoints" KV bucket.
		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("JetStream: %v", err)
		}
		_, err = js.CreateKeyValue(
			&nats.KeyValueConfig{Bucket: "checkpoints"},
		)
		if err != nil {
			t.Fatalf("CreateKeyValue checkpoints: %v", err)
		}

		orch := engine.NewOrchestrator(nc, tel)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// Agent loop: continue 3 times, then complete.
		harness.SubscribeWorker(t, nc, "counter",
			func(tc worker.TaskContext) error {
				// Load checkpoint to get current count.
				var count int
				cp, _ := tc.LoadCheckpoint()
				if cp != nil {
					json.Unmarshal(cp, &count)
				}
				count++

				// Save checkpoint.
				cpData, _ := json.Marshal(count)
				tc.Checkpoint(cpData)

				if count >= 3 {
					return tc.Complete(cpData)
				}
				return tc.Continue(cpData)
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "agent-loop")
		wb := dag.NewWorkflow(wfName)
		wb.AgentLoop("loop", "counter").
			WithMaxIterations(10)
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		// Positive: workflow completes.
		run := harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Positive: step completed.
		if run.Steps["loop"].Status != dag.StepStatusCompleted {
			t.Fatalf("step: %s", run.Steps["loop"].Status)
		}

		// Negative: iterations happened (count=3 in output).
		if string(run.Steps["loop"].Output) != "3" {
			t.Fatalf("output: %s",
				string(run.Steps["loop"].Output))
		}
	})
}
