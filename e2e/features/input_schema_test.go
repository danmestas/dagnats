// e2e/features/input_schema_test.go
// Tests input schema validation. Methodology: register workflow with
// schema, start with invalid input (fails), start with valid input
// (succeeds).
package features

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestInputSchemaValidation(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "echo",
			func(tc worker.TaskContext) error {
				return tc.Complete(tc.Input())
			},
		)

		svc := harness.NewTestService(t, nc)
		ctx := context.Background()

		wfName := harness.UniqueName(t, "schema")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "echo")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		wfDef.InputSchema = json.RawMessage(`{
			"type": "object",
			"required": ["name"],
			"properties": {
				"name": {"type": "string"}
			}
		}`)
		err = svc.RegisterWorkflow(ctx, wfDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		// Positive: invalid input → rejected at API boundary.
		_, err = svc.StartRun(
			ctx, wfName, []byte(`{"age": 25}`),
		)
		if err == nil {
			t.Fatal("StartRun should reject invalid input")
		}

		// Negative: valid input → run completes.
		goodRunID, err := svc.StartRun(
			ctx, wfName, []byte(`{"name": "Dan"}`),
		)
		if err != nil {
			t.Fatalf("StartRun (good): %v", err)
		}
		harness.WaitForRunStatus(
			t, svc, goodRunID,
			dag.RunStatusCompleted, 15*time.Second,
		)
	})
}
