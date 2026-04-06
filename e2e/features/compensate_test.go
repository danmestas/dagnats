// e2e/features/compensate_test.go
// Tests saga compensation: steps complete, then a downstream step
// fails. Compensate steps run in reverse order. Methodology: 3-step
// pipeline where the last step fails; verify compensation order.
package features

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestCompensateSaga(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		var order []string
		var mu sync.Mutex

		harness.SubscribeWorker(t, nc, "create",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"order-1"`))
			},
		)
		harness.SubscribeWorker(t, nc, "charge",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"txn-1"`))
			},
		)
		harness.SubscribeWorker(t, nc, "ship",
			func(tc worker.TaskContext) error {
				return worker.NewNonRetryableError(
					fmt.Errorf("warehouse fire"),
				)
			},
		)
		harness.SubscribeWorker(t, nc, "refund",
			func(tc worker.TaskContext) error {
				mu.Lock()
				order = append(order, "refund")
				mu.Unlock()
				// Verify input contains original output
				var input map[string]json.RawMessage
				json.Unmarshal(tc.Input(), &input)
				if input["original_step"] == nil {
					t.Error("missing original_step in input")
				}
				return tc.Complete([]byte(`"refunded"`))
			},
		)
		harness.SubscribeWorker(t, nc, "undo",
			func(tc worker.TaskContext) error {
				mu.Lock()
				order = append(order, "undo")
				mu.Unlock()
				return tc.Complete([]byte(`"undone"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		wfName := harness.UniqueName(t, "saga")
		wb := dag.NewWorkflow(wfName)
		create := wb.Task("create-order", "create")
		charge := wb.Task("charge-payment", "charge").
			After(create)
		wb.Task("ship-order", "ship").After(charge)

		undoCreate := wb.Task("undo-create", "undo")
		refund := wb.Task("refund-payment", "refund")
		create.Compensate(undoCreate)
		charge.Compensate(refund)

		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		runID := harness.RegisterAndStart(t, svc, wfDef, nil)

		deadline := time.After(15 * time.Second)
		ticker := time.NewTicker(200 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-deadline:
				run, _ := svc.GetRun(
					context.Background(), runID,
				)
				t.Fatalf("timeout: run status=%s", run.Status)
			case <-ticker.C:
				run, _ := svc.GetRun(
					context.Background(), runID,
				)
				if run.Status == dag.RunStatusCompensated {
					mu.Lock()
					defer mu.Unlock()
					// Positive: 2 compensations ran
					if len(order) != 2 {
						t.Fatalf("expected 2, got %d",
							len(order))
					}
					// Positive: reverse order
					if order[0] != "refund" {
						t.Fatalf("[0] = %q, want refund",
							order[0])
					}
					if order[1] != "undo" {
						t.Fatalf("[1] = %q, want undo",
							order[1])
					}
					return
				}
			}
		}
	})
}
