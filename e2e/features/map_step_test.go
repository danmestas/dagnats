// e2e/features/map_step_test.go
// Tests dynamic fan-out/fan-in via Map step: fetch produces array,
// map processes each element, summarize receives ordered results.
// Methodology: real embedded NATS, real orchestrator, real workers.
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

func TestMapStepE2E(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {

		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		// "fetch" worker returns a JSON array of 3 items.
		harness.SubscribeWorker(t, nc, "fetch-items",
			func(tc worker.TaskContext) error {
				items := []map[string]string{
					{"id": "a"},
					{"id": "b"},
					{"id": "c"},
				}
				data, err := json.Marshal(items)
				if err != nil {
					return tc.Fail(err)
				}
				return tc.Complete(data)
			},
		)

		// "process" worker transforms each item.
		harness.SubscribeWorker(t, nc, "process-item",
			func(tc worker.TaskContext) error {
				var item map[string]string
				if err := json.Unmarshal(
					tc.Input(), &item,
				); err != nil {
					return tc.Fail(err)
				}
				result := map[string]string{
					"processed": item["id"],
				}
				data, err := json.Marshal(result)
				if err != nil {
					return tc.Fail(err)
				}
				return tc.Complete(data)
			},
		)

		// "summarize" worker receives the collected array.
		var summaryInput json.RawMessage
		harness.SubscribeWorker(t, nc, "summarize",
			func(tc worker.TaskContext) error {
				summaryInput = tc.Input()
				return tc.Complete(
					[]byte(`"summary-done"`),
				)
			},
		)

		svc := harness.NewTestService(t, nc)

		// Build workflow: fetch -> map(process) -> summarize
		wb := dag.NewWorkflow("map-e2e")
		fetch := wb.Task("fetch", "fetch-items")
		process := wb.Map(
			"process-each", "process-item",
		).After(fetch)
		wb.Task(
			"summarize", "summarize",
		).After(process)
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

		runID, err := svc.StartRun(
			context.Background(), "map-e2e", nil,
		)
		if err != nil {
			t.Fatalf("StartRun: %v", err)
		}

		// Poll with bounded deadline.
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

		// Positive: workflow completed.
		if run.Status != dag.RunStatusCompleted {
			t.Fatalf("Status = %s, want completed",
				run.Status)
		}

		// Positive: map step has 3 completed instances.
		mapState := run.Steps["process-each"]
		if len(mapState.MapInstances) != 3 {
			t.Fatalf("MapInstances len = %d, want 3",
				len(mapState.MapInstances))
		}
		for i, inst := range mapState.MapInstances {
			if inst.Status != dag.StepStatusCompleted {
				t.Fatalf(
					"instance %d status = %s, want completed",
					i, inst.Status,
				)
			}
		}

		// Positive: summarize received 3-element array.
		if summaryInput == nil {
			t.Fatal("summarize received no input")
		}
		var collected []json.RawMessage
		if err := json.Unmarshal(
			summaryInput, &collected,
		); err != nil {
			t.Fatalf("unmarshal summary input: %v", err)
		}
		if len(collected) != 3 {
			t.Fatalf("collected len = %d, want 3",
				len(collected))
		}

		// Positive: results are in order (a, b, c).
		for i, expected := range []string{
			"a", "b", "c",
		} {
			var item map[string]string
			json.Unmarshal(collected[i], &item)
			if item["processed"] != expected {
				t.Fatalf(
					"collected[%d].processed = %q, want %q",
					i, item["processed"], expected,
				)
			}
		}
	})
}
