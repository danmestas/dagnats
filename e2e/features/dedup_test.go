// e2e/features/dedup_test.go
// Tests JetStream deduplication via Nats-Msg-Id. Methodology: publish
// same workflow.started event twice with identical MsgId, verify only
// one run is created.
package features

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestDeduplication(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		harness.SubscribeWorker(t, nc, "dedup-task",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"done"`))
			},
		)

		svc := harness.NewTestService(t, nc)
		ctx := context.Background()
		js, _ := nc.JetStream()

		wfName := harness.UniqueName(t, "dedup-wf")
		wb := dag.NewWorkflow(wfName)
		wb.Task("step", "dedup-task")
		wfDef, err := wb.Build()
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		err = svc.RegisterWorkflow(ctx, wfDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow: %v", err)
		}

		// Manually publish workflow.started event twice with same MsgId.
		runID := "dedup-run-" + harness.UniqueName(t, "id")
		defData, _ := json.Marshal(wfDef)
		evt := protocol.NewWorkflowEvent(
			protocol.EventWorkflowStarted, runID, defData,
		)
		evtData, _ := evt.Marshal()
		msgID := evt.NATSMsgID()

		// First publish.
		_, err = js.Publish(
			evt.NATSSubject(), evtData, nats.MsgId(msgID),
		)
		if err != nil {
			t.Fatalf("Publish 1: %v", err)
		}

		// Second publish — same MsgId (should be deduped).
		_, err = js.Publish(
			evt.NATSSubject(), evtData, nats.MsgId(msgID),
		)
		if err != nil {
			t.Fatalf("Publish 2: %v", err)
		}

		// Wait for the run to complete. Supercluster needs extra
		// time for cross-cluster replication and leader election.
		harness.WaitForRunStatus(
			t, svc, runID,
			dag.RunStatusCompleted, 30*time.Second,
		)

		// Positive: exactly one run exists.
		runs, err := svc.ScanRuns(ctx, api.RunsFilter{Workflow: wfName}, 0)
		if err != nil {
			t.Fatalf("ScanRuns: %v", err)
		}

		count := 0
		for _, r := range runs {
			if r.RunID == runID {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("expected 1 run, got %d", count)
		}

		// Negative: no duplicate run created.
		if len(runs) > 1 {
			t.Fatalf("expected 1 total run, got %d", len(runs))
		}
	})
}
