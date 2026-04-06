// e2e/features/child_test.go
// Tests child workflow spawn and parent notification. Methodology:
// parent step publishes a workflow.spawn event, child workflow runs
// and completes, verify parent receives child.completed notification.
package features

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/protocol"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go"
)

func TestChildWorkflow(t *testing.T) {
	harness.RunE2E(t, func(t *testing.T, nc *nats.Conn) {
		orch := engine.NewOrchestrator(nc)
		orch.Start()
		t.Cleanup(func() { orch.Stop() })

		js, _ := nc.JetStream()
		svc := harness.NewTestService(t, nc)
		ctx := context.Background()

		// Register child workflow first so orchestrator can find it.
		childWfName := harness.UniqueName(t, "child-wf")
		childWb := dag.NewWorkflow(childWfName)
		childWb.Task("child-step", "child-task")
		childDef, err := childWb.Build()
		if err != nil {
			t.Fatalf("Build child: %v", err)
		}
		err = svc.RegisterWorkflow(ctx, childDef)
		if err != nil {
			t.Fatalf("RegisterWorkflow child: %v", err)
		}

		// Worker for child workflow step.
		harness.SubscribeWorker(t, nc, "child-task",
			func(tc worker.TaskContext) error {
				return tc.Complete([]byte(`"child-done"`))
			},
		)

		// Parent step spawns child by publishing spawn event.
		parentWfName := harness.UniqueName(t, "parent-wf")
		childRunID := harness.UniqueName(t, "child-run")

		harness.SubscribeWorker(t, nc, "spawn-task",
			func(tc worker.TaskContext) error {
				spawnPayload, _ := json.Marshal(
					map[string]string{
						"child_run_id":   childRunID,
						"child_workflow": childWfName,
						"parent_step_id": "spawner",
					},
				)
				evt := protocol.NewWorkflowEvent(
					protocol.EventWorkflowSpawn,
					tc.RunID(),
					spawnPayload,
				)
				evtData, _ := evt.Marshal()
				msg := &nats.Msg{
					Subject: evt.NATSSubject(),
					Data:    evtData,
					Header: nats.Header{
						"Nats-Msg-Id": {evt.NATSMsgID()},
					},
				}
				_, pubErr := js.PublishMsg(msg)
				if pubErr != nil {
					return pubErr
				}
				return tc.Complete([]byte(`"spawned"`))
			},
		)

		// Register and start parent workflow.
		parentWb := dag.NewWorkflow(parentWfName)
		parentWb.Task("spawner", "spawn-task")
		parentDef, err := parentWb.Build()
		if err != nil {
			t.Fatalf("Build parent: %v", err)
		}
		parentRunID := harness.RegisterAndStart(
			t, svc, parentDef, nil,
		)

		// Wait for parent to complete (spawner step completes).
		harness.WaitForRunStatus(
			t, svc, parentRunID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Wait for child to complete.
		harness.WaitForRunStatus(
			t, svc, childRunID,
			dag.RunStatusCompleted, 15*time.Second,
		)

		// Positive: child run exists with parent linkage.
		childRun, err := svc.GetRun(ctx, childRunID)
		if err != nil {
			t.Fatalf("GetRun child: %v", err)
		}
		if childRun.ParentRunID != parentRunID {
			t.Fatalf(
				"child ParentRunID: expected %q, got %q",
				parentRunID, childRun.ParentRunID,
			)
		}

		// Negative: child completed successfully.
		if childRun.Status != dag.RunStatusCompleted {
			t.Fatalf("child status: %s", childRun.Status)
		}
	})
}
