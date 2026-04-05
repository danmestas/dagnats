// internal/engine/cancel_on_test.go
// Tests for event-based cancellation: workflow registers CancelOn,
// matching event arrives, workflow is cancelled.
// Methodology: register a workflow with CancelOn, publish matching/
// non-matching events, verify the run transitions to cancelled or
// stays running. Uses real embedded NATS server.
package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCancelOnEventCancelsWorkflow(t *testing.T) {
	// Methodology: start a workflow with CancelOn configured.
	// Publish a matching event. Verify run status becomes cancelled.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "cancel-on-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "a",
				Task: "slow-task",
				Type: dag.StepTypeNormal,
			},
		},
		CancelOn: []dag.CancelOn{
			{
				Event: "task.completed",
				Match: dag.Match{
					Left:  "task_id",
					Op:    dag.MatchOpEq,
					Right: "input.task_id",
				},
			},
		},
	}

	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow with input containing task_id.
	startPayload, _ := json.Marshal(struct {
		WorkflowDef json.RawMessage `json:"workflow_def"`
		Input       json.RawMessage `json:"input"`
	}{
		WorkflowDef: defData,
		Input:       []byte(`{"task_id":"task-123"}`),
	})
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		"run-cancel-on", startPayload,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)

	// Positive: verify running before cancel event.
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "run-cancel-on")
	if err != nil {
		t.Fatalf("Load run: %v", err)
	}
	if run.Status != dag.RunStatusRunning {
		t.Fatalf(
			"status = %s, want running", run.Status,
		)
	}

	// Publish matching event to EVENTS stream.
	js.Publish(
		"event.task.completed",
		[]byte(`{"task_id":"task-123","result":"done"}`),
	)

	// Wait for cancellation to propagate.
	time.Sleep(1 * time.Second)

	// Positive: run should be cancelled.
	run2, err := store.Load(context.Background(), "run-cancel-on")
	if err != nil {
		t.Fatalf("Load run after cancel: %v", err)
	}
	if run2.Status != dag.RunStatusCancelled {
		t.Fatalf(
			"status = %s, want cancelled", run2.Status,
		)
	}
}

func TestCancelOnNoMatchDoesNotCancel(t *testing.T) {
	// Methodology: start a workflow with CancelOn configured.
	// Publish a non-matching event. Verify run stays running.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "cancel-nomatch-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "a",
				Task: "slow-task",
				Type: dag.StepTypeNormal,
			},
		},
		CancelOn: []dag.CancelOn{
			{
				Event: "task.completed",
				Match: dag.Match{
					Left:  "task_id",
					Op:    dag.MatchOpEq,
					Right: "input.task_id",
				},
			},
		},
	}

	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	startPayload, _ := json.Marshal(struct {
		WorkflowDef json.RawMessage `json:"workflow_def"`
		Input       json.RawMessage `json:"input"`
	}{
		WorkflowDef: defData,
		Input:       []byte(`{"task_id":"task-123"}`),
	})
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		"run-nomatch", startPayload,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)

	// Non-matching event (different task_id).
	js.Publish(
		"event.task.completed",
		[]byte(`{"task_id":"task-999"}`),
	)

	time.Sleep(500 * time.Millisecond)

	// Positive: should still be running.
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "run-nomatch")
	if err != nil {
		t.Fatalf("Load run: %v", err)
	}
	if run.Status != dag.RunStatusRunning {
		t.Fatalf(
			"status = %s, want running", run.Status,
		)
	}

	// Negative: should NOT be cancelled.
	if run.Status == dag.RunStatusCancelled {
		t.Fatal("run should not be cancelled")
	}
}
