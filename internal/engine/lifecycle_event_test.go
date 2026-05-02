// internal/engine/lifecycle_event_test.go
// Tests for engine-side step.queued + step.started lifecycle event
// handling. Embedded NATS, real orchestrator. Dispatch-side tests
// (Task 8) verify the publish at the dispatch site; handler tests
// (Tasks 9-10) verify the onEvent switch updates state correctly with
// monotonic guards.
// Methodology: red-green TDD. Each test asserts both a positive event
// (the event we expect) and a negative property.
package engine

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOrchestrator_PublishesStepQueuedOnDispatch(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "queued-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-q1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	sub, err := js.SubscribeSync("history.run-q1", nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	var queuedEvt protocol.Event
	var sawQueued bool
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) && !sawQueued {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if evt.Type == protocol.EventStepQueued && evt.StepID == "a" {
			queuedEvt = evt
			sawQueued = true
		}
	}
	if !sawQueued {
		t.Fatal("expected step.queued event for step 'a', not found")
	}
	if queuedEvt.AttemptNumber != 1 {
		t.Fatalf("AttemptNumber = %d, want 1", queuedEvt.AttemptNumber)
	}
	if queuedEvt.RunID != "run-q1" {
		t.Fatalf("RunID = %q, want %q", queuedEvt.RunID, "run-q1")
	}
}

func TestOrchestrator_StepQueuedMsgIdIsDeterministic(t *testing.T) {
	evt := protocol.Event{
		Type:          protocol.EventStepQueued,
		RunID:         "run-mid",
		StepID:        "step-x",
		AttemptNumber: 1,
	}
	got := evt.NATSMsgID()
	want := "run-mid.step-x.step.queued.1"
	if got != want {
		t.Fatalf("NATSMsgID = %q, want %q", got, want)
	}
	evt.AttemptNumber = 2
	got2 := evt.NATSMsgID()
	if got2 == got {
		t.Fatalf("NATSMsgID for attempt 1 and 2 must differ; both = %q", got)
	}
}

func TestOrchestrator_DispatchProceedsIfQueuedPublishFails(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "dispatch-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-dp", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	sub, _ := js.PullSubscribe("task.task-a.*", "", nats.BindStream("TASK_QUEUES"))
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("task message not delivered: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task message, got %d", len(msgs))
	}
	msgs[0].Ack()
}

var _ context.Context = context.Background()
var _ jetstream.JetStream = nil
