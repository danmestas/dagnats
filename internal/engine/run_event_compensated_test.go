// internal/engine/run_event_compensated_test.go
// e2e test for the event.run.* consumer contract's compensated outcome
// (#625 review round 2: "add an e2e for the compensated path — one of
// the two outcomes this PR says were previously un-notified").
// Methodology: real embedded NATS/JetStream, white-box package engine
// (mirrors TestOrchestratorCompensationChain's low-level event-driving
// style since compensation has no dagnatstest.Harness helper). Drives a
// step failure through to a completed compensation step, then asserts
// the resulting event.run.* carries type=run.failed (compensation
// outcomes bucket to Failed) and status=compensated (the precise
// dag.RunStatus).
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestOrchestrator_CompensatedRun_PublishesRunFailedEventWithCompensatedStatus(
	t *testing.T,
) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name:    "comp-evt-test",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "comp-evt-task-a", Type: dag.StepTypeNormal,
				Compensate: "undo-a"},
			{ID: "b", Task: "comp-evt-task-b", DependsOn: []string{"a"},
				Type:  dag.StepTypeNormal,
				Retry: &dag.RetryPolicy{MaxAttempts: 1}},
			{ID: "undo-a", Task: "comp-evt-task-undo-a",
				Type: dag.StepTypeNormal},
		},
		AuxSteps: map[string]bool{"undo-a": true},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	runID := "comp-evt-run-1"

	// Subscribe BEFORE driving the run, so the terminal event cannot
	// race the subscription.
	sub, err := js.SubscribeSync(
		runEventSubject(wfDef.Name, runID, "compensated"),
		nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, defData,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal start event: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	subA, _ := js.PullSubscribe(
		"task.comp-evt-task-a.*", "", nats.BindStream("TASK_QUEUES"),
	)
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil || len(msgsA) != 1 {
		t.Fatalf("expected task-a: err=%v len=%d", err, len(msgsA))
	}
	msgsA[0].Ack()
	completeA := protocol.NewWorkflowEvent(
		protocol.EventStepCompleted, runID, []byte(`{"result":"ok"}`),
	)
	completeA.StepID = "a"
	completeAData, err := completeA.Marshal()
	if err != nil {
		t.Fatalf("marshal step a completed: %v", err)
	}
	mustPublish(t, js, completeA.NATSSubject(), completeAData,
		nats.MsgId(completeA.NATSMsgID()))

	subB, _ := js.PullSubscribe(
		"task.comp-evt-task-b.*", "", nats.BindStream("TASK_QUEUES"),
	)
	msgsB, err := subB.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil || len(msgsB) != 1 {
		t.Fatalf("expected task-b: err=%v len=%d", err, len(msgsB))
	}
	msgsB[0].Ack()
	failB := protocol.NewWorkflowEvent(
		protocol.EventStepFailed, runID,
		[]byte(`{"error":"boom","failure_type":"non_retriable"}`),
	)
	failB.StepID = "b"
	failBData, err := failB.Marshal()
	if err != nil {
		t.Fatalf("marshal step b failed: %v", err)
	}
	mustPublish(t, js, failB.NATSSubject(), failBData,
		nats.MsgId(failB.NATSMsgID()))

	subUndo, _ := js.PullSubscribe(
		"task.comp-evt-task-undo-a.*", "", nats.BindStream("TASK_QUEUES"),
	)
	msgsUndo, err := subUndo.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil || len(msgsUndo) != 1 {
		t.Fatalf("expected undo-a: err=%v len=%d", err, len(msgsUndo))
	}
	msgsUndo[0].Ack()
	completeUndo := protocol.NewWorkflowEvent(
		protocol.EventStepCompleted, runID, []byte(`{"result":"undone"}`),
	)
	completeUndo.StepID = "undo-a"
	completeUndoData, err := completeUndo.Marshal()
	if err != nil {
		t.Fatalf("marshal undo-a completed: %v", err)
	}
	mustPublish(t, js, completeUndo.NATSSubject(), completeUndoData,
		nats.MsgId(completeUndo.NATSMsgID()))

	// Positive: the run reaches Compensated in the persisted snapshot.
	deadline := time.Now().Add(5 * time.Second)
	var finalRun dag.WorkflowRun
	for time.Now().Before(deadline) {
		run, loadErr := orch.store.Load(t.Context(), runID)
		if loadErr == nil && run.Status == dag.RunStatusCompensated {
			finalRun = run
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if finalRun.RunID != runID {
		t.Fatalf("run %q did not reach Compensated within deadline", runID)
	}

	// Positive: the event.run.* notification carries type=run.failed
	// (compensation buckets to Failed) with the precise status.
	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("expected event.run.*.compensated message: %v", err)
	}
	var evt protocol.RunEvent
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal RunEvent: %v", err)
	}
	if evt.Type != protocol.RunEventFailed {
		t.Fatalf("Type = %q, want %q (compensation buckets to Failed)",
			evt.Type, protocol.RunEventFailed)
	}
	if evt.Status != "compensated" {
		t.Fatalf("Status = %q, want %q", evt.Status, "compensated")
	}
	if evt.RunID != runID {
		t.Fatalf("RunID = %q, want %q", evt.RunID, runID)
	}
}
