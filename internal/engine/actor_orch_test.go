package engine

// Methodology: integration test with real embedded NATS. Verify the
// ActorOrchestrator spawns per-run actors and routes events correctly.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestActorOrchBasicWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	// Register workflow
	wfDef := dag.WorkflowDef{
		Name:    "actor-test",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("actor-test", defData)

	// Start ActorOrchestrator
	orch := NewActorOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Publish workflow.started
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "arun-1", defData)
	data, _ := startEvt.Marshal()
	msg := &nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	}
	js.PublishMsg(msg)

	// Wait for actor to process
	time.Sleep(200 * time.Millisecond)

	// Positive: actor was spawned for this run
	wa := orch.GetWorkflowActor("arun-1")
	if wa == nil {
		t.Fatalf("expected workflow actor for arun-1")
	}

	// Positive: run is in Running state
	if wa.RunStatus() != dag.RunStatusRunning {
		t.Fatalf("status = %v, want Running", wa.RunStatus())
	}
}

func TestActorOrchEnqueuesTasksOnStart(t *testing.T) {
	// Methodology: start a workflow via ActorOrchestrator, verify
	// that a task message appears on the TASK_QUEUES stream.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name:    "actor-enqueue",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("actor-enqueue", defData)

	orch := NewActorOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "enq-run-1", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {startEvt.NATSMsgID()},
		},
	})

	// Positive: task appears on TASK_QUEUES stream
	sub, err := js.PullSubscribe(
		"task.task-a.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("task not enqueued: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs))
	}

	// Positive: task payload has correct run/step IDs
	var payload protocol.TaskPayload
	if err := json.Unmarshal(msgs[0].Data, &payload); err != nil {
		t.Fatalf("unmarshal task payload: %v", err)
	}
	if payload.RunID != "enq-run-1" {
		t.Fatalf("RunID = %q, want enq-run-1", payload.RunID)
	}
	if payload.StepID != "s1" {
		t.Fatalf("StepID = %q, want s1", payload.StepID)
	}
}

func TestActorOrchLinearChainEnqueuesNext(t *testing.T) {
	// Methodology: two-step chain s1 → s2. Start workflow, verify
	// s1 task appears. Complete s1, verify s2 task appears.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name:    "actor-chain",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "s1", Task: "task-a",
				Type: dag.StepTypeNormal,
			},
			{
				ID: "s2", Task: "task-b",
				DependsOn: []string{"s1"},
				Type:      dag.StepTypeNormal,
			},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("actor-chain", defData)

	orch := NewActorOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "chain-run", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {startEvt.NATSMsgID()},
		},
	})

	// Positive: task-a enqueued
	subA, _ := js.PullSubscribe(
		"task.task-a.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("task-a not enqueued: %v", err)
	}
	msgsA[0].Ack()

	// Negative: task-b NOT enqueued yet
	subB, _ := js.PullSubscribe(
		"task.task-b.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	_, err = subB.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if err == nil {
		t.Fatal("task-b should not be enqueued before s1 completes")
	}

	// Complete s1
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "chain-run", "s1",
		[]byte(`"s1-done"`))
	compData, _ := compEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: compEvt.NATSSubject(),
		Data:    compData,
		Header: nats.Header{
			"Nats-Msg-Id": {compEvt.NATSMsgID()},
		},
	})

	// Positive: task-b now enqueued
	msgsB, err := subB.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("task-b not enqueued after s1 completed: %v", err)
	}
	if len(msgsB) != 1 {
		t.Fatalf("expected 1 task-b message, got %d", len(msgsB))
	}

	// Positive: task-b input contains s1 output
	var bPayload protocol.TaskPayload
	json.Unmarshal(msgsB[0].Data, &bPayload)
	if string(bPayload.Input) != `"s1-done"` {
		t.Fatalf(
			"task-b input = %q, want s1-done",
			string(bPayload.Input),
		)
	}
}

func TestActorOrchSurvivesMalformedEvent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name:    "ao-recover",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "ta", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("ao-recover", defData)

	orch := NewActorOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Publish garbage data.
	js.Publish("history.ao-garbage",
		[]byte("invalid json garbage"),
		nats.MsgId("ao-garbage-1"))
	time.Sleep(200 * time.Millisecond)

	// Positive: subsequent valid event still works.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "ao-after", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header: nats.Header{
			"Nats-Msg-Id": {startEvt.NATSMsgID()},
		},
	})
	time.Sleep(300 * time.Millisecond)

	wa := orch.GetWorkflowActor("ao-after")
	// Positive: actor was spawned for valid event.
	if wa == nil {
		t.Fatal("actor should exist after recovery")
	}
	if wa.RunStatus() != dag.RunStatusRunning {
		t.Fatalf("status = %v, want Running", wa.RunStatus())
	}
}

func TestActorOrchIgnoresUnhandledEvent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()

	orch := NewActorOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Publish an unhandled event type.
	evt := protocol.Event{
		Type:  "custom.unknown",
		RunID: "ao-unk-1",
	}
	data, _ := json.Marshal(evt)
	msg := &nats.Msg{
		Subject: "history.ao-unk-1",
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {"ao-unk-1.custom"}},
	}
	js.PublishMsg(msg)
	time.Sleep(200 * time.Millisecond)

	// Positive: no actor spawned.
	wa := orch.GetWorkflowActor("ao-unk-1")
	if wa != nil {
		t.Fatal("no actor should be spawned for unhandled event")
	}
}

func TestActorOrchHandlesStepFailed(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name:    "actor-fail",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("actor-fail", defData)

	orch := NewActorOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "arun-fail", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	time.Sleep(200 * time.Millisecond)

	// Fail step s1.
	failEvt := protocol.NewWorkflowEvent(
		protocol.EventStepFailed, "arun-fail",
		[]byte(`"actor error"`))
	failEvt.StepID = "s1"
	fData, _ := failEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: failEvt.NATSSubject(),
		Data:    fData,
		Header:  nats.Header{"Nats-Msg-Id": {failEvt.NATSMsgID()}},
	})

	deadline := time.Now().Add(3 * time.Second)
	wa := orch.GetWorkflowActor("arun-fail")
	for time.Now().Before(deadline) {
		if wa != nil && wa.RunStatus() == dag.RunStatusFailed {
			break
		}
		wa = orch.GetWorkflowActor("arun-fail")
		time.Sleep(50 * time.Millisecond)
	}

	if wa == nil {
		t.Fatal("expected workflow actor for arun-fail")
	}
	// Positive: workflow is Failed.
	if wa.RunStatus() != dag.RunStatusFailed {
		t.Fatalf("status = %v, want Failed", wa.RunStatus())
	}
	// Positive: step has error.
	state := wa.StepState("s1")
	if state.Status != dag.StepStatusFailed {
		t.Fatalf("step status = %v, want Failed", state.Status)
	}
}

func TestActorOrchRoutesCompletionToActor(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name:    "actor-test-2",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("actor-test-2", defData)

	orch := NewActorOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "arun-2", defData)
	data, _ := startEvt.Marshal()
	msg := &nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	}
	js.PublishMsg(msg)
	time.Sleep(200 * time.Millisecond)

	// Complete step s1
	completeEvt := protocol.NewWorkflowEvent(
		protocol.EventStepCompleted, "arun-2", []byte(`"result"`))
	completeEvt.StepID = "s1"
	data2, _ := completeEvt.Marshal()
	msg2 := &nats.Msg{
		Subject: completeEvt.NATSSubject(),
		Data:    data2,
		Header:  nats.Header{"Nats-Msg-Id": {completeEvt.NATSMsgID()}},
	}
	js.PublishMsg(msg2)

	// Wait for completion
	deadline := time.Now().Add(3 * time.Second)
	wa := orch.GetWorkflowActor("arun-2")
	for time.Now().Before(deadline) {
		if wa != nil && wa.RunStatus() == dag.RunStatusCompleted {
			break
		}
		wa = orch.GetWorkflowActor("arun-2")
		time.Sleep(50 * time.Millisecond)
	}

	if wa == nil {
		t.Fatalf("expected workflow actor for arun-2")
	}

	// Positive: workflow completed
	if wa.RunStatus() != dag.RunStatusCompleted {
		t.Fatalf("status = %v, want Completed", wa.RunStatus())
	}
}
