// engine/orchestrator_test.go
// Tests for the orchestrator core loop: consuming history events, resolving
// ready steps, and publishing task messages. Uses real embedded NATS server.
// Methodology: publish events to history stream, let orchestrator process them,
// then verify tasks appear on the correct subjects and KV state is updated.
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestOrchestratorStartsFirstStep(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	wfDef := dag.WorkflowDef{Name: "test-wf", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		{ID: "b", Task: "task-b", DependsOn: []string{"a"}, Type: dag.StepTypeNormal},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
	orch.Start()
	defer orch.Stop()

	evt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-1", defData)
	evtData, _ := evt.Marshal()
	js.Publish(evt.NATSSubject(), evtData, nats.MsgId(evt.NATSMsgID()))

	// task-a should be enqueued
	sub, err := js.PullSubscribe("task.task-a.*", "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe failed: %v", err)
	}
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task failed (timeout?): %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task message, got %d", len(msgs))
	}

	// task-b should NOT be enqueued yet
	subB, _ := js.PullSubscribe("task.task-b.*", "", nats.BindStream("TASK_QUEUES"))
	msgsB, _ := subB.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if len(msgsB) > 0 {
		t.Fatal("task-b should not be enqueued before task-a completes")
	}
}

func TestOrchestratorAdvancesAfterStepCompleted(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{Name: "test-wf", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		{ID: "b", Task: "task-b", DependsOn: []string{"a"}, Type: dag.StepTypeNormal},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-2", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

	subA, _ := js.PullSubscribe("task.task-a.*", "", nats.BindStream("TASK_QUEUES"))
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-a failed: %v", err)
	}
	msgsA[0].Ack()

	compEvt := protocol.NewStepEvent(protocol.EventStepCompleted, "run-2", "a", []byte(`"done"`))
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData, nats.MsgId(compEvt.NATSMsgID()))

	subB, _ := js.PullSubscribe("task.task-b.*", "", nats.BindStream("TASK_QUEUES"))
	msgsB, err := subB.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-b failed (timeout?): %v", err)
	}
	if len(msgsB) != 1 {
		t.Fatalf("expected 1 task-b message, got %d", len(msgsB))
	}
}

func TestOrchestratorCompletesWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{Name: "single-step", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopLogger(), observe.NewNoopMetrics())
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-3", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

	time.Sleep(200 * time.Millisecond)
	compEvt := protocol.NewStepEvent(protocol.EventStepCompleted, "run-3", "a", []byte(`"done"`))
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData, nats.MsgId(compEvt.NATSMsgID()))

	time.Sleep(500 * time.Millisecond)
	store := NewSnapshotStore(js)
	run, err := store.Load("run-3")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("Status = %v, want Completed", run.Status)
	}
}
