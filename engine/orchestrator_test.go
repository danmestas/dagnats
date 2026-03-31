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

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
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

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
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

func TestOrchestratorEnforcesMaxIterations(t *testing.T) {
	// Methodology: red-green TDD. Workflow has a single agent-loop step with
	// MaxIterations=2. After the 2nd step.continue event the orchestrator must
	// mark the run Failed and must NOT publish a 3rd task message.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{Name: "loop-wf", Version: "1", Steps: []dag.StepDef{
		{
			ID:   "loop-step",
			Task: "agent-task",
			Type: dag.StepTypeAgentLoop,
			Loop: &dag.AgentLoopConfig{MaxIterations: 2},
		},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start the workflow — iteration 0 task should be published.
	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-iter", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

	taskSub, err := js.PullSubscribe("task.agent-task.*", "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe failed: %v", err)
	}
	msgs, err := taskSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch initial task failed: %v", err)
	}
	msgs[0].Ack()

	// First step.continue — iteration becomes 1, still within MaxIterations=2.
	cont1 := protocol.NewStepEvent(protocol.EventStepContinue, "run-iter", "loop-step", nil)
	cont1Data, _ := cont1.Marshal()
	js.Publish(cont1.NATSSubject(), cont1Data, nats.MsgId(cont1.NATSMsgID()))

	msgs2, err := taskSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch iteration-1 task failed: %v", err)
	}
	msgs2[0].Ack()

	// Second step.continue — iteration becomes 2, equals MaxIterations → must fail.
	cont2 := protocol.NewStepEvent(
		protocol.EventStepContinue, "run-iter", "loop-step",
		[]byte(`"continue"`),
	)
	// Use a distinct MsgId so JetStream dedup doesn't drop it.
	cont2Data, _ := cont2.Marshal()
	js.Publish(cont2.NATSSubject(), cont2Data,
		nats.MsgId("run-iter.loop-step.step.continue.2"))

	// Give the orchestrator time to process.
	time.Sleep(500 * time.Millisecond)

	// Run must be marked Failed.
	store := NewSnapshotStore(js)
	run, err := store.Load("run-iter")
	if err != nil {
		t.Fatalf("Load run failed: %v", err)
	}
	if run.Status != dag.RunStatusFailed {
		t.Fatalf("run.Status = %v, want Failed", run.Status)
	}

	// No 3rd task message should have been published.
	msgs3, _ := taskSub.Fetch(1, nats.MaxWait(300*time.Millisecond))
	if len(msgs3) > 0 {
		t.Fatal("expected no 3rd task message after MaxIterations exceeded")
	}
}

func TestOrchestratorEnforcesMaxDuration(t *testing.T) {
	// Methodology: red-green TDD. Workflow has a single agent-loop step with
	// MaxDuration=1ms. After starting the loop and sleeping, the next
	// step.continue must fail the run instead of re-enqueuing.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{Name: "dur-wf", Version: "1", Steps: []dag.StepDef{
		{
			ID:   "dur-step",
			Task: "dur-task",
			Type: dag.StepTypeAgentLoop,
			Loop: &dag.AgentLoopConfig{MaxIterations: 100, MaxDuration: 1 * time.Millisecond},
		},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-dur", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

	taskSub, err := js.PullSubscribe("task.dur-task.*", "", nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe failed: %v", err)
	}
	msgs, err := taskSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch initial task failed: %v", err)
	}
	msgs[0].Ack()

	// Send first continue to set LoopStartedAt, then sleep past MaxDuration.
	cont1 := protocol.NewStepEvent(protocol.EventStepContinue, "run-dur", "dur-step", nil)
	cont1Data, _ := cont1.Marshal()
	js.Publish(cont1.NATSSubject(), cont1Data, nats.MsgId(cont1.NATSMsgID()))

	msgs2, err := taskSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch iteration-1 task failed: %v", err)
	}
	msgs2[0].Ack()

	// Sleep well past the 1ms MaxDuration.
	time.Sleep(50 * time.Millisecond)

	// Second continue should trip MaxDuration.
	cont2 := protocol.NewStepEvent(protocol.EventStepContinue, "run-dur", "dur-step", nil)
	cont2Data, _ := cont2.Marshal()
	js.Publish(cont2.NATSSubject(), cont2Data,
		nats.MsgId("run-dur.dur-step.step.continue.2"))

	time.Sleep(500 * time.Millisecond)

	store := NewSnapshotStore(js)
	run, err := store.Load("run-dur")
	if err != nil {
		t.Fatalf("Load run failed: %v", err)
	}
	if run.Status != dag.RunStatusFailed {
		t.Fatalf("run.Status = %v, want Failed (MaxDuration exceeded)", run.Status)
	}

	// No further task messages should have been published.
	msgs3, _ := taskSub.Fetch(1, nats.MaxWait(300*time.Millisecond))
	if len(msgs3) > 0 {
		t.Fatal("expected no task message after MaxDuration exceeded")
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

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
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

func TestOrchestratorRoutesAgentStepsToCustomStream(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)

	err := natsutil.SetupAll(nc,
		natsutil.WithStreams(natsutil.StreamConfig{
			Name:     "AGENT_TASKS",
			Subjects: []string{"agent.task.>"},
		}),
	)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	// Register workflow with one agent step
	wfDef := dag.WorkflowDef{
		Name:    "routed-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "agent-step", Task: "llm-task",
				Type:     dag.StepTypeAgent,
				Metadata: map[string]string{"role": "coder"},
			},
		},
	}
	defData, _ := json.Marshal(wfDef)
	if _, err := defKV.Put("routed-wf", defData); err != nil {
		t.Fatalf("put def: %v", err)
	}

	// Subscribe to AGENT_TASKS to verify routing
	agentSub, err := js.SubscribeSync("agent.task.>",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe agent tasks: %v", err)
	}

	routes := map[dag.StepType]string{
		dag.StepTypeAgent: "agent.task",
	}
	orch := NewOrchestrator(nc, observe.NewNoopTelemetry(),
		WithStepRoutes(routes))
	orch.Start()
	defer orch.Stop()

	// Publish workflow.started event
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-route-1", defData)
	data, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), data,
		nats.MsgId(startEvt.NATSMsgID()))

	// Agent task should arrive on AGENT_TASKS, not TASK_QUEUES
	agentMsg, err := agentSub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("expected message on AGENT_TASKS: %v", err)
	}
	if agentMsg == nil {
		t.Fatalf("agent message should not be nil")
	}
	agentMsg.Ack()
}
