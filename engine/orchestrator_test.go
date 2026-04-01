// engine/orchestrator_test.go
// Tests for the orchestrator core loop: consuming history events, resolving
// ready steps, and publishing task messages. Uses real embedded NATS server.
// Methodology: publish events to history stream, let orchestrator process them,
// then verify tasks appear on the correct subjects and KV state is updated.
package engine

import (
	"encoding/json"
	"fmt"
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

func TestOrchestratorHandlesWorkflowSpawn(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)

	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	// Register a child workflow definition
	childDef := dag.WorkflowDef{
		Name:    "child-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "child-step", Task: "child-task",
				Type: dag.StepTypeNormal},
		},
	}
	childDefData, _ := json.Marshal(childDef)
	if _, err := defKV.Put("child-wf", childDefData); err != nil {
		t.Fatalf("put child def: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Publish spawn event
	spawnPayload, _ := json.Marshal(map[string]string{
		"child_run_id":   "child-run-1",
		"child_workflow": "child-wf",
		"parent_step_id": "parent-step-a",
	})
	spawnEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "parent-run-1", spawnPayload)
	data, _ := spawnEvt.Marshal()
	js.Publish(spawnEvt.NATSSubject(), data,
		nats.MsgId(spawnEvt.NATSMsgID()))

	// Wait for child run to appear in snapshot store
	store := NewSnapshotStore(js)
	var childRun dag.WorkflowRun
	var loadErr error
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		childRun, loadErr = store.Load("child-run-1")
		if loadErr == nil {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if loadErr != nil {
		t.Fatalf("child run should exist: %v", loadErr)
	}
	if childRun.ParentRunID != "parent-run-1" {
		t.Fatalf("ParentRunID = %q, want parent-run-1",
			childRun.ParentRunID)
	}
	if childRun.ParentStepID != "parent-step-a" {
		t.Fatalf("ParentStepID = %q, want parent-step-a",
			childRun.ParentStepID)
	}
}

func TestOrchestratorChildCompletionNotifiesParent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)

	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	// Register a single-step child workflow
	childDef := dag.WorkflowDef{
		Name: "notify-child", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "echo-task", Type: dag.StepTypeNormal},
		},
	}
	childDefData, _ := json.Marshal(childDef)
	defKV.Put("notify-child", childDefData)

	// Subscribe to parent's history for child.completed
	parentSub, err := js.SubscribeSync("history.parent-run-2",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe parent history: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Spawn a child workflow
	spawnPayload, _ := json.Marshal(map[string]string{
		"child_run_id":   "child-run-2",
		"child_workflow": "notify-child",
		"parent_step_id": "parent-step-b",
	})
	spawnEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "parent-run-2", spawnPayload)
	data, _ := spawnEvt.Marshal()
	js.Publish(spawnEvt.NATSSubject(), data,
		nats.MsgId(spawnEvt.NATSMsgID()))

	// Wait for child task to appear, then simulate completion
	time.Sleep(300 * time.Millisecond)
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted,
		"child-run-2", "s1", []byte(`"child-result"`))
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Look for workflow.child.completed on parent's history
	deadline := time.Now().Add(5 * time.Second)
	found := false
	for time.Now().Before(deadline) {
		m, err := parentSub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		m.Ack()
		evt, _ := protocol.UnmarshalEvent(m.Data)
		if evt.Type == protocol.EventWorkflowChildCompleted {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected workflow.child.completed on parent history")
	}
}

func TestOrchestratorRejectsExcessiveNesting(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)

	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")
	store := NewSnapshotStore(js)

	// Register child workflow def
	childDef := dag.WorkflowDef{
		Name: "deep-child", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t", Type: dag.StepTypeNormal},
		},
	}
	childDefData, _ := json.Marshal(childDef)
	defKV.Put("deep-child", childDefData)

	// Create a chain: run-0 -> run-1 -> run-2 (depth 3)
	for i := 0; i < 3; i++ {
		run := dag.WorkflowRun{
			RunID: fmt.Sprintf("run-%d", i), WorkflowID: "deep-child",
			Status:    dag.RunStatusRunning,
			Steps:     map[string]dag.StepState{"s1": {Status: dag.StepStatusRunning}},
			CreatedAt: time.Now(),
		}
		if i > 0 {
			run.ParentRunID = fmt.Sprintf("run-%d", i-1)
			run.ParentStepID = "s1"
		}
		store.Save(run)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Try to spawn from run-2 (depth would be 4, exceeds 3)
	spawnPayload, _ := json.Marshal(map[string]string{
		"child_run_id":   "run-3",
		"child_workflow": "deep-child",
		"parent_step_id": "s1",
	})
	spawnEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "run-2", spawnPayload)
	data, _ := spawnEvt.Marshal()
	js.Publish(spawnEvt.NATSSubject(), data,
		nats.MsgId(spawnEvt.NATSMsgID()))

	// Poll briefly — run-3 should never be created
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := store.Load("run-3"); err == nil {
			t.Fatalf("run-3 should not exist — nesting too deep")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestOrchestratorCancelsRunningWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name: "cancel-test", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "slow-task", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("cancel-test", defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cancel-run-1", defData)
	data, _ := startEvt.Marshal()
	msg := &nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	}
	js.PublishMsg(msg)
	time.Sleep(200 * time.Millisecond)

	// Cancel the workflow
	cancelEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, "cancel-run-1", nil)
	cancelData, _ := cancelEvt.Marshal()
	cancelMsg := &nats.Msg{
		Subject: cancelEvt.NATSSubject(),
		Data:    cancelData,
		Header:  nats.Header{"Nats-Msg-Id": {cancelEvt.NATSMsgID()}},
	}
	js.PublishMsg(cancelMsg)

	// Wait for processing
	store := NewSnapshotStore(js)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load("cancel-run-1")
		if err == nil && run.Status == dag.RunStatusCancelled {
			// Positive: run is cancelled
			// Positive: step is cancelled
			s1 := run.Steps["s1"]
			if s1.Status != dag.StepStatusCancelled {
				t.Fatalf("step status = %v, want Cancelled",
					s1.Status)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should be cancelled within 3s")
}

func TestOrchestratorRetriesWithPolicy(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name: "retry-test", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  3,
			Strategy:     dag.RetryFixed,
			InitialDelay: 100 * time.Millisecond,
			MaxDelay:     1 * time.Second,
		},
		Steps: []dag.StepDef{
			{ID: "s1", Task: "flaky-task", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("retry-test", defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "retry-run-1", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	time.Sleep(200 * time.Millisecond)

	// First failure — should not be permanently failed
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "retry-run-1", "s1",
		[]byte(`"transient error"`))
	failData, _ := failEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: failEvt.NATSSubject(), Data: failData,
		Header: nats.Header{"Nats-Msg-Id": {failEvt.NATSMsgID()}},
	})
	time.Sleep(200 * time.Millisecond)

	store := NewSnapshotStore(js)
	run, _ := store.Load("retry-run-1")

	// Positive: run is still running (not failed yet)
	if run.Status != dag.RunStatusRunning {
		t.Fatalf("status = %v after 1 failure, want Running",
			run.Status)
	}

	// Positive: step has 1 attempt recorded
	if run.Steps["s1"].Attempts != 1 {
		t.Fatalf("attempts = %d, want 1",
			run.Steps["s1"].Attempts)
	}
}

func TestOrchestratorExhaustsRetries(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name: "exhaust-test", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  2,
			Strategy:     dag.RetryFixed,
			InitialDelay: 50 * time.Millisecond,
		},
		Steps: []dag.StepDef{
			{ID: "s1", Task: "bad-task", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("exhaust-test", defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "exhaust-run-1", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	time.Sleep(200 * time.Millisecond)

	// Fail 3 times (> MaxAttempts of 2)
	for i := 0; i < 3; i++ {
		failEvt := protocol.NewStepEvent(
			protocol.EventStepFailed, "exhaust-run-1", "s1",
			[]byte(`"permanent error"`))
		// Unique msg ID per attempt
		msgID := fmt.Sprintf("exhaust-run-1.s1.fail.%d", i)
		failData, _ := failEvt.Marshal()
		js.PublishMsg(&nats.Msg{
			Subject: failEvt.NATSSubject(), Data: failData,
			Header:  nats.Header{"Nats-Msg-Id": {msgID}},
		})
		time.Sleep(100 * time.Millisecond)
	}

	store := NewSnapshotStore(js)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load("exhaust-run-1")
		if err == nil && run.Status == dag.RunStatusFailed {
			// Positive: permanently failed
			if run.Steps["s1"].Status != dag.StepStatusFailed {
				t.Fatalf("step = %v, want Failed",
					run.Steps["s1"].Status)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should be failed after exhausting retries")
}
