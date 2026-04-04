// engine/orchestrator_test.go
// Tests for the orchestrator core loop: consuming history events, resolving
// ready steps, and publishing task messages. Uses real embedded NATS server.
// Methodology: publish events to history stream, let orchestrator process them,
// then verify tasks appear on the correct subjects and KV state is updated.
package engine

import (
	"encoding/json"
	"fmt"
	"strings"
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
			Header: nats.Header{"Nats-Msg-Id": {msgID}},
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

func TestOrchestratorWorkflowTimeout(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name:    "timeout-test",
		Version: "1",
		Timeout: 200 * time.Millisecond,
		Steps: []dag.StepDef{
			{ID: "slow", Task: "slow-task", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("timeout-test", defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "timeout-run-1", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	time.Sleep(100 * time.Millisecond)

	// Wait for timeout to expire
	time.Sleep(200 * time.Millisecond)

	// Send a step event after timeout (should trigger cancel)
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "timeout-run-1", "slow",
		[]byte(`"timed out"`))
	failData, _ := failEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: failEvt.NATSSubject(), Data: failData,
		Header: nats.Header{"Nats-Msg-Id": {failEvt.NATSMsgID()}},
	})

	// Check that run is cancelled
	store := NewSnapshotStore(js)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load("timeout-run-1")
		if err == nil && run.Status == dag.RunStatusCancelled {
			return // Positive: timed out → cancelled
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should be cancelled after timeout")
}

func TestOrchestratorPublishesDeadLetter(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name:    "dlq-test",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "bad-task", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("dlq-test", defData)

	// Subscribe to DLQ
	dlqSub, err := js.SubscribeSync("dead.>",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe DLQ: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "dlq-run-1", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	time.Sleep(200 * time.Millisecond)

	// Fail the step permanently (no retries configured)
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "dlq-run-1", "s1",
		[]byte(`"permanent error"`))
	failData, _ := failEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: failEvt.NATSSubject(), Data: failData,
		Header: nats.Header{"Nats-Msg-Id": {failEvt.NATSMsgID()}},
	})

	// Positive: DLQ message appears
	dlqMsg, err := dlqSub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected DLQ message: %v", err)
	}
	dlqMsg.Ack()

	// Positive: subject contains task name
	if !strings.HasPrefix(dlqMsg.Subject, "dead.bad-task.") {
		t.Fatalf("DLQ subject = %q, want prefix dead.bad-task.",
			dlqMsg.Subject)
	}
}

func TestOrchestratorOnFailureStep(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	// Workflow: deploy fails → notify runs
	wfDef := dag.WorkflowDef{
		Name:    "onfail-test",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:        "deploy",
				Task:      "deploy-task",
				Type:      dag.StepTypeNormal,
				OnFailure: "notify",
			},
			{
				ID:   "notify",
				Task: "notify-task",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defData, _ := json.Marshal(wfDef)
	defKV.Put("onfail-test", defData)

	// Subscribe to task queue for notify
	taskSub, _ := js.SubscribeSync("task.notify-task.>",
		nats.AckExplicit(), nats.DeliverAll())

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "onfail-run-1", defData)
	data, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(), Data: data,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	time.Sleep(200 * time.Millisecond)

	// Fail deploy step permanently
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "onfail-run-1", "deploy",
		[]byte(`"deploy crashed"`))
	failData, _ := failEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: failEvt.NATSSubject(), Data: failData,
		Header: nats.Header{"Nats-Msg-Id": {failEvt.NATSMsgID()}},
	})

	// Positive: notify task should be enqueued
	msg, err := taskSub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected notify task to be enqueued: %v", err)
	}
	msg.Ack()

	// Positive: workflow should NOT be failed yet (on-failure is running)
	store := NewSnapshotStore(js)
	time.Sleep(200 * time.Millisecond)
	run, _ := store.Load("onfail-run-1")
	if run.Status == dag.RunStatusFailed {
		t.Fatalf("workflow should not be failed while on-failure step pending")
	}
}

func TestOrchestratorWorkerGroupRouting(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	// Define workflow with a step targeting worker group "gpu"
	wfDef := dag.WorkflowDef{
		Name:    "gpu-workflow",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:          "train",
				Task:        "ml-training",
				Type:        dag.StepTypeNormal,
				WorkerGroup: "gpu",
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start the workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "gpu-run-1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

	// Positive: task should appear on gpu-specific subject
	gpuSub, err := js.PullSubscribe(
		"task.ml-training.gpu.*", "", nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe gpu subject failed: %v", err)
	}
	msgs, err := gpuSub.Fetch(1, nats.MaxWait(3*time.Second))
	if err != nil {
		t.Fatalf("task did not arrive on gpu subject: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task on gpu subject, got %d", len(msgs))
	}

	// Negative: task should NOT appear on non-group subject
	generalSub, _ := js.PullSubscribe(
		"task.ml-training.gpu-run-1", "", nats.BindStream("TASK_QUEUES"),
	)
	generalMsgs, _ := generalSub.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if len(generalMsgs) > 0 {
		t.Fatal("task should not appear on non-group subject when group is set")
	}
}

func TestOrchestratorStepContinuePublishesTask(t *testing.T) {
	// Methodology: agent-loop step with MaxIterations=3. After
	// step.continue, verify new task and iteration count.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	wfDef := dag.WorkflowDef{
		Name: "cont-wf", Version: "1",
		Steps: []dag.StepDef{{
			ID: "agent", Task: "agent-task",
			Type: dag.StepTypeAgentLoop,
			Loop: &dag.AgentLoopConfig{MaxIterations: 3},
		}},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cont-run", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))
	taskSub, err := js.PullSubscribe(
		"task.agent-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe failed: %v", err)
	}
	msgs, err := taskSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch initial task failed: %v", err)
	}
	msgs[0].Ack()

	cont := protocol.NewStepEvent(
		protocol.EventStepContinue, "cont-run", "agent", nil)
	contData, _ := cont.Marshal()
	js.Publish(cont.NATSSubject(), contData,
		nats.MsgId(cont.NATSMsgID()))

	// Positive: new task message appears.
	msgs2, err := taskSub.Fetch(
		1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch iteration-1 task: %v", err)
	}
	if len(msgs2) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs2))
	}
	// Positive: iteration count = 1 in snapshot.
	store := NewSnapshotStore(js)
	run, err := store.Load("cont-run")
	if err != nil {
		t.Fatalf("Load run: %v", err)
	}
	if run.Steps["agent"].Iterations != 1 {
		t.Fatalf("Iterations = %d, want 1",
			run.Steps["agent"].Iterations)
	}
}

// skipIfWorkflow builds a->b(SkipIf)->c workflow definition.
func skipIfWorkflow() dag.WorkflowDef {
	return dag.WorkflowDef{
		Name: "skip-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a",
				Type: dag.StepTypeNormal},
			{
				ID: "b", Task: "task-b",
				DependsOn: []string{"a"},
				Type:      dag.StepTypeNormal,
				SkipIf: &dag.ParentCond{
					StepID: "a", Field: "status",
					Op: "==", Value: "skip",
				},
			},
			{ID: "c", Task: "task-c",
				DependsOn: []string{"b"},
				Type:      dag.StepTypeNormal},
		},
	}
}

func TestOrchestratorSkipIfSkipsStep(t *testing.T) {
	// Methodology: complete "a" with output triggering SkipIf.
	// Verify "b" is skipped and "c" proceeds.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := skipIfWorkflow()
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "skip-run", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	subA, _ := js.PullSubscribe(
		"task.task-a.*", "", nats.BindStream("TASK_QUEUES"))
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-a failed: %v", err)
	}
	msgsA[0].Ack()

	output := []byte(`{"status":"skip"}`)
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "skip-run", "a", output)
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Positive: task-c enqueued (b is skipped).
	subC, _ := js.PullSubscribe(
		"task.task-c.*", "", nats.BindStream("TASK_QUEUES"))
	msgsC, err := subC.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-c failed: %v", err)
	}
	if len(msgsC) != 1 {
		t.Fatalf("expected 1 task-c, got %d", len(msgsC))
	}

	// Positive: step "b" is Skipped.
	store := NewSnapshotStore(js)
	run, err := store.Load("skip-run")
	if err != nil {
		t.Fatalf("Load run failed: %v", err)
	}
	if run.Steps["b"].Status != dag.StepStatusSkipped {
		t.Fatalf("b = %v, want Skipped",
			run.Steps["b"].Status)
	}
}

func TestOrchestratorSnapshotAfterCompletion(t *testing.T) {
	// Methodology: after step completion, read KV directly and
	// verify snapshot state matches expectations.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "snap-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-s1",
				Type: dag.StepTypeNormal},
			{
				ID: "s2", Task: "task-s2",
				DependsOn: []string{"s1"},
				Type:      dag.StepTypeNormal,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "snap-run", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))
	time.Sleep(300 * time.Millisecond)

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "snap-run", "s1",
		[]byte(`"result-1"`))
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))
	time.Sleep(500 * time.Millisecond)

	store := NewSnapshotStore(js)
	run, err := store.Load("snap-run")
	if err != nil {
		t.Fatalf("Load snapshot failed: %v", err)
	}
	// Positive: s1 completed with output.
	if run.Steps["s1"].Status != dag.StepStatusCompleted {
		t.Fatalf("s1 = %v, want Completed",
			run.Steps["s1"].Status)
	}
	if string(run.Steps["s1"].Output) != `"result-1"` {
		t.Fatalf("output = %q, want %q",
			string(run.Steps["s1"].Output), `"result-1"`)
	}
}

func TestOrchestratorSnapshotRestore(t *testing.T) {
	// Methodology: write a snapshot to KV manually, verify
	// loadRunAndDef restores it correctly.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "snap-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-s1",
				Type: dag.StepTypeNormal},
			{ID: "s2", Task: "task-s2",
				DependsOn: []string{"s1"},
				Type:      dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	store := NewSnapshotStore(js)
	crafted := dag.WorkflowRun{
		RunID:      "crafted-run",
		WorkflowID: "snap-wf",
		Status:     dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"s1": {
				Status: dag.StepStatusCompleted,
				Output: []byte(`"crafted"`),
			},
			"s2": {Status: dag.StepStatusPending},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(crafted); err != nil {
		t.Fatalf("Save crafted: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	wfDefR, runR, err := orch.loadRunAndDef("crafted-run")
	if err != nil {
		t.Fatalf("loadRunAndDef: %v", err)
	}
	// Positive: restored def matches.
	if wfDefR.Name != "snap-wf" {
		t.Fatalf("def = %q, want snap-wf", wfDefR.Name)
	}
	// Positive: restored run state matches.
	if runR.Steps["s1"].Status != dag.StepStatusCompleted {
		t.Fatalf("s1 = %v, want Completed",
			runR.Steps["s1"].Status)
	}
}

func TestOrchestratorInputSchemaValidation(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	// Define workflow with input schema requiring "repo" field
	schema := json.RawMessage(`{
		"type": "object",
		"required": ["repo"],
		"properties": {
			"repo": {"type": "string"}
		}
	}`)

	wfDef := dag.WorkflowDef{
		Name:        "schema-wf",
		Version:     "1",
		InputSchema: schema,
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Positive: valid input with required "repo" field
	validInput := json.RawMessage(`{"repo": "github.com/test/repo"}`)
	startPayload, _ := json.Marshal(map[string]interface{}{
		"workflow_def": wfDef,
		"input":        validInput,
	})
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "valid-run", startPayload,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()),
	)

	// Task should be enqueued
	sub, _ := js.PullSubscribe(
		"task.task-a.*", "", nats.BindStream("TASK_QUEUES"),
	)
	msgs, err := sub.Fetch(1, nats.MaxWait(3*time.Second))
	if err != nil {
		t.Fatalf("task not enqueued for valid input: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task for valid input, got %d", len(msgs))
	}

	// Negative: invalid input missing "repo" field
	invalidInput := json.RawMessage(`{"wrong_field": "value"}`)
	invalidPayload, _ := json.Marshal(map[string]interface{}{
		"workflow_def": wfDef,
		"input":        invalidInput,
	})
	invalidEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "invalid-run", invalidPayload,
	)
	invalidData, _ := invalidEvt.Marshal()
	js.Publish(
		invalidEvt.NATSSubject(), invalidData,
		nats.MsgId(invalidEvt.NATSMsgID()),
	)

	// Wait a moment for processing
	time.Sleep(500 * time.Millisecond)

	// Check that the run exists but is marked as failed
	store := NewSnapshotStore(js)
	run, err := store.Load("invalid-run")
	if err != nil {
		t.Fatalf("failed run should exist in snapshot: %v", err)
	}
	if run.Status != dag.RunStatusFailed {
		t.Fatalf(
			"expected RunStatusFailed for invalid input, got %s",
			run.Status,
		)
	}

	// No task should be enqueued
	sub2, _ := js.PullSubscribe(
		"task.task-a.invalid-run", "", nats.BindStream("TASK_QUEUES"),
	)
	msgs2, _ := sub2.Fetch(1, nats.MaxWait(500*time.Millisecond))
	if len(msgs2) > 0 {
		t.Fatal("task should not be enqueued for invalid input")
	}
}

func TestOrchestratorStepContinueWithLoopDelay(t *testing.T) {
	// Methodology: verify the LoopDelay path in handleStepContinue.
	// A short delay (50ms) should still produce a task message.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "delay-wf", Version: "1",
		Steps: []dag.StepDef{{
			ID:   "delayed",
			Task: "delay-task",
			Type: dag.StepTypeAgentLoop,
			Loop: &dag.AgentLoopConfig{
				MaxIterations: 5,
				LoopDelay:     50 * time.Millisecond,
			},
		}},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "delay-run", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	taskSub, err := js.PullSubscribe(
		"task.delay-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	if err != nil {
		t.Fatalf("PullSubscribe failed: %v", err)
	}
	// Drain initial task.
	msgs, err := taskSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch initial task failed: %v", err)
	}
	msgs[0].Ack()

	// Send step.continue — delayed re-enqueue.
	cont := protocol.NewStepEvent(
		protocol.EventStepContinue, "delay-run", "delayed", nil)
	contData, _ := cont.Marshal()
	js.Publish(cont.NATSSubject(), contData,
		nats.MsgId(cont.NATSMsgID()))

	// Positive: task appears after the delay.
	msgs2, err := taskSub.Fetch(1, nats.MaxWait(3*time.Second))
	if err != nil {
		t.Fatalf("delayed task did not arrive: %v", err)
	}
	if len(msgs2) != 1 {
		t.Fatalf("expected 1 delayed task, got %d", len(msgs2))
	}
}

func TestOrchestratorSkipIfCompletesWorkflow(t *testing.T) {
	// Methodology: a -> b (SkipIf). Skip all -> workflow complete.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	wfDef := dag.WorkflowDef{
		Name: "skipall-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-a",
				Type: dag.StepTypeNormal},
			{
				ID: "b", Task: "task-b",
				DependsOn: []string{"a"},
				Type:      dag.StepTypeNormal,
				SkipIf: &dag.ParentCond{
					StepID: "a", Field: "skip",
					Op: "==", Value: true,
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

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "skipall-run", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	subA, _ := js.PullSubscribe(
		"task.task-a.*", "", nats.BindStream("TASK_QUEUES"))
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-a failed: %v", err)
	}
	msgsA[0].Ack()

	// Complete a with skip=true — b should be skipped,
	// workflow should complete.
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "skipall-run", "a",
		[]byte(`{"skip":true}`))
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	store := NewSnapshotStore(js)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load("skipall-run")
		if err == nil &&
			run.Status == dag.RunStatusCompleted {
			// Positive: workflow completed.
			if run.Steps["b"].Status != dag.StepStatusSkipped {
				t.Fatalf("b status = %v, want Skipped",
					run.Steps["b"].Status)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should complete when all steps done/skipped")
}

// waitForEventType polls a subscription for a specific event type.
func waitForEventType(
	sub *nats.Subscription,
	target protocol.EventType,
	timeout time.Duration,
) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		m, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		m.Ack()
		evt, _ := protocol.UnmarshalEvent(m.Data)
		if evt.Type == target {
			return true
		}
	}
	return false
}

func TestOrchestratorChildFailureNotifiesParent(t *testing.T) {
	// Methodology: spawn a child, fail it, verify parent gets
	// workflow.child.failed event.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}
	js, _ := nc.JetStream()
	defKV, _ := js.KeyValue("workflow_defs")

	childDef := dag.WorkflowDef{
		Name: "fail-child", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "fail-task",
				Type: dag.StepTypeNormal},
		},
	}
	childDefData, _ := json.Marshal(childDef)
	defKV.Put("fail-child", childDefData)

	parentSub, err := js.SubscribeSync(
		"history.parent-fail",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	spawnPayload, _ := json.Marshal(map[string]string{
		"child_run_id":   "child-fail-1",
		"child_workflow": "fail-child",
		"parent_step_id": "parent-step",
	})
	spawnEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowSpawn, "parent-fail",
		spawnPayload)
	data, _ := spawnEvt.Marshal()
	js.Publish(spawnEvt.NATSSubject(), data,
		nats.MsgId(spawnEvt.NATSMsgID()))
	time.Sleep(300 * time.Millisecond)

	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "child-fail-1", "s1",
		[]byte(`"child error"`))
	failData, _ := failEvt.Marshal()
	js.Publish(failEvt.NATSSubject(), failData,
		nats.MsgId(failEvt.NATSMsgID()))

	// Positive: parent notified.
	found := waitForEventType(parentSub,
		protocol.EventWorkflowChildFailed, 5*time.Second)
	if !found {
		t.Fatal("expected child.failed on parent")
	}
	// Positive: child run is Failed.
	store := NewSnapshotStore(js)
	childRun, _ := store.Load("child-fail-1")
	if childRun.Status != dag.RunStatusFailed {
		t.Fatalf("child = %v, want Failed",
			childRun.Status)
	}
}

func TestSplitTraceparent(t *testing.T) {
	// Methodology: unit test for the traceparent parsing utility.

	// Positive: valid W3C traceparent header.
	traceID, spanID, ok := splitTraceparent(
		"00-abc123-def456-01")
	if !ok {
		t.Fatal("expected ok=true for valid traceparent")
	}
	if traceID != "abc123" || spanID != "def456" {
		t.Fatalf("traceID=%q spanID=%q, want abc123/def456",
			traceID, spanID)
	}

	// Negative: invalid format (wrong version prefix).
	_, _, ok2 := splitTraceparent("01-abc-def-01")
	if ok2 {
		t.Fatal("expected ok=false for version != 00")
	}

	// Negative: too few segments.
	_, _, ok3 := splitTraceparent("00-abc-def")
	if ok3 {
		t.Fatal("expected ok=false for 3-segment string")
	}
}

func TestIsHandledEventType(t *testing.T) {
	// Methodology: unit test for event type filtering.

	// Positive: known types are handled.
	handled := []protocol.EventType{
		protocol.EventWorkflowStarted,
		protocol.EventStepCompleted,
		protocol.EventStepContinue,
		protocol.EventStepFailed,
		protocol.EventWorkflowSpawn,
		protocol.EventWorkflowCancelled,
	}
	for _, et := range handled {
		if !isHandledEventType(et) {
			t.Fatalf("%s should be handled", et)
		}
	}

	// Negative: unknown type is not handled.
	if isHandledEventType("foo.bar") {
		t.Fatal("foo.bar should not be handled")
	}
}

func TestErrString(t *testing.T) {
	// Positive: nil returns empty string.
	if errString(nil) != "" {
		t.Fatal("errString(nil) should be empty")
	}
	// Positive: non-nil returns error message.
	if errString(fmt.Errorf("boom")) != "boom" {
		t.Fatal("errString should return error text")
	}
}

func TestParseTraceparentFromHeader(t *testing.T) {
	// Methodology: unit test for traceparent parsing from NATS
	// message headers vs event field fallback.

	// Positive: header takes priority.
	msg := &nats.Msg{
		Header: nats.Header{
			"traceparent": {"00-tid1-sid1-01"},
		},
	}
	evt := &protocol.Event{TraceParent: "00-tid2-sid2-01"}
	traceID, spanID, ok := parseTraceparent(msg, evt)
	if !ok {
		t.Fatal("expected ok=true with header traceparent")
	}
	if traceID != "tid1" || spanID != "sid1" {
		t.Fatalf("header should take priority: got %s/%s",
			traceID, spanID)
	}

	// Positive: falls back to event field when no header.
	msg2 := &nats.Msg{}
	traceID2, spanID2, ok2 := parseTraceparent(msg2, evt)
	if !ok2 {
		t.Fatal("expected ok=true with event traceparent")
	}
	if traceID2 != "tid2" || spanID2 != "sid2" {
		t.Fatalf("should fall back to event: got %s/%s",
			traceID2, spanID2)
	}

	// Negative: neither header nor event has traceparent.
	msg3 := &nats.Msg{}
	evt3 := &protocol.Event{}
	_, _, ok3 := parseTraceparent(msg3, evt3)
	if ok3 {
		t.Fatal("expected ok=false when no traceparent")
	}
}

func TestOrchestratorTraceparentPropagation(t *testing.T) {
	// Methodology: verify traceparent from a published event
	// flows through the orchestrator without breaking processing.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "trace-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "trace-task",
				Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Publish with traceparent header.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "trace-run", defData)
	startData, _ := startEvt.Marshal()
	tp := "00-aaaa-bbbb-01"
	msg := &nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    startData,
		Header: nats.Header{
			"Nats-Msg-Id": {startEvt.NATSMsgID()},
			"traceparent": {tp},
		},
	}
	js.PublishMsg(msg)

	// Positive: task should still be enqueued.
	sub, _ := js.PullSubscribe(
		"task.trace-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("task not enqueued with traceparent: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs))
	}
}

func TestCompletedSetAndQueuedSet(t *testing.T) {
	// Methodology: unit test for the set-building helpers.
	run := dag.WorkflowRun{
		RunID:      "test-sets",
		WorkflowID: "wf",
		Status:     dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted},
			"b": {Status: dag.StepStatusSkipped},
			"c": {Status: dag.StepStatusQueued},
			"d": {Status: dag.StepStatusPending},
			"e": {Status: dag.StepStatusFailed},
			"f": {Status: dag.StepStatusRunning},
		},
	}

	completed := completedSet(run)
	// Positive: a and b are in completed set.
	if !completed["a"] || !completed["b"] {
		t.Fatal("a and b should be in completed set")
	}
	// Negative: c, d, e, f are NOT in completed set.
	if completed["c"] || completed["d"] ||
		completed["e"] || completed["f"] {
		t.Fatal("c/d/e/f should not be in completed set")
	}

	queued := queuedSet(run)
	// Positive: a, b, c, e, f are in queued set.
	for _, id := range []string{"a", "b", "c", "e", "f"} {
		if !queued[id] {
			t.Fatalf("%s should be in queued set", id)
		}
	}
	// Negative: d (pending) is NOT in queued set.
	if queued["d"] {
		t.Fatal("d (pending) should not be in queued set")
	}
}

func TestOrchestratorCancelNonRunningIsNoop(t *testing.T) {
	// Methodology: cancel an already-completed run. The handler
	// should return nil without modifying the run.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "cancel-noop", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t1", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start and complete the workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cnoop-run", defData)
	sd, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), sd,
		nats.MsgId(startEvt.NATSMsgID()))
	time.Sleep(200 * time.Millisecond)

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "cnoop-run", "s1",
		[]byte(`"done"`))
	cd, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), cd,
		nats.MsgId(compEvt.NATSMsgID()))

	store := NewSnapshotStore(js)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load("cnoop-run")
		if err == nil &&
			run.Status == dag.RunStatusCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Now cancel the completed workflow.
	cancelEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, "cnoop-run", nil)
	ccd, _ := cancelEvt.Marshal()
	js.Publish(cancelEvt.NATSSubject(), ccd,
		nats.MsgId(cancelEvt.NATSMsgID()))
	time.Sleep(300 * time.Millisecond)

	// Positive: run is still Completed (not Cancelled).
	run, _ := store.Load("cnoop-run")
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("status = %v, want Completed (cancel is noop)",
			run.Status)
	}
	// Positive: step is still Completed.
	if run.Steps["s1"].Status != dag.StepStatusCompleted {
		t.Fatalf("step = %v, want Completed",
			run.Steps["s1"].Status)
	}
}

func TestOrchestratorStartWithInput(t *testing.T) {
	// Methodology: start a workflow with input payload (structured
	// format with workflow_def + input). Verify the workflow starts.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "input-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-in",
				Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start with structured payload.
	payload, _ := json.Marshal(map[string]interface{}{
		"workflow_def": wfDef,
		"input":        map[string]string{"key": "val"},
	})
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "input-run", payload)
	sd, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), sd,
		nats.MsgId(startEvt.NATSMsgID()))

	// Positive: task should be enqueued.
	sub, _ := js.PullSubscribe(
		"task.task-in.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("task not enqueued: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs))
	}

	// Positive: run is Running.
	store := NewSnapshotStore(js)
	run, err := store.Load("input-run")
	if err != nil {
		t.Fatalf("load run: %v", err)
	}
	if run.Status != dag.RunStatusRunning {
		t.Fatalf("status = %v, want Running", run.Status)
	}
}

func TestOrchestratorHandlesMalformedEvent(t *testing.T) {
	// Methodology: verify that malformed event data does not
	// crash the orchestrator and the system recovers.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "malform-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t1", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Publish garbage data to history stream.
	js.Publish("history.malform-run",
		[]byte("not valid json"),
		nats.MsgId("malform-1"))

	time.Sleep(200 * time.Millisecond)

	// Positive: orchestrator survives and processes next event.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "recover-run", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	store := NewSnapshotStore(js)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load("recover-run")
		if err == nil && run.Status == dag.RunStatusRunning {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Positive: orchestrator recovered from malformed event.
	t.Fatalf("orchestrator should recover from malformed event")
}

func TestCheckLoopBoundsNoLoopConfig(t *testing.T) {
	// Methodology: unit test for checkLoopBounds edge cases.

	// Positive: nil Loop config returns false (no bounds).
	step := dag.StepDef{ID: "s", Task: "t"}
	exceeded, reason := checkLoopBounds(step, dag.StepState{})
	if exceeded {
		t.Fatal("nil Loop should not exceed bounds")
	}
	if reason != "" {
		t.Fatalf("reason should be empty, got %q", reason)
	}
}

func TestOrchestratorStopIdempotent(t *testing.T) {
	// Methodology: verify Stop can be called multiple times
	// and on a never-started orchestrator without panic.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())

	// Positive: Stop on never-started is safe.
	orch.Stop()
	orch.Stop()

	// Positive: Stop after Start is safe.
	orch.Start()
	orch.Stop()
	orch.Stop()
}

func TestOrchestratorHandlesUnknownEventType(t *testing.T) {
	// Methodology: verify unknown event types are acked silently.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "unknown-evt", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t1", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Publish an event with an unhandled type.
	evt := protocol.Event{
		Type:  "custom.unknown",
		RunID: "unknown-run",
	}
	data, _ := json.Marshal(evt)
	js.Publish("history.unknown-run", data,
		nats.MsgId("unknown-run.custom"))
	time.Sleep(200 * time.Millisecond)

	// Positive: no run was created (event was ignored).
	store := NewSnapshotStore(js)
	_, err := store.Load("unknown-run")
	if err == nil {
		t.Fatal("unknown event should not create a run")
	}

	// Positive: subsequent events still work.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "post-unknown",
		defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))
	time.Sleep(300 * time.Millisecond)
	_, err = store.Load("post-unknown")
	if err != nil {
		t.Fatalf("orchestrator should still work: %v", err)
	}
}

func TestLoadRunAndDefMissingRun(t *testing.T) {
	// Methodology: verify loadRunAndDef returns error for
	// a run that doesn't exist in the snapshot store.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())

	// Positive: error returned for missing run.
	_, _, err := orch.loadRunAndDef("nonexistent-run")
	if err == nil {
		t.Fatal("expected error for missing run")
	}

	// Positive: error message mentions the run ID.
	if !strings.Contains(err.Error(), "nonexistent-run") {
		t.Fatalf("error should mention run ID: %v", err)
	}
}

func TestLoadRunAndDefMissingWorkflowDef(t *testing.T) {
	// Methodology: snapshot exists but the workflow definition
	// is not registered. loadRunAndDef should return error.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	store := NewSnapshotStore(js)
	run := dag.WorkflowRun{
		RunID:      "orphan-run",
		WorkflowID: "missing-def",
		Status:     dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"s1": {Status: dag.StepStatusPending},
		},
		CreatedAt: time.Now().UTC(),
	}
	if err := store.Save(run); err != nil {
		t.Fatalf("Save: %v", err)
	}

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())

	// Positive: error returned for missing workflow def.
	_, _, err := orch.loadRunAndDef("orphan-run")
	if err == nil {
		t.Fatal("expected error for missing workflow def")
	}

	// Positive: error references the workflow ID.
	if !strings.Contains(err.Error(), "missing-def") {
		t.Fatalf("error should mention workflow ID: %v", err)
	}
}

func TestBuildTaskMsg(t *testing.T) {
	// Methodology: unit test for buildTaskMsg construction.
	msg := buildTaskMsg("task.foo.run-1", []byte("data"),
		"run-1.foo.queued")

	// Positive: subject is set.
	if msg.Subject != "task.foo.run-1" {
		t.Fatalf("Subject = %q, want task.foo.run-1",
			msg.Subject)
	}
	// Positive: dedup ID is set.
	if msg.Header.Get("Nats-Msg-Id") != "run-1.foo.queued" {
		t.Fatalf("Nats-Msg-Id = %q, want run-1.foo.queued",
			msg.Header.Get("Nats-Msg-Id"))
	}
}

func TestStepSubjectRouting(t *testing.T) {
	// Methodology: unit test for subject resolution.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	routes := map[dag.StepType]string{
		dag.StepTypeAgent: "agent.task",
	}
	orch := NewOrchestrator(nc, observe.NewNoopTelemetry(),
		WithStepRoutes(routes))

	// Normal step -> default prefix.
	step := dag.StepDef{
		ID: "s1", Task: "my-task",
		Type: dag.StepTypeNormal,
	}
	subj := orch.stepSubject(step, "run-1")
	if subj != "task.my-task.run-1" {
		t.Fatalf("subject = %q, want task.my-task.run-1", subj)
	}

	// Agent step -> custom prefix.
	agentStep := dag.StepDef{
		ID: "s2", Task: "llm",
		Type: dag.StepTypeAgent,
	}
	agentSubj := orch.stepSubject(agentStep, "run-1")
	if agentSubj != "agent.task.llm.run-1" {
		t.Fatalf("subject = %q, want agent.task.llm.run-1",
			agentSubj)
	}
}

func TestFindStepDef(t *testing.T) {
	wfDef := dag.WorkflowDef{
		Name: "find-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "a", Task: "ta", Type: dag.StepTypeNormal},
			{ID: "b", Task: "tb", Type: dag.StepTypeNormal},
		},
	}

	// Positive: found step.
	step, found := findStepDef(wfDef, "b")
	if !found {
		t.Fatal("expected to find step b")
	}
	if step.Task != "tb" {
		t.Fatalf("step.Task = %q, want tb", step.Task)
	}

	// Negative: missing step.
	_, found2 := findStepDef(wfDef, "z")
	if found2 {
		t.Fatal("expected not to find step z")
	}
}

func TestPublishReadyTasksParallel(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	// Create a workflow with 5 independent entry steps (no deps)
	steps := make([]dag.StepDef, 5)
	for i := range steps {
		steps[i] = dag.StepDef{
			ID:   fmt.Sprintf("s%d", i),
			Task: fmt.Sprintf("task-%d", i),
			Type: dag.StepTypeNormal,
		}
	}
	wfDef := dag.WorkflowDef{
		Name: "parallel-wf", Version: "1", Steps: steps,
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-parallel", defData,
	)
	evtData, _ := evt.Marshal()
	js.Publish(evt.NATSSubject(), evtData, nats.MsgId(evt.NATSMsgID()))

	// All 5 tasks should appear
	for i := 0; i < 5; i++ {
		subject := fmt.Sprintf("task.task-%d.*", i)
		sub, err := js.PullSubscribe(subject, "",
			nats.BindStream("TASK_QUEUES"))
		if err != nil {
			t.Fatalf("PullSubscribe %s: %v", subject, err)
		}
		msgs, err := sub.Fetch(1, nats.MaxWait(5*time.Second))
		if err != nil {
			t.Fatalf("Fetch task-%d failed: %v", i, err)
		}
		// Positive: each task published
		if len(msgs) != 1 {
			t.Fatalf("task-%d: expected 1 msg, got %d", i, len(msgs))
		}
	}
}

func TestOrchestratorMapStepFanOut(t *testing.T) {
	// Methodology: workflow has fetch -> map -> summarize.
	// fetch returns a JSON array of 3 items.
	// map processes each item (3 instances).
	// summarize receives the collected array of results.
	// Verify: all 3 map instances complete, summarize gets [r0, r1, r2].
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "map-fanout", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "fetch", Task: "fetch-task",
				Type: dag.StepTypeNormal,
			},
			{
				ID: "process", Task: "process-task",
				Type:      dag.StepTypeMap,
				DependsOn: []string{"fetch"},
				Map:       &dag.MapConfig{MaxItems: 10},
			},
			{
				ID: "summarize", Task: "summarize-task",
				Type:      dag.StepTypeNormal,
				DependsOn: []string{"process"},
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "map-run-1", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	// Drain fetch task.
	fetchSub, _ := js.PullSubscribe(
		"task.fetch-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgs, err := fetchSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch fetch-task failed: %v", err)
	}
	msgs[0].Ack()

	// Complete fetch with a JSON array of 3 items.
	fetchOutput := []byte(`["item-a","item-b","item-c"]`)
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "map-run-1",
		"fetch", fetchOutput)
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Wait for 3 map instance tasks to appear.
	mapSub, _ := js.PullSubscribe(
		"task.process-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	mapMsgs, err := mapSub.Fetch(
		3, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch map tasks failed: %v", err)
	}
	// Positive: exactly 3 map instance tasks published.
	if len(mapMsgs) != 3 {
		t.Fatalf("expected 3 map tasks, got %d", len(mapMsgs))
	}

	// Complete all 3 map instances.
	for i := 0; i < 3; i++ {
		mapMsgs[i].Ack()
		instanceID := fmt.Sprintf("process.map.%d", i)
		result := []byte(fmt.Sprintf(`"result-%d"`, i))
		evt := protocol.NewStepEvent(
			protocol.EventStepCompleted, "map-run-1",
			instanceID, result)
		data, _ := evt.Marshal()
		msgID := fmt.Sprintf(
			"map-run-1.%s.completed", instanceID)
		js.Publish(evt.NATSSubject(), data,
			nats.MsgId(msgID))
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for summarize task.
	sumSub, _ := js.PullSubscribe(
		"task.summarize-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	sumMsgs, err := sumSub.Fetch(
		1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch summarize-task failed: %v", err)
	}
	// Positive: summarize task receives collected array.
	if len(sumMsgs) != 1 {
		t.Fatalf("expected 1 summarize task, got %d",
			len(sumMsgs))
	}
	var payload protocol.TaskPayload
	if err := json.Unmarshal(
		sumMsgs[0].Data, &payload,
	); err != nil {
		t.Fatalf("unmarshal summarize payload: %v", err)
	}
	// Verify input is the collected array.
	var collected []json.RawMessage
	if err := json.Unmarshal(
		payload.Input, &collected,
	); err != nil {
		t.Fatalf("unmarshal collected: %v", err)
	}
	if len(collected) != 3 {
		t.Fatalf("collected len = %d, want 3",
			len(collected))
	}

	// Complete summarize -> workflow completes.
	sumMsgs[0].Ack()
	sumEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "map-run-1",
		"summarize", []byte(`"final"`))
	sumData, _ := sumEvt.Marshal()
	js.Publish(sumEvt.NATSSubject(), sumData,
		nats.MsgId(sumEvt.NATSMsgID()))

	store := NewSnapshotStore(js)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load("map-run-1")
		if err == nil &&
			run.Status == dag.RunStatusCompleted {
			// Positive: workflow completed.
			if run.Steps["process"].Status !=
				dag.StepStatusCompleted {
				t.Fatalf("process = %v, want Completed",
					run.Steps["process"].Status)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should complete after map fan-out")
}

func TestOrchestratorMapStepFailFast(t *testing.T) {
	// Methodology: workflow has fetch -> map.
	// One map instance fails. Verify: map step and workflow fail.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "map-fail", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "fetch", Task: "fetch-task",
				Type: dag.StepTypeNormal,
			},
			{
				ID: "process", Task: "process-task",
				Type:      dag.StepTypeMap,
				DependsOn: []string{"fetch"},
				Map:       &dag.MapConfig{MaxItems: 10},
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "map-fail-1", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	// Drain and complete fetch.
	fetchSub, _ := js.PullSubscribe(
		"task.fetch-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	fMsgs, err := fetchSub.Fetch(
		1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch fetch-task failed: %v", err)
	}
	fMsgs[0].Ack()

	fetchOutput := []byte(`["a","b","c"]`)
	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "map-fail-1",
		"fetch", fetchOutput)
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Wait for map tasks.
	mapSub, _ := js.PullSubscribe(
		"task.process-task.*", "",
		nats.BindStream("TASK_QUEUES"))
	mapMsgs, err := mapSub.Fetch(
		3, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch map tasks failed: %v", err)
	}
	for _, m := range mapMsgs {
		m.Ack()
	}

	// Fail instance 1.
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "map-fail-1",
		"process.map.1", []byte(`"instance error"`))
	failData, _ := failEvt.Marshal()
	js.Publish(failEvt.NATSSubject(), failData,
		nats.MsgId("map-fail-1.process.map.1.failed"))

	// Verify workflow fails.
	store := NewSnapshotStore(js)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load("map-fail-1")
		if err == nil &&
			run.Status == dag.RunStatusFailed {
			// Positive: map step is failed.
			if run.Steps["process"].Status !=
				dag.StepStatusFailed {
				t.Fatalf("process = %v, want Failed",
					run.Steps["process"].Status)
			}
			// Positive: map instance 1 is failed.
			inst := run.Steps["process"].MapInstances
			if len(inst) != 3 {
				t.Fatalf("MapInstances len = %d, want 3",
					len(inst))
			}
			if inst[1].Status != dag.StepStatusFailed {
				t.Fatalf("instance[1] = %v, want Failed",
					inst[1].Status)
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should fail after map instance failure")
}

func TestMapInstanceIDHelpers(t *testing.T) {
	// Methodology: unit tests for map instance ID construction
	// and parsing utilities.

	// Positive: mapInstanceID constructs correct format.
	id := mapInstanceID("process", 2)
	if id != "process.map.2" {
		t.Fatalf("mapInstanceID = %q, want process.map.2", id)
	}

	// Positive: isMapInstanceID detects compound IDs.
	if !isMapInstanceID("process.map.0") {
		t.Fatal("process.map.0 should be a map instance ID")
	}

	// Negative: normal step IDs are not map instances.
	if isMapInstanceID("process") {
		t.Fatal("process should not be a map instance ID")
	}

	// Positive: parseMapInstanceID extracts base and index.
	base, idx := parseMapInstanceID("process.map.5")
	if base != "process" || idx != 5 {
		t.Fatalf("parse = (%q, %d), want (process, 5)",
			base, idx)
	}
}

func TestOrchestratorSleepStep(t *testing.T) {
	// Methodology: workflow has task-a -> sleep(100ms) -> task-b.
	// Start orchestrator, complete task-a manually, verify the sleep
	// step completes via durable timer, then task-b gets enqueued.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "sleep-wf", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "task-a", Task: "echo-a",
				Type: dag.StepTypeNormal,
			},
			{
				ID: "nap", Type: dag.StepTypeSleep,
				DependsOn: []string{"task-a"},
				Duration:  100 * time.Millisecond,
			},
			{
				ID: "task-b", Task: "echo-b",
				Type:      dag.StepTypeNormal,
				DependsOn: []string{"nap"},
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "sleep-run-1", defData)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	// Drain and complete task-a.
	subA, _ := js.PullSubscribe(
		"task.echo-a.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-a failed: %v", err)
	}
	msgsA[0].Ack()

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "sleep-run-1",
		"task-a", []byte(`"done"`))
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// task-b should appear after the sleep timer fires (~100ms).
	subB, _ := js.PullSubscribe(
		"task.echo-b.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsB, err := subB.Fetch(1, nats.MaxWait(10*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-b failed (sleep didn't fire?): %v", err)
	}

	// Positive: task-b was enqueued.
	if len(msgsB) != 1 {
		t.Fatalf("expected 1 task-b message, got %d", len(msgsB))
	}
	msgsB[0].Ack()

	// Complete task-b so workflow finishes.
	compB := protocol.NewStepEvent(
		protocol.EventStepCompleted, "sleep-run-1",
		"task-b", []byte(`"final"`))
	compBData, _ := compB.Marshal()
	js.Publish(compB.NATSSubject(), compBData,
		nats.MsgId(compB.NATSMsgID()))

	// Wait for workflow to complete.
	time.Sleep(500 * time.Millisecond)

	run, err := orch.store.Load("sleep-run-1")
	if err != nil {
		t.Fatalf("load run failed: %v", err)
	}

	// Positive: workflow completed.
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"expected run status Completed, got %s",
			run.Status,
		)
	}

	// Positive: sleep step completed.
	sleepState := run.Steps["nap"]
	if sleepState.Status != dag.StepStatusCompleted {
		t.Fatalf(
			"expected sleep step Completed, got %s",
			sleepState.Status,
		)
	}

	// Negative: task-a should not be in pending state.
	if run.Steps["task-a"].Status == dag.StepStatusPending {
		t.Fatal("task-a should not still be pending")
	}
}

func TestOrchestratorRateLimitDelaysTask(t *testing.T) {
	// Methodology: workflow with a single step that has a global rate
	// limit of 1 per 10 seconds. Start two runs quickly. The first
	// should get its task published immediately. The second should be
	// deferred to the SLEEP_TIMERS stream instead of TASK_QUEUES.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name:    "rl-wf",
		Version: "1",
		Steps: []dag.StepDef{{
			ID:   "rl-step",
			Task: "rl-task",
			Type: dag.StepTypeNormal,
			RateLimit: &dag.RateLimit{
				Limit:  1,
				Period: 10 * time.Second,
			},
		}},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start first run — should consume the one token.
	evt1 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "rl-run-1", defData,
	)
	data1, _ := evt1.Marshal()
	js.Publish(
		evt1.NATSSubject(), data1,
		nats.MsgId(evt1.NATSMsgID()),
	)

	// First task should appear on TASK_QUEUES.
	taskSub, err := js.PullSubscribe(
		"task.rl-task.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe: %v", err)
	}
	msgs, err := taskSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch first task: %v", err)
	}
	// Positive: first task published normally.
	if len(msgs) != 1 {
		t.Fatalf("expected 1 task, got %d", len(msgs))
	}
	msgs[0].Ack()

	// Start second run — rate limit should be exhausted.
	evt2 := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "rl-run-2", defData,
	)
	data2, _ := evt2.Marshal()
	js.Publish(
		evt2.NATSSubject(), data2,
		nats.MsgId(evt2.NATSMsgID()),
	)

	// Second task should NOT appear on TASK_QUEUES.
	time.Sleep(500 * time.Millisecond)
	msgs2, _ := taskSub.Fetch(
		1, nats.MaxWait(500*time.Millisecond),
	)
	// Negative: second task was deferred, not published.
	if len(msgs2) > 0 {
		t.Fatal("second task should be deferred by rate limit")
	}

	// The deferred task should be on the SLEEP_TIMERS stream.
	sleepSub, err := js.PullSubscribe(
		"sleep.>", "",
		nats.BindStream("SLEEP_TIMERS"),
	)
	if err != nil {
		t.Fatalf("PullSubscribe sleep: %v", err)
	}
	sleepMsgs, err := sleepSub.Fetch(
		1, nats.MaxWait(3*time.Second),
	)
	if err != nil {
		t.Fatalf("Fetch sleep timer: %v", err)
	}
	// Positive: a rate_retry timer was scheduled.
	if len(sleepMsgs) != 1 {
		t.Fatalf(
			"expected 1 sleep timer msg, got %d",
			len(sleepMsgs),
		)
	}

	var tm TimerMessage
	if err := json.Unmarshal(
		sleepMsgs[0].Data, &tm,
	); err != nil {
		t.Fatalf("unmarshal timer: %v", err)
	}
	// Positive: action is rate_retry.
	if tm.Action != TimerActionRateRetry {
		t.Fatalf("action = %q, want rate_retry", tm.Action)
	}
	// Negative: TaskType is set correctly.
	if tm.TaskType != "rl-task" {
		t.Fatalf("TaskType = %q, want rl-task", tm.TaskType)
	}
}

func TestOrchestratorWaitForEventMatches(t *testing.T) {
	// Methodology: workflow has task-a -> wait-for-event -> task-b.
	// Complete task-a with output containing order_id. Publish a
	// matching event to the EVENTS stream. Verify task-b runs and
	// workflow completes.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "wait-wf", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "task-a", Task: "echo-a",
				Type: dag.StepTypeNormal,
			},
			{
				ID:        "wait-step",
				Type:      dag.StepTypeWaitForEvent,
				DependsOn: []string{"task-a"},
				WaitForEvent: &dag.WaitForEventOpts{
					Event: "payment.completed",
					Match: dag.Match{
						Left:  "order_id",
						Op:    dag.MatchOpEq,
						Right: "step.task-a.output.order_id",
					},
					Timeout: 5 * time.Second,
				},
			},
			{
				ID: "task-b", Task: "echo-b",
				Type:      dag.StepTypeNormal,
				DependsOn: []string{"wait-step"},
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Start workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "wait-run-1", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	// Drain and complete task-a with order_id output.
	subA, _ := js.PullSubscribe(
		"task.echo-a.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-a failed: %v", err)
	}
	msgsA[0].Ack()

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "wait-run-1",
		"task-a", []byte(`{"order_id":"ord-abc"}`))
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Wait for the wait step to register with the correlator.
	time.Sleep(500 * time.Millisecond)

	// Publish a matching event on the EVENTS stream.
	eventPayload := []byte(
		`{"order_id":"ord-abc","status":"paid"}`,
	)
	js.Publish("event.payment.completed", eventPayload)

	// task-b should be enqueued after the wait step matches.
	subB, _ := js.PullSubscribe(
		"task.echo-b.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsB, err := subB.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-b failed (timeout?): %v", err)
	}

	// Positive: task-b was dispatched.
	if len(msgsB) != 1 {
		t.Fatalf("expected 1 task-b message, got %d", len(msgsB))
	}

	// Complete task-b to finish the workflow.
	msgsB[0].Ack()
	compB := protocol.NewStepEvent(
		protocol.EventStepCompleted, "wait-run-1",
		"task-b", []byte(`"final"`))
	compBData, _ := compB.Marshal()
	js.Publish(compB.NATSSubject(), compBData,
		nats.MsgId(compB.NATSMsgID()))

	time.Sleep(500 * time.Millisecond)

	// Positive: run should be completed.
	store := NewSnapshotStore(js)
	run, err := store.Load("wait-run-1")
	if err != nil {
		t.Fatalf("Load run failed: %v", err)
	}
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("run.Status = %v, want Completed", run.Status)
	}

	// Negative: wait step output should be the event payload.
	waitState := run.Steps["wait-step"]
	if string(waitState.Output) !=
		`{"order_id":"ord-abc","status":"paid"}` {
		t.Fatalf("wait step output = %s, want event payload",
			string(waitState.Output))
	}
}

func TestOrchestratorWaitForEventTimeout(t *testing.T) {
	// Methodology: workflow has task-a -> wait-for-event(200ms) ->
	// task-b. Complete task-a, do NOT publish matching event. Verify
	// the wait step completes with timeout output and task-b still
	// runs (timeout is not a failure).
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "wait-timeout-wf", Version: "1",
		Steps: []dag.StepDef{
			{
				ID: "task-a", Task: "echo-a",
				Type: dag.StepTypeNormal,
			},
			{
				ID:        "wait-step",
				Type:      dag.StepTypeWaitForEvent,
				DependsOn: []string{"task-a"},
				WaitForEvent: &dag.WaitForEventOpts{
					Event: "payment.completed",
					Match: dag.Match{
						Left:  "order_id",
						Op:    dag.MatchOpEq,
						Right: "step.task-a.output.order_id",
					},
					Timeout: 200 * time.Millisecond,
				},
			},
			{
				ID: "task-b", Task: "echo-b",
				Type:      dag.StepTypeNormal,
				DependsOn: []string{"wait-step"},
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "wait-run-2", defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	subA, _ := js.PullSubscribe(
		"task.echo-a.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-a failed: %v", err)
	}
	msgsA[0].Ack()

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "wait-run-2",
		"task-a", []byte(`{"order_id":"ord-xyz"}`))
	compData, _ := compEvt.Marshal()
	js.Publish(compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	// Do NOT publish a matching event. Wait for timeout.
	// task-b should still be enqueued after the timeout.
	subB, _ := js.PullSubscribe(
		"task.echo-b.*", "",
		nats.BindStream("TASK_QUEUES"))
	msgsB, err := subB.Fetch(1, nats.MaxWait(10*time.Second))
	if err != nil {
		t.Fatalf(
			"Fetch task-b after timeout failed: %v", err,
		)
	}

	// Positive: task-b was dispatched after timeout.
	if len(msgsB) != 1 {
		t.Fatalf("expected 1 task-b message, got %d", len(msgsB))
	}

	// Check the wait step has timeout output.
	store := NewSnapshotStore(js)
	run, loadErr := store.Load("wait-run-2")
	if loadErr != nil {
		t.Fatalf("Load run failed: %v", loadErr)
	}
	waitState := run.Steps["wait-step"]

	// Positive: wait step is completed (not failed).
	if waitState.Status != dag.StepStatusCompleted {
		t.Fatalf("wait step status = %v, want Completed",
			waitState.Status)
	}

	// Negative: output indicates timeout, not a match.
	if string(waitState.Output) != `{"timeout":true}` {
		t.Fatalf("wait step output = %s, want timeout indicator",
			string(waitState.Output))
	}
}

func TestNonRetriableFailureSkipsRetries(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "test-nr", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  5,
			Strategy:     dag.RetryFixed,
			InitialDelay: time.Second,
		},
		Steps: []dag.StepDef{
			{
				ID:   "a",
				Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	// Wait for task-a to be enqueued (proves start was processed).
	taskSub, subErr := js.SubscribeSync(
		"task.task-a.>",
		nats.AckExplicit(),
		nats.DeliverAll(),
	)
	if subErr != nil {
		t.Fatalf("subscribe task-a: %v", subErr)
	}

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		"run-nr-1",
		defData,
	)
	startData, _ := startEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    startData,
		Header: nats.Header{
			"Nats-Msg-Id": {startEvt.NATSMsgID()},
		},
	})

	// Wait for the task to appear — proves workflow was created.
	taskMsg, taskErr := taskSub.NextMsg(3 * time.Second)
	if taskErr != nil {
		t.Fatalf("task-a not enqueued: %v", taskErr)
	}
	taskMsg.Ack()

	failPayload, _ := json.Marshal(protocol.StepFailedPayload{
		Error:       "permanent error",
		FailureType: protocol.FailureTypeNonRetriable,
	})
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed,
		"run-nr-1", "a",
		failPayload,
	)
	failData, _ := failEvt.Marshal()
	js.PublishMsg(&nats.Msg{
		Subject: failEvt.NATSSubject(),
		Data:    failData,
		Header: nats.Header{
			"Nats-Msg-Id": {failEvt.NATSMsgID()},
		},
	})

	time.Sleep(500 * time.Millisecond)
	store := NewSnapshotStore(js)
	run, loadErr := store.Load("run-nr-1")
	if loadErr != nil {
		t.Fatalf("load run after fail: %v", loadErr)
	}

	// Positive: non-retriable should fail the workflow
	// immediately despite 5 max retries configured.
	if run.Status != dag.RunStatusFailed {
		t.Fatalf(
			"run status = %s, want failed", run.Status,
		)
	}

	// Negative: should have only 1 attempt (no retries).
	stepState := run.Steps["a"]
	if stepState.Attempts != 1 {
		t.Fatalf("attempts = %d, want 1", stepState.Attempts)
	}
}

func TestRetryAfterSchedulesExactDelay(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "test-ra", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  3,
			Strategy:     dag.RetryFixed,
			InitialDelay: time.Minute,
		},
		Steps: []dag.StepDef{
			{ID: "a", Task: "task-ra", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	startPayload, _ := json.Marshal(map[string]any{
		"workflow_def": wfDef,
	})
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-ra-1", startPayload,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	time.Sleep(500 * time.Millisecond)

	failPayload, _ := json.Marshal(protocol.StepFailedPayload{
		Error:        "rate limited",
		FailureType:  protocol.FailureTypeRetryAfter,
		RetryAfterMs: 200,
	})
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "run-ra-1", "a", failPayload,
	)
	failData, _ := failEvt.Marshal()
	js.Publish(failEvt.NATSSubject(), failData,
		nats.MsgId(failEvt.NATSMsgID()))

	// The task should be re-published after ~200ms via SLEEP_TIMERS.
	sub, _ := js.PullSubscribe(
		"task.task-ra.*", "",
		nats.BindStream("TASK_QUEUES"),
	)
	// Skip initial enqueue
	msgs, fetchErr := sub.Fetch(1, nats.MaxWait(2*time.Second))
	if fetchErr != nil {
		t.Fatalf("initial task not received: %v", fetchErr)
	}
	if len(msgs) != 1 {
		t.Fatalf("expected 1 initial task, got %d", len(msgs))
	}

	// Second message = retry after timer fired
	retryMsgs, retryErr := sub.Fetch(1, nats.MaxWait(5*time.Second))
	if retryErr != nil {
		t.Fatalf("retry task not received within 5s: %v", retryErr)
	}
	if len(retryMsgs) != 1 {
		t.Fatalf("expected 1 retry task, got %d", len(retryMsgs))
	}

	// Verify run is NOT failed (retries remain)
	time.Sleep(100 * time.Millisecond)
	store := NewSnapshotStore(js)
	run, loadErr := store.Load("run-ra-1")
	if loadErr != nil {
		t.Fatalf("load run: %v", loadErr)
	}
	if run.Status == dag.RunStatusFailed {
		t.Fatal("run should not be failed — retries remain")
	}
}

func TestOldStringPayloadTreatedAsRetriable(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := nc.JetStream()

	wfDef := dag.WorkflowDef{
		Name: "test-compat", Version: "1",
		DefaultRetry: &dag.RetryPolicy{
			MaxAttempts:  3,
			Strategy:     dag.RetryFixed,
			InitialDelay: time.Second,
		},
		Steps: []dag.StepDef{
			{
				ID:   "a",
				Task: "task-a",
				Type: dag.StepTypeNormal,
			},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData, _ := json.Marshal(wfDef)
	defKV.Put(wfDef.Name, defData)

	orch := NewOrchestrator(nc, observe.NewNoopTelemetry())
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		"run-compat",
		defData,
	)
	startData, _ := startEvt.Marshal()
	js.Publish(
		startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)

	oldPayload := []byte(`"transient error"`)
	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed,
		"run-compat", "a",
		oldPayload,
	)
	failData, _ := failEvt.Marshal()
	js.Publish(
		failEvt.NATSSubject(), failData,
		nats.MsgId(failEvt.NATSMsgID()),
	)

	time.Sleep(500 * time.Millisecond)
	store := NewSnapshotStore(js)
	run, loadErr := store.Load("run-compat")
	if loadErr != nil {
		t.Fatalf("load run: %v", loadErr)
	}

	// Positive: old format should be treated as retriable,
	// not cause immediate permanent failure.
	if run.Status == dag.RunStatusFailed {
		t.Fatal(
			"old format payload should be retriable, " +
				"not permanent",
		)
	}

	// Negative: should have recorded exactly 1 attempt.
	stepState := run.Steps["a"]
	if stepState.Attempts != 1 {
		t.Fatalf(
			"attempts = %d, want 1", stepState.Attempts,
		)
	}
}
