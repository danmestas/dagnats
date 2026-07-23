// engine/orchestrator_step_test.go
// Step-execution tests for the orchestrator: agent-step stream routing,
// worker-group routing, step-continue republication (with and without loop
// delay), skip-if evaluation, and input-schema validation. Uses real
// embedded NATS server.
// Methodology: publish events to the history stream, let the orchestrator
// process them, then verify tasks appear on the correct subjects and KV
// state advances. Each test gets its own embedded server.

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
	defData := mustMarshal(t, wfDef)
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
	orch := NewOrchestrator(nc,
		WithStepRoutes(routes))
	orch.Start()
	defer orch.Stop()

	// Publish workflow.started event
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-route-1", defData)
	data, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), data,
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
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start the workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "gpu-run-1", defData,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

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
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	wfDef := dag.WorkflowDef{
		Name: "cont-wf", Version: "1",
		Steps: []dag.StepDef{{
			ID: "agent", Task: "agent-task",
			Type:   dag.StepTypeAgentLoop,
			Config: dag.MarshalConfig(&dag.AgentLoopConfig{MaxIterations: 3}),
		}},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cont-run", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
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
	contData, err := cont.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, cont.NATSSubject(), contData,
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
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "cont-run")
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
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := skipIfWorkflow()
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "skip-run", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
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
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
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
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "skip-run")
	if err != nil {
		t.Fatalf("Load run failed: %v", err)
	}
	if run.Steps["b"].Status != dag.StepStatusSkipped {
		t.Fatalf("b = %v, want Skipped",
			run.Steps["b"].Status)
	}
}

func TestOrchestratorInputSchemaValidation(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

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
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Positive: valid input with required "repo" field
	validInput := json.RawMessage(`{"repo": "github.com/test/repo"}`)
	startPayload := mustMarshal(t, map[string]any{
		"workflow_def": wfDef,
		"input":        validInput,
	})
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "valid-run", startPayload,
	)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js,
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
	invalidPayload := mustMarshal(t, map[string]any{
		"workflow_def": wfDef,
		"input":        invalidInput,
	})
	invalidEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "invalid-run", invalidPayload,
	)
	invalidData, err := invalidEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js,
		invalidEvt.NATSSubject(), invalidData,
		nats.MsgId(invalidEvt.NATSMsgID()),
	)

	// Wait until run reaches Failed state.
	waitForRunStatus(t, orch.store, "invalid-run",
		dag.RunStatusFailed, 5*time.Second)

	// Check that the run exists but is marked as failed
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "invalid-run")
	if err != nil {
		t.Fatalf("failed run should exist in snapshot: %v", err)
	}
	if run.Status != dag.RunStatusFailed {
		t.Fatalf(
			"expected RunStatusFailed for invalid input, got %s",
			run.Status,
		)
	}
	// Positive: a schema-validation failure is terminal, so the run
	// stamps CompletedAt — the Traces "Duration" must not render an
	// em-dash for a run that has finished.
	if run.CompletedAt == nil {
		t.Fatal("CompletedAt = nil, want non-nil on schema-failed run")
	}
	// Negative: the stamp never precedes the run's creation.
	if run.CompletedAt.Before(run.CreatedAt) {
		t.Fatalf("CompletedAt %v before CreatedAt %v",
			run.CompletedAt, run.CreatedAt)
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
			Config: dag.MarshalConfig(&dag.AgentLoopConfig{
				MaxIterations: 5,
				LoopDelay:     50 * time.Millisecond,
			}),
		}},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "delay-run", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
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
	contData, err := cont.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, cont.NATSSubject(), contData,
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
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
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
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "skipall-run", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
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
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	store := NewSnapshotStore(jsNew)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), "skipall-run")
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
