// engine/orchestrator_lifecycle_test.go
// Lifecycle tests for the orchestrator: run start/advance, completion,
// failure, cancellation, timeouts, terminal timestamp stamping, snapshot
// on completion/restore, malformed/unknown events, and traceparent
// persistence. Uses real embedded NATS server.
// Methodology: publish events to the history stream, let the orchestrator
// process them, then verify run KV state, terminal timestamps, and task
// subjects. Each test gets its own embedded server.

package engine

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
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
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	evt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-1", defData)
	evtData, err := evt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, evt.NATSSubject(), evtData, nats.MsgId(evt.NATSMsgID()))

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
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-2", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

	subA, _ := js.PullSubscribe("task.task-a.*", "", nats.BindStream("TASK_QUEUES"))
	msgsA, err := subA.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch task-a failed: %v", err)
	}
	msgsA[0].Ack()

	compEvt := protocol.NewStepEvent(protocol.EventStepCompleted, "run-2", "a", []byte(`"done"`))
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData, nats.MsgId(compEvt.NATSMsgID()))

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
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{Name: "loop-wf", Version: "1", Steps: []dag.StepDef{
		{
			ID:     "loop-step",
			Task:   "agent-task",
			Type:   dag.StepTypeAgentLoop,
			Config: dag.MarshalConfig(&dag.AgentLoopConfig{MaxIterations: 2}),
		},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start the workflow — iteration 0 task should be published.
	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-iter", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

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
	cont1Data, err := cont1.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, cont1.NATSSubject(), cont1Data, nats.MsgId(cont1.NATSMsgID()))

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
	cont2Data, err := cont2.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, cont2.NATSSubject(), cont2Data,
		nats.MsgId("run-iter.loop-step.step.continue.2"))

	// Wait until orchestrator marks the run Failed.
	waitForRunStatus(t, orch.store, "run-iter",
		dag.RunStatusFailed, 5*time.Second)

	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "run-iter")
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
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{Name: "dur-wf", Version: "1", Steps: []dag.StepDef{
		{
			ID:     "dur-step",
			Task:   "dur-task",
			Type:   dag.StepTypeAgentLoop,
			Config: dag.MarshalConfig(&dag.AgentLoopConfig{MaxIterations: 100, MaxDuration: 1 * time.Millisecond}),
		},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-dur", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

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
	cont1Data, err := cont1.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, cont1.NATSSubject(), cont1Data, nats.MsgId(cont1.NATSMsgID()))

	msgs2, err := taskSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch iteration-1 task failed: %v", err)
	}
	msgs2[0].Ack()

	// Sleep well past the 1ms MaxDuration.
	time.Sleep(50 * time.Millisecond)

	// Second continue should trip MaxDuration.
	cont2 := protocol.NewStepEvent(protocol.EventStepContinue, "run-dur", "dur-step", nil)
	cont2Data, err := cont2.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, cont2.NATSSubject(), cont2Data,
		nats.MsgId("run-dur.dur-step.step.continue.2"))

	waitForRunStatus(t, orch.store, "run-dur",
		dag.RunStatusFailed, 5*time.Second)

	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "run-dur")
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
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{Name: "single-step", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-3", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData, nats.MsgId(startEvt.NATSMsgID()))

	waitForStepStatus(t, orch.store, "run-3", "a",
		dag.StepStatusQueued, 5*time.Second)
	compEvt := protocol.NewStepEvent(protocol.EventStepCompleted, "run-3", "a", []byte(`"done"`))
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData, nats.MsgId(compEvt.NATSMsgID()))

	waitForRunStatus(t, orch.store, "run-3",
		dag.RunStatusCompleted, 5*time.Second)
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "run-3")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if run.Status != dag.RunStatusCompleted {
		t.Fatalf("Status = %v, want Completed", run.Status)
	}
}

func TestOrchestratorPersistsTraceParentOnStart(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{Name: "tp-step", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	const tp = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-tp", defData)
	startEvt.TraceParent = tp
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	waitForStepStatus(t, orch.store, "run-tp", "a",
		dag.StepStatusQueued, 5*time.Second)
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "run-tp")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	// Positive: the run carries the inbound traceparent.
	if run.TraceParent != tp {
		t.Fatalf("TraceParent = %q, want %q", run.TraceParent, tp)
	}
}

func TestOrchestratorEmptyTraceParentStaysEmpty(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{Name: "tp-empty", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-tpe", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	waitForStepStatus(t, orch.store, "run-tpe", "a",
		dag.StepStatusQueued, 5*time.Second)
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "run-tpe")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	// Negative: no inbound traceparent => no fabricated trace context.
	if run.TraceParent != "" {
		t.Fatalf("TraceParent = %q, want empty", run.TraceParent)
	}
}

func TestOrchestratorSetsCompletedAtOnCompletion(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{Name: "ca-step", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-ca", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	waitForStepStatus(t, orch.store, "run-ca", "a",
		dag.StepStatusQueued, 5*time.Second)
	compEvt := protocol.NewStepEvent(protocol.EventStepCompleted, "run-ca", "a", []byte(`"done"`))
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))

	waitForRunStatus(t, orch.store, "run-ca",
		dag.RunStatusCompleted, 5*time.Second)
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "run-ca")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	// Positive: completion stamps CompletedAt after CreatedAt.
	if run.CompletedAt == nil {
		t.Fatal("CompletedAt = nil, want non-nil on completed run")
	}
	if !run.CompletedAt.After(run.CreatedAt) {
		t.Fatalf("CompletedAt %v not after CreatedAt %v",
			run.CompletedAt, run.CreatedAt)
	}
}

func TestOrchestratorSetsCompletedAtOnFailure(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// No retry policy: a single step failure fails the run permanently.
	wfDef := dag.WorkflowDef{Name: "ca-fail", Version: "1", Steps: []dag.StepDef{
		{ID: "a", Task: "task-a", Type: dag.StepTypeNormal},
	}}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(protocol.EventWorkflowStarted, "run-cf", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	waitForStepStatus(t, orch.store, "run-cf", "a",
		dag.StepStatusQueued, 5*time.Second)
	startedEvt := protocol.NewStepEvent(protocol.EventStepStarted, "run-cf", "a", nil)
	startedEvt.AttemptNumber = 1
	startedData, err := startedEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startedEvt.NATSSubject(), startedData,
		nats.MsgId(startedEvt.NATSMsgID()))
	time.Sleep(50 * time.Millisecond)
	failEvt := protocol.NewStepEvent(protocol.EventStepFailed, "run-cf", "a", []byte(`"boom"`))
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, failEvt.NATSSubject(), failData,
		nats.MsgId(failEvt.NATSMsgID()))

	waitForRunStatus(t, orch.store, "run-cf",
		dag.RunStatusFailed, 5*time.Second)
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "run-cf")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	// Positive: a permanently-failed run stamps a completion timestamp so
	// the Traces list reports an honest duration for terminal-failed runs.
	if run.CompletedAt == nil {
		t.Fatal("CompletedAt = nil, want non-nil on failed run")
	}
	// Negative: the stamp falls after the run's creation, never before.
	if run.CompletedAt.Before(run.CreatedAt) {
		t.Fatalf("CompletedAt %v before CreatedAt %v",
			run.CompletedAt, run.CreatedAt)
	}
}

func TestOrchestratorCancelsRunningWorkflow(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name: "cancel-test", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "slow-task", Type: dag.StepTypeNormal},
		},
	}
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, "cancel-test", defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cancel-run-1", defData)
	data, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	msg := &nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	}
	mustPublishMsg(t, js, msg)
	waitForRunStatus(t, orch.store, "cancel-run-1",
		dag.RunStatusRunning, 5*time.Second)

	// Cancel the workflow
	cancelEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, "cancel-run-1", nil)
	cancelData, err := cancelEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	cancelMsg := &nats.Msg{
		Subject: cancelEvt.NATSSubject(),
		Data:    cancelData,
		Header:  nats.Header{"Nats-Msg-Id": {cancelEvt.NATSMsgID()}},
	}
	mustPublishMsg(t, js, cancelMsg)

	// Wait for processing
	store := NewSnapshotStore(jsNew)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), "cancel-run-1")
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

func TestOrchestratorSetsCompletedAtOnCancellation(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name: "cancel-ca", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "slow-task", Type: dag.StepTypeNormal},
		},
	}
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, "cancel-ca", defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cancel-ca-1", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: startEvt.NATSSubject(), Data: startData,
		Header: nats.Header{"Nats-Msg-Id": {startEvt.NATSMsgID()}},
	})
	waitForRunStatus(t, orch.store, "cancel-ca-1",
		dag.RunStatusRunning, 5*time.Second)

	cancelEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, "cancel-ca-1", nil)
	cancelData, err := cancelEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: cancelEvt.NATSSubject(), Data: cancelData,
		Header: nats.Header{"Nats-Msg-Id": {cancelEvt.NATSMsgID()}},
	})
	waitForRunStatus(t, orch.store, "cancel-ca-1",
		dag.RunStatusCancelled, 5*time.Second)

	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "cancel-ca-1")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	// Positive: cancellation stamps CompletedAt so a terminal-cancelled run
	// reports an honest duration rather than an asymmetric em-dash.
	if run.CompletedAt == nil {
		t.Fatal("CompletedAt = nil, want non-nil on cancelled run")
	}
	// Negative: the stamp falls after the run's creation, never before.
	if run.CompletedAt.Before(run.CreatedAt) {
		t.Fatalf("CompletedAt %v before CreatedAt %v",
			run.CompletedAt, run.CreatedAt)
	}
}

func TestOrchestratorWorkflowTimeout(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	defKV, _ := js.KeyValue("workflow_defs")

	wfDef := dag.WorkflowDef{
		Name:    "timeout-test",
		Version: "1",
		Timeout: 200 * time.Millisecond,
		Steps: []dag.StepDef{
			{ID: "slow", Task: "slow-task", Type: dag.StepTypeNormal},
		},
	}
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, "timeout-test", defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "timeout-run-1", defData)
	data, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
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
	failData, err := failEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublishMsg(t, js, &nats.Msg{
		Subject: failEvt.NATSSubject(), Data: failData,
		Header: nats.Header{"Nats-Msg-Id": {failEvt.NATSMsgID()}},
	})

	// Check that run is cancelled
	store := NewSnapshotStore(jsNew)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), "timeout-run-1")
		if err == nil && run.Status == dag.RunStatusCancelled {
			return // Positive: timed out → cancelled
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("workflow should be cancelled after timeout")
}

func TestOrchestratorSnapshotAfterCompletion(t *testing.T) {
	// Methodology: after step completion, read KV directly and
	// verify snapshot state matches expectations.
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
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "snap-run", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))
	waitForStepStatus(t, orch.store, "snap-run", "s1",
		dag.StepStatusQueued, 5*time.Second)

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "snap-run", "s1",
		[]byte(`"result-1"`))
	compData, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), compData,
		nats.MsgId(compEvt.NATSMsgID()))
	waitForStepStatus(t, orch.store, "snap-run", "s1",
		dag.StepStatusCompleted, 5*time.Second)

	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "snap-run")
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
	// Positive: s2 must have been advanced to Queued (or beyond)
	// once s1 completed. Without this assertion, a regression that
	// completes s1 but never advances the DAG to s2 would still
	// pass the test (the snapshot would still reflect "s1 completed").
	s2 := run.Steps["s2"].Status
	if s2 != dag.StepStatusQueued &&
		s2 != dag.StepStatusRunning &&
		s2 != dag.StepStatusCompleted {
		t.Fatalf("s2 = %v, want Queued/Running/Completed after s1 completed", s2)
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
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

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
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	store := NewSnapshotStore(jsNew)
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
	if err := store.Save(context.Background(), crafted); err != nil {
		t.Fatalf("Save crafted: %v", err)
	}

	orch := NewOrchestrator(nc)
	wfDefR, runR, err := orch.loadRunAndDef(context.Background(), "crafted-run")
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
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Publish with traceparent header.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "trace-run", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	tp := "00-aaaa-bbbb-01"
	msg := &nats.Msg{
		Subject: startEvt.NATSSubject(),
		Data:    startData,
		Header: nats.Header{
			"Nats-Msg-Id": {startEvt.NATSMsgID()},
			"traceparent": {tp},
		},
	}
	mustPublishMsg(t, js, msg)

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

func TestOrchestratorCancelNonRunningIsNoop(t *testing.T) {
	// Methodology: cancel an already-completed run. The handler
	// should return nil without modifying the run.
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
		Name: "cancel-noop", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t1", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start and complete the workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "cnoop-run", defData)
	sd, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), sd,
		nats.MsgId(startEvt.NATSMsgID()))
	waitForStepStatus(t, orch.store, "cnoop-run", "s1",
		dag.StepStatusQueued, 5*time.Second)

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "cnoop-run", "s1",
		[]byte(`"done"`))
	cd, err := compEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, compEvt.NATSSubject(), cd,
		nats.MsgId(compEvt.NATSMsgID()))

	store := NewSnapshotStore(jsNew)
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), "cnoop-run")
		if err == nil &&
			run.Status == dag.RunStatusCompleted {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Now cancel the completed workflow.
	cancelEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowCancelled, "cnoop-run", nil)
	ccd, err := cancelEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, cancelEvt.NATSSubject(), ccd,
		nats.MsgId(cancelEvt.NATSMsgID()))
	time.Sleep(300 * time.Millisecond)

	// Positive: run is still Completed (not Cancelled).
	run, _ := store.Load(context.Background(), "cnoop-run")
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
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "input-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-in",
				Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Start with structured payload.
	payload := mustMarshal(t, map[string]any{
		"workflow_def": wfDef,
		"input":        map[string]string{"key": "val"},
	})
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "input-run", payload)
	sd, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), sd,
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
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "input-run")
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
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "malform-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t1", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Publish garbage data to history stream.
	mustPublish(t, js, "history.malform-run",
		[]byte("not valid json"),
		nats.MsgId("malform-1"))

	time.Sleep(200 * time.Millisecond)

	// Positive: orchestrator survives and processes next event.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "recover-run", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	store := NewSnapshotStore(jsNew)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		run, err := store.Load(context.Background(), "recover-run")
		if err == nil && run.Status == dag.RunStatusRunning {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	// Positive: orchestrator recovered from malformed event.
	t.Fatalf("orchestrator should recover from malformed event")
}

func TestOrchestratorStopIdempotent(t *testing.T) {
	// Methodology: verify Stop can be called multiple times
	// and on a never-started orchestrator without panic.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	orch := NewOrchestrator(nc)

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
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	wfDef := dag.WorkflowDef{
		Name: "unknown-evt", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t1", Type: dag.StepTypeNormal},
		},
	}
	defKV, _ := js.KeyValue("workflow_defs")
	defData := mustMarshal(t, wfDef)
	mustPut(t, defKV, wfDef.Name, defData)

	orch := NewOrchestrator(nc)
	orch.Start()
	defer orch.Stop()

	// Publish an event with an unhandled type.
	evt := protocol.Event{
		Type:  "custom.unknown",
		RunID: "unknown-run",
	}
	data := mustMarshal(t, evt)
	mustPublish(t, js, "history.unknown-run", data,
		nats.MsgId("unknown-run.custom"))
	time.Sleep(200 * time.Millisecond)

	// Positive: no run was created (event was ignored).
	store := NewSnapshotStore(jsNew)
	_, err = store.Load(context.Background(), "unknown-run")
	if err == nil {
		t.Fatal("unknown event should not create a run")
	}

	// Positive: subsequent events still work.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "post-unknown",
		defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))
	waitForRunStatus(t, orch.store, "post-unknown",
		dag.RunStatusRunning, 5*time.Second)
	_, err = store.Load(context.Background(), "post-unknown")
	if err != nil {
		t.Fatalf("orchestrator should still work: %v", err)
	}
}

func TestMarkTerminalStampsStatusAndCompletedAt(t *testing.T) {
	// Methodology: pure unit test for the single terminal-transition
	// funnel. Every terminal status must set both Status and a non-nil
	// CompletedAt; a non-terminal status must panic so no caller can
	// route a still-running transition through the helper.
	terminal := []dag.RunStatus{
		dag.RunStatusCompleted,
		dag.RunStatusFailed,
		dag.RunStatusCancelled,
		dag.RunStatusCompensated,
	}
	for _, status := range terminal {
		run := markTerminal(
			dag.WorkflowRun{RunID: "r1"}, status,
		)
		// Positive: status applied and CompletedAt stamped.
		if run.Status != status {
			t.Fatalf("Status = %v, want %v", run.Status, status)
		}
		if run.CompletedAt == nil {
			t.Fatalf("CompletedAt = nil for terminal status %v", status)
		}
	}

	// Negative: a non-terminal status panics (programmer error).
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("markTerminal(Running) should panic")
			}
		}()
		markTerminal(dag.WorkflowRun{RunID: "r1"}, dag.RunStatusRunning)
	}()

	// Negative: an empty RunID panics.
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("markTerminal with empty RunID should panic")
			}
		}()
		markTerminal(dag.WorkflowRun{}, dag.RunStatusCompleted)
	}()
}

func TestOrchestratorLoopStepFailureStampsCompletedAt(t *testing.T) {
	// Methodology: an agent-loop step with MaxIterations=1 fails the
	// workflow on the first step.continue (iterations 1 >= max). This
	// drives the failLoopStep terminal path. Verify the failed run
	// stamps CompletedAt so the Traces "Duration" is honest.
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
		Name: "loop-fail", Version: "1",
		Steps: []dag.StepDef{{
			ID:   "looped",
			Task: "loop-task",
			Type: dag.StepTypeAgentLoop,
			Config: dag.MarshalConfig(&dag.AgentLoopConfig{
				MaxIterations: 1,
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
		protocol.EventWorkflowStarted, "loop-fail-run", defData)
	startData, err := startEvt.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, startEvt.NATSSubject(), startData,
		nats.MsgId(startEvt.NATSMsgID()))

	// Drain the initial loop task.
	taskSub, _ := js.PullSubscribe(
		"task.loop-task.*", "", nats.BindStream("TASK_QUEUES"))
	msgs, err := taskSub.Fetch(1, nats.MaxWait(5*time.Second))
	if err != nil {
		t.Fatalf("Fetch initial loop task failed: %v", err)
	}
	msgs[0].Ack()

	// step.continue pushes iterations to 1, exceeding MaxIterations=1.
	cont := protocol.NewStepEvent(
		protocol.EventStepContinue, "loop-fail-run", "looped", nil)
	contData, err := cont.Marshal()
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	mustPublish(t, js, cont.NATSSubject(), contData,
		nats.MsgId(cont.NATSMsgID()))

	waitForRunStatus(t, orch.store, "loop-fail-run",
		dag.RunStatusFailed, 5*time.Second)
	store := NewSnapshotStore(jsNew)
	run, err := store.Load(context.Background(), "loop-fail-run")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	// Positive: a loop-step failure is terminal, so CompletedAt is
	// stamped — the Traces "Duration" must not render an em-dash.
	if run.CompletedAt == nil {
		t.Fatal("CompletedAt = nil, want non-nil on loop-failed run")
	}
	// Negative: the stamp never precedes the run's creation.
	if run.CompletedAt.Before(run.CreatedAt) {
		t.Fatalf("CompletedAt %v before CreatedAt %v",
			run.CompletedAt, run.CreatedAt)
	}
}
