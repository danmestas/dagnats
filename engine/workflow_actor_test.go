package engine

// Methodology: test WorkflowActor event handling in isolation using
// the actor runtime directly, plus integration tests with real NATS
// for SnapshotStore persistence. Messages are injected manually.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/actor"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/protocol"
)

func TestWorkflowActorHandlesStarted(t *testing.T) {
	wfDef := dag.WorkflowDef{
		Name:    "test-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)

	wa := NewWorkflowActor("run-1", nil, nil)
	rt := actor.NewRuntime()
	defer rt.StopAll()

	addr := actor.Address{Type: "workflow", ID: "run-1"}
	if err := rt.Spawn(addr, wa); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// Send workflow.started event
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-1", defData)
	rt.Send(addr, actor.Message{Payload: evt})

	// Wait for processing
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wa.RunStatus() != dag.RunStatusPending {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Positive: run status is Running
	if wa.RunStatus() != dag.RunStatusRunning {
		t.Fatalf("status = %v, want Running", wa.RunStatus())
	}

	// Positive: step s1 is Queued
	state := wa.StepState("s1")
	if state.Status != dag.StepStatusQueued {
		t.Fatalf("step s1 status = %v, want Queued", state.Status)
	}
}

func TestWorkflowActorHandlesStepCompleted(t *testing.T) {
	wfDef := dag.WorkflowDef{
		Name:    "test-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)

	wa := NewWorkflowActor("run-2", nil, nil)
	rt := actor.NewRuntime()
	defer rt.StopAll()

	addr := actor.Address{Type: "workflow", ID: "run-2"}
	rt.Spawn(addr, wa)

	// Start the workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-2", defData)
	rt.Send(addr, actor.Message{Payload: startEvt})
	time.Sleep(50 * time.Millisecond)

	// Complete step s1
	completeEvt := protocol.NewWorkflowEvent(
		protocol.EventStepCompleted, "run-2", []byte(`"done"`))
	completeEvt.StepID = "s1"
	rt.Send(addr, actor.Message{Payload: completeEvt})

	// Wait for processing
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wa.RunStatus() == dag.RunStatusCompleted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Positive: run is completed
	if wa.RunStatus() != dag.RunStatusCompleted {
		t.Fatalf("status = %v, want Completed", wa.RunStatus())
	}

	// Positive: step has output
	state := wa.StepState("s1")
	if state.Status != dag.StepStatusCompleted {
		t.Fatalf("step status = %v, want Completed", state.Status)
	}
}

func TestWorkflowActorHandlesStepFailed(t *testing.T) {
	wfDef := dag.WorkflowDef{
		Name:    "fail-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)

	wa := NewWorkflowActor("run-fail", nil, nil)
	rt := actor.NewRuntime()
	defer rt.StopAll()

	addr := actor.Address{Type: "workflow", ID: "run-fail"}
	if err := rt.Spawn(addr, wa); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// Start the workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-fail", defData)
	rt.Send(addr, actor.Message{Payload: startEvt})
	time.Sleep(50 * time.Millisecond)

	// Fail step s1
	failEvt := protocol.NewWorkflowEvent(
		protocol.EventStepFailed, "run-fail",
		[]byte(`"step crashed"`))
	failEvt.StepID = "s1"
	rt.Send(addr, actor.Message{Payload: failEvt})

	// Wait for processing
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wa.RunStatus() == dag.RunStatusFailed {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Positive: run is failed
	if wa.RunStatus() != dag.RunStatusFailed {
		t.Fatalf("status = %v, want Failed", wa.RunStatus())
	}

	// Positive: step has error message
	state := wa.StepState("s1")
	if state.Status != dag.StepStatusFailed {
		t.Fatalf("step status = %v, want Failed", state.Status)
	}
}

func TestWorkflowActorHandlesStepContinue(t *testing.T) {
	wfDef := dag.WorkflowDef{
		Name:    "loop-wf",
		Version: "1",
		Steps: []dag.StepDef{{
			ID:   "loop",
			Task: "loop-task",
			Type: dag.StepTypeAgentLoop,
			Loop: &dag.AgentLoopConfig{MaxIterations: 5},
		}},
	}
	defData, _ := json.Marshal(wfDef)

	wa := NewWorkflowActor("run-loop", nil, nil)
	rt := actor.NewRuntime()
	defer rt.StopAll()

	addr := actor.Address{Type: "workflow", ID: "run-loop"}
	if err := rt.Spawn(addr, wa); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// Start the workflow
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-loop", defData)
	rt.Send(addr, actor.Message{Payload: startEvt})
	time.Sleep(50 * time.Millisecond)

	// Send step.continue event
	contEvt := protocol.NewStepEvent(
		protocol.EventStepContinue, "run-loop", "loop", nil)
	rt.Send(addr, actor.Message{Payload: contEvt})
	time.Sleep(50 * time.Millisecond)

	// Positive: iteration count incremented to 1
	state := wa.StepState("loop")
	if state.Iterations != 1 {
		t.Fatalf("Iterations = %d, want 1", state.Iterations)
	}

	// Positive: run is still Running (not completed)
	if wa.RunStatus() != dag.RunStatusRunning {
		t.Fatalf("status = %v, want Running", wa.RunStatus())
	}
}

func TestWorkflowActorStepCompletedAdvancesDAG(t *testing.T) {
	// Workflow: s1 -> s2. Complete s1, verify s2 becomes Queued.
	wfDef := dag.WorkflowDef{
		Name:    "chain-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{
				ID:   "s1",
				Task: "task-a",
				Type: dag.StepTypeNormal,
			},
			{
				ID:        "s2",
				Task:      "task-b",
				DependsOn: []string{"s1"},
				Type:      dag.StepTypeNormal,
			},
		},
	}
	defData, _ := json.Marshal(wfDef)

	wa := NewWorkflowActor("run-chain", nil, nil)
	rt := actor.NewRuntime()
	defer rt.StopAll()

	addr := actor.Address{Type: "workflow", ID: "run-chain"}
	if err := rt.Spawn(addr, wa); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// Start the workflow — only s1 should be Queued.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-chain", defData)
	rt.Send(addr, actor.Message{Payload: startEvt})
	time.Sleep(50 * time.Millisecond)

	// Confirm s1 is Queued, s2 is Pending.
	if wa.StepState("s1").Status != dag.StepStatusQueued {
		t.Fatalf("s1 should be Queued, got %v",
			wa.StepState("s1").Status)
	}
	if wa.StepState("s2").Status != dag.StepStatusPending {
		t.Fatalf("s2 should be Pending before s1 completes, got %v",
			wa.StepState("s2").Status)
	}

	// Complete s1 — s2 should become Queued.
	compEvt := protocol.NewWorkflowEvent(
		protocol.EventStepCompleted, "run-chain",
		[]byte(`"done"`))
	compEvt.StepID = "s1"
	rt.Send(addr, actor.Message{Payload: compEvt})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wa.StepState("s2").Status == dag.StepStatusQueued {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Positive: s2 is now Queued.
	if wa.StepState("s2").Status != dag.StepStatusQueued {
		t.Fatalf("s2 status = %v, want Queued",
			wa.StepState("s2").Status)
	}
	// Positive: run is still Running (s2 not yet complete).
	if wa.RunStatus() != dag.RunStatusRunning {
		t.Fatalf("status = %v, want Running", wa.RunStatus())
	}
}

func TestWorkflowActorFailedBeforeStart(t *testing.T) {
	// Methodology: sending step.failed before workflow.started
	// must return error (not panic).
	wa := NewWorkflowActor("run-nostart", nil, nil)

	failEvt := protocol.NewStepEvent(
		protocol.EventStepFailed, "run-nostart", "s1",
		[]byte(`"err"`))
	err := wa.Receive(
		&actor.Context{},
		actor.Message{Payload: failEvt},
	)
	// Positive: error returned because workflow not started.
	if err == nil {
		t.Fatal("expected error for fail before start")
	}
	// Positive: run stays Pending.
	if wa.RunStatus() != dag.RunStatusPending {
		t.Fatalf("status = %v, want Pending", wa.RunStatus())
	}
}

func TestWorkflowActorContinueBeforeStart(t *testing.T) {
	// Methodology: sending step.continue before workflow.started
	// must return error.
	wa := NewWorkflowActor("run-nostart2", nil, nil)

	contEvt := protocol.NewStepEvent(
		protocol.EventStepContinue, "run-nostart2", "s1", nil)
	err := wa.Receive(
		&actor.Context{},
		actor.Message{Payload: contEvt},
	)
	// Positive: error returned.
	if err == nil {
		t.Fatal("expected error for continue before start")
	}
	// Positive: run stays Pending.
	if wa.RunStatus() != dag.RunStatusPending {
		t.Fatalf("status = %v, want Pending", wa.RunStatus())
	}
}

func TestWorkflowActorCompletedBeforeStart(t *testing.T) {
	// Methodology: sending step.completed before workflow.started
	// must return error.
	wa := NewWorkflowActor("run-nostart3", nil, nil)

	compEvt := protocol.NewStepEvent(
		protocol.EventStepCompleted, "run-nostart3", "s1",
		[]byte(`"done"`))
	err := wa.Receive(
		&actor.Context{},
		actor.Message{Payload: compEvt},
	)
	// Positive: error returned.
	if err == nil {
		t.Fatal("expected error for completed before start")
	}
	// Positive: run stays Pending.
	if wa.RunStatus() != dag.RunStatusPending {
		t.Fatalf("status = %v, want Pending", wa.RunStatus())
	}
}

func TestWorkflowActorUnhandledEventType(t *testing.T) {
	// Methodology: sending an unhandled event type must not
	// error or panic, just return nil.
	wfDef := dag.WorkflowDef{
		Name: "unk-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "t", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)

	wa := NewWorkflowActor("run-unk", nil, nil)
	rt := actor.NewRuntime()
	defer rt.StopAll()

	addr := actor.Address{Type: "workflow", ID: "run-unk"}
	if err := rt.Spawn(addr, wa); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// Start workflow first.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-unk", defData)
	rt.Send(addr, actor.Message{Payload: startEvt})
	time.Sleep(50 * time.Millisecond)

	// Send unhandled event type.
	unkEvt := protocol.Event{
		Type:  "custom.unknown",
		RunID: "run-unk",
	}
	err := wa.Receive(
		&actor.Context{},
		actor.Message{Payload: unkEvt},
	)
	// Positive: no error returned.
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Positive: status unchanged (still Running).
	if wa.RunStatus() != dag.RunStatusRunning {
		t.Fatalf("status = %v, want Running", wa.RunStatus())
	}
}

func TestWorkflowActorWithSnapshotStore(t *testing.T) {
	// Methodology: create a WorkflowActor with a real NATS
	// SnapshotStore. Verify state is persisted to KV.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	js, _ := nc.JetStream()
	store := NewSnapshotStore(js)

	wfDef := dag.WorkflowDef{
		Name: "persist-wf", Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "ta", Type: dag.StepTypeNormal},
		},
	}
	defData, _ := json.Marshal(wfDef)

	wa := NewWorkflowActor("persist-run", store, nil)
	rt := actor.NewRuntime()
	defer rt.StopAll()

	addr := actor.Address{Type: "workflow", ID: "persist-run"}
	if err := rt.Spawn(addr, wa); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// Start workflow.
	startEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "persist-run", defData)
	rt.Send(addr, actor.Message{Payload: startEvt})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wa.RunStatus() == dag.RunStatusRunning {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Positive: run persisted to KV.
	run, err := store.Load("persist-run")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if run.Status != dag.RunStatusRunning {
		t.Fatalf("status = %v, want Running", run.Status)
	}

	// Complete the step.
	compEvt := protocol.NewWorkflowEvent(
		protocol.EventStepCompleted, "persist-run",
		[]byte(`"ok"`))
	compEvt.StepID = "s1"
	rt.Send(addr, actor.Message{Payload: compEvt})

	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wa.RunStatus() == dag.RunStatusCompleted {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Positive: completed state persisted to KV.
	run2, err := store.Load("persist-run")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if run2.Status != dag.RunStatusCompleted {
		t.Fatalf("status = %v, want Completed", run2.Status)
	}
}

func TestWorkflowActorHandlesEnvelopePayload(t *testing.T) {
	// Methodology: the API sends started events with an envelope
	// {"workflow_def": {...}, "input": {...}}. The actor must
	// unwrap this correctly instead of silently producing a
	// zero-value WorkflowDef.
	wfDef := dag.WorkflowDef{
		Name:    "envelope-wf",
		Version: "1",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task-a", Type: dag.StepTypeNormal},
		},
	}
	envelope, _ := json.Marshal(map[string]interface{}{
		"workflow_def": wfDef,
		"input":        map[string]string{"key": "val"},
	})

	wa := NewWorkflowActor("run-env", nil, nil)
	rt := actor.NewRuntime()
	defer rt.StopAll()

	addr := actor.Address{Type: "workflow", ID: "run-env"}
	if err := rt.Spawn(addr, wa); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, "run-env", envelope)
	rt.Send(addr, actor.Message{Payload: evt})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if wa.RunStatus() != dag.RunStatusPending {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Positive: run status is Running (not panic/zero-value)
	if wa.RunStatus() != dag.RunStatusRunning {
		t.Fatalf("status = %v, want Running", wa.RunStatus())
	}

	// Positive: step s1 is Queued
	state := wa.StepState("s1")
	if state.Status != dag.StepStatusQueued {
		t.Fatalf("step s1 = %v, want Queued", state.Status)
	}
}

func TestWorkflowActorReceiveRejectsNonEvent(t *testing.T) {
	wa := NewWorkflowActor("run-bad", nil, nil)
	rt := actor.NewRuntime()
	defer rt.StopAll()

	addr := actor.Address{Type: "workflow", ID: "run-bad"}
	if err := rt.Spawn(addr, wa); err != nil {
		t.Fatalf("spawn: %v", err)
	}

	// Send a non-Event message directly via Receive.
	err := wa.Receive(
		&actor.Context{},
		actor.Message{Payload: "not-an-event"},
	)
	// Positive: error returned for wrong type.
	if err == nil {
		t.Fatal("expected error for non-Event message")
	}
	// Positive: run stays Pending (unchanged).
	if wa.RunStatus() != dag.RunStatusPending {
		t.Fatalf("status = %v, want Pending", wa.RunStatus())
	}
}
