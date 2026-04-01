package engine

// Methodology: test WorkflowActor event handling in isolation using
// the actor runtime directly. No NATS — tests inject messages manually.

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/actor"
	"github.com/danmestas/dagnats/dag"
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

	wa := NewWorkflowActor("run-1", nil)
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

	wa := NewWorkflowActor("run-2", nil)
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
