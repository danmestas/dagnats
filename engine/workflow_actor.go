package engine

import (
	"encoding/json"
	"fmt"
	"sync"

	"github.com/danmestas/dagnats/actor"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
)

// WorkflowActor manages one workflow run as a supervised actor.
// State lives in memory — snapshots save to KV for durability but
// loads only happen on actor start (recovery).
type WorkflowActor struct {
	runID string
	def   *dag.WorkflowDef
	run   *dag.WorkflowRun
	store *SnapshotStore // nil in unit tests
	mu    sync.RWMutex   // protects read access to run state
}

// NewWorkflowActor creates a workflow actor for the given run.
// store may be nil for testing without NATS.
func NewWorkflowActor(
	runID string, store *SnapshotStore,
) *WorkflowActor {
	if runID == "" {
		panic("NewWorkflowActor: runID must not be empty")
	}
	return &WorkflowActor{
		runID: runID,
		store: store,
	}
}

// Receive processes workflow events from the actor mailbox.
func (wa *WorkflowActor) Receive(
	ctx *actor.Context, msg actor.Message,
) error {
	evt, ok := msg.Payload.(protocol.Event)
	if !ok {
		return fmt.Errorf(
			"unexpected message type: %T", msg.Payload,
		)
	}
	return wa.handleEvent(evt)
}

// handleEvent dispatches the event to the appropriate handler.
func (wa *WorkflowActor) handleEvent(evt protocol.Event) error {
	switch evt.Type {
	case protocol.EventWorkflowStarted:
		return wa.handleStarted(evt)
	case protocol.EventStepCompleted:
		return wa.handleStepCompleted(evt)
	case protocol.EventStepFailed:
		return wa.handleStepFailed(evt)
	case protocol.EventStepContinue:
		return wa.handleStepContinue(evt)
	default:
		return nil
	}
}

func (wa *WorkflowActor) handleStarted(
	evt protocol.Event,
) error {
	var wfDef dag.WorkflowDef
	// Payload may be an envelope {"workflow_def":..., "input":...}
	// from the API, or a bare WorkflowDef (backward compat).
	var envelope struct {
		WorkflowDef json.RawMessage `json:"workflow_def"`
		Input       json.RawMessage `json:"input"`
	}
	if err := json.Unmarshal(
		evt.Payload, &envelope,
	); err == nil && envelope.WorkflowDef != nil {
		if err := json.Unmarshal(
			envelope.WorkflowDef, &wfDef,
		); err != nil {
			return fmt.Errorf("unmarshal WorkflowDef: %w", err)
		}
	} else {
		if err := json.Unmarshal(
			evt.Payload, &wfDef,
		); err != nil {
			return fmt.Errorf("unmarshal WorkflowDef: %w", err)
		}
	}
	run := dag.NewWorkflowRun(wfDef, wa.runID)
	run.Status = dag.RunStatusRunning

	wa.mu.Lock()
	wa.def = &wfDef
	wa.run = &run
	wa.mu.Unlock()

	// Resolve and queue ready steps
	completed := completedSet(run)
	queued := queuedSet(run)
	ready := dag.ResolveReady(wfDef, completed, queued)
	for _, step := range ready {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		run.Steps[step.ID] = state
	}

	return wa.saveIfStore()
}

func (wa *WorkflowActor) handleStepCompleted(
	evt protocol.Event,
) error {
	if wa.run == nil || wa.def == nil {
		return fmt.Errorf("workflow not started")
	}

	wa.mu.Lock()
	state := wa.run.Steps[evt.StepID]
	state.Status = dag.StepStatusCompleted
	state.Output = evt.Payload
	wa.run.Steps[evt.StepID] = state

	completed := completedSet(*wa.run)
	if dag.IsComplete(*wa.def, completed) {
		wa.run.Status = dag.RunStatusCompleted
		wa.mu.Unlock()
		return wa.saveIfStore()
	}

	// Resolve newly ready steps
	queued := queuedSet(*wa.run)
	ready := dag.ResolveReady(*wa.def, completed, queued)
	for _, step := range ready {
		s := wa.run.Steps[step.ID]
		s.Status = dag.StepStatusQueued
		wa.run.Steps[step.ID] = s
	}
	wa.mu.Unlock()

	return wa.saveIfStore()
}

func (wa *WorkflowActor) handleStepFailed(
	evt protocol.Event,
) error {
	if wa.run == nil {
		return fmt.Errorf("workflow not started")
	}

	wa.mu.Lock()
	state := wa.run.Steps[evt.StepID]
	state.Status = dag.StepStatusFailed
	if evt.Payload != nil {
		state.Error = string(evt.Payload)
	}
	wa.run.Steps[evt.StepID] = state
	wa.run.Status = dag.RunStatusFailed
	wa.mu.Unlock()

	return wa.saveIfStore()
}

func (wa *WorkflowActor) handleStepContinue(
	evt protocol.Event,
) error {
	if wa.run == nil || wa.def == nil {
		return fmt.Errorf("workflow not started")
	}

	wa.mu.Lock()
	state := wa.run.Steps[evt.StepID]
	state.Iterations++
	wa.run.Steps[evt.StepID] = state
	wa.mu.Unlock()

	return wa.saveIfStore()
}

// saveIfStore persists the run to KV if a store is configured.
func (wa *WorkflowActor) saveIfStore() error {
	if wa.store == nil || wa.run == nil {
		return nil
	}
	wa.mu.RLock()
	defer wa.mu.RUnlock()
	return wa.store.Save(*wa.run)
}

// RunStatus returns the current run status (thread-safe).
func (wa *WorkflowActor) RunStatus() dag.RunStatus {
	wa.mu.RLock()
	defer wa.mu.RUnlock()
	if wa.run == nil {
		return dag.RunStatusPending
	}
	return wa.run.Status
}

// StepState returns a step's current state (thread-safe).
func (wa *WorkflowActor) StepState(stepID string) dag.StepState {
	wa.mu.RLock()
	defer wa.mu.RUnlock()
	if wa.run == nil {
		return dag.StepState{}
	}
	return wa.run.Steps[stepID]
}
