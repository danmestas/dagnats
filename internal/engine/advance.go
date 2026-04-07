// advance.go
// Pure function core of the engine state machine. Advance computes the next
// WorkflowRun state and a list of side effects from a (def, run, event) triple.
// No I/O, no NATS, no KV — the Orchestrator feeds events in and executes the
// resulting effects. This separation makes state transitions unit-testable
// without infrastructure.
package engine

import (
	"fmt"

	"github.com/danmestas/dagnats/dag"
)

// EventType identifies the kind of event fed into Advance.
// Mirrors protocol.EventType but decoupled from wire format.
type EventType string

const (
	EventStepCompleted EventType = "step.completed"
	EventStepFailed    EventType = "step.failed"
	EventStepContinue  EventType = "step.continue"
)

// FailureType mirrors protocol.FailureType for the pure core.
type FailureType string

const (
	FailureTypeRetriable    FailureType = "retriable"
	FailureTypeNonRetriable FailureType = "non_retriable"
)

// FailPayload carries structured failure info into the pure core.
type FailPayload struct {
	Error       string
	FailureType FailureType
}

// Event is the input to the pure Advance function. Carries just enough
// data to compute state transitions — no NATS headers or timestamps.
type Event struct {
	Type        EventType
	StepID      string
	Payload     []byte
	FailPayload FailPayload
}

// SideEffect is the sealed interface for effects Advance produces.
// The Orchestrator shell pattern-matches on concrete types to execute I/O.
type SideEffect interface{ sideEffect() }

// EnqueueTask tells the shell to publish a task message for a step.
type EnqueueTask struct {
	Step      dag.StepDef
	Input     []byte
	Iteration int
}

func (EnqueueTask) sideEffect() {}

// CompleteWorkflow tells the shell to mark the run as completed.
type CompleteWorkflow struct {
	Output []byte
}

func (CompleteWorkflow) sideEffect() {}

// FailWorkflow tells the shell to mark the run as failed.
type FailWorkflow struct {
	StepID string
	Error  string
}

func (FailWorkflow) sideEffect() {}

// SkipStep tells the shell to record that a step was skipped.
type SkipStep struct {
	StepID string
}

func (SkipStep) sideEffect() {}

// Advance computes the next state and side effects for a workflow run.
// Pure function: no receivers, no I/O, no NATS, no KV.
func Advance(
	def dag.WorkflowDef,
	run dag.WorkflowRun,
	evt Event,
) (dag.WorkflowRun, []SideEffect) {
	if evt.StepID == "" {
		panic("Advance: event StepID must not be empty")
	}
	if run.Steps == nil {
		panic("Advance: run.Steps must not be nil")
	}

	// Deep-copy the steps map so callers keep an immutable original.
	run.Steps = copySteps(run.Steps)

	switch evt.Type {
	case EventStepCompleted:
		return advanceCompleted(def, run, evt)
	case EventStepFailed:
		return advanceFailed(def, run, evt)
	case EventStepContinue:
		return advanceContinue(def, run, evt)
	default:
		panic("Advance: unknown event type: " + string(evt.Type))
	}
}

// advanceCompleted handles a step completion event. Marks the step
// Completed, resolves skips and ready steps, and checks for workflow
// completion.
func advanceCompleted(
	def dag.WorkflowDef,
	run dag.WorkflowRun,
	evt Event,
) (dag.WorkflowRun, []SideEffect) {
	state := run.Steps[evt.StepID]
	state.Status = dag.StepStatusCompleted
	state.Output = evt.Payload
	run.Steps[evt.StepID] = state

	return resolveNextSteps(def, run)
}

// advanceFailed handles a step failure event. For non-retriable failures
// the step is marked Failed and the workflow fails. Also resolves any
// SkipIf conditions that trigger on the failed step.
func advanceFailed(
	def dag.WorkflowDef,
	run dag.WorkflowRun,
	evt Event,
) (dag.WorkflowRun, []SideEffect) {
	state := run.Steps[evt.StepID]
	state.Attempts++
	state.Error = evt.FailPayload.Error

	if evt.FailPayload.FailureType == FailureTypeNonRetriable {
		state.Status = dag.StepStatusFailed
		run.Steps[evt.StepID] = state

		effects := resolveSkips(def, run)
		run.Status = dag.RunStatusFailed
		effects = append(effects, FailWorkflow{
			StepID: evt.StepID,
			Error:  evt.FailPayload.Error,
		})
		return run, effects
	}

	// Retriable: just record the attempt, let the shell handle
	// retry scheduling. No side effects from the pure core.
	run.Steps[evt.StepID] = state
	return run, nil
}

// advanceContinue handles an agent-loop continue event. Increments
// the iteration counter and checks MaxIterations bounds.
func advanceContinue(
	def dag.WorkflowDef,
	run dag.WorkflowRun,
	evt Event,
) (dag.WorkflowRun, []SideEffect) {
	stepDef, found := findStepInDef(def, evt.StepID)
	if !found {
		panic(fmt.Sprintf(
			"advanceContinue: step %q not in def", evt.StepID,
		))
	}

	state := run.Steps[evt.StepID]
	state.Iterations++

	cfg, err := dag.ParseAgentLoopConfig(stepDef)
	if err != nil {
		panic("advanceContinue: " + err.Error())
	}

	if cfg.MaxIterations > 0 &&
		state.Iterations >= cfg.MaxIterations {
		state.Status = dag.StepStatusFailed
		state.Error = fmt.Sprintf(
			"agent loop exceeded max iterations (%d)",
			cfg.MaxIterations,
		)
		run.Steps[evt.StepID] = state
		run.Status = dag.RunStatusFailed
		return run, []SideEffect{FailWorkflow{
			StepID: evt.StepID,
			Error:  state.Error,
		}}
	}

	run.Steps[evt.StepID] = state

	input, inputErr := dag.ResolveInput(stepDef, run.Steps)
	if inputErr != nil {
		panic("advanceContinue: ResolveInput: " + inputErr.Error())
	}

	return run, []SideEffect{EnqueueTask{
		Step:      stepDef,
		Input:     input,
		Iteration: state.Iterations,
	}}
}

// resolveNextSteps computes skips, ready steps, and workflow
// completion after a step state change. Returns effects.
func resolveNextSteps(
	def dag.WorkflowDef,
	run dag.WorkflowRun,
) (dag.WorkflowRun, []SideEffect) {
	var effects []SideEffect

	// Resolve skips first — they may unblock downstream steps.
	effects = append(effects, resolveSkips(def, run)...)

	completed := buildCompletedSet(run)
	if dag.IsComplete(def, completed) {
		run.Status = dag.RunStatusCompleted
		effects = append(effects, CompleteWorkflow{})
		return run, effects
	}

	queued := buildQueuedSet(run)
	ready := dag.ResolveReady(def, completed, queued)
	for _, step := range ready {
		if run.Steps[step.ID].Status == dag.StepStatusSkipped {
			continue
		}
		input, err := dag.ResolveInput(step, run.Steps)
		if err != nil {
			panic("resolveNextSteps: ResolveInput: " + err.Error())
		}
		effects = append(effects, EnqueueTask{
			Step:  step,
			Input: input,
		})
	}
	return run, effects
}

// resolveSkips marks steps whose SkipIf condition is satisfied.
// Returns SkipStep effects for each newly skipped step.
func resolveSkips(
	def dag.WorkflowDef,
	run dag.WorkflowRun,
) []SideEffect {
	completed := buildCompletedSet(run)
	queued := buildQueuedSet(run)
	skipped := dag.ResolveSkipped(
		def, completed, queued, run.Steps,
	)
	var effects []SideEffect
	for _, step := range skipped {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusSkipped
		run.Steps[step.ID] = state
		effects = append(effects, SkipStep{StepID: step.ID})
	}
	return effects
}

// findStepInDef locates a step definition by ID. Decoupled from
// the orchestrator's findStepDef method for the pure core.
func findStepInDef(
	def dag.WorkflowDef, stepID string,
) (dag.StepDef, bool) {
	for _, s := range def.Steps {
		if s.ID == stepID {
			return s, true
		}
	}
	return dag.StepDef{}, false
}

// buildCompletedSet returns step IDs in terminal-success states.
func buildCompletedSet(run dag.WorkflowRun) map[string]bool {
	result := make(map[string]bool, len(run.Steps))
	for id, state := range run.Steps {
		switch state.Status {
		case dag.StepStatusCompleted, dag.StepStatusSkipped,
			dag.StepStatusRecovered:
			result[id] = true
		}
	}
	return result
}

// buildQueuedSet returns step IDs that are no longer Pending.
func buildQueuedSet(run dag.WorkflowRun) map[string]bool {
	result := make(map[string]bool, len(run.Steps))
	for id, state := range run.Steps {
		switch state.Status {
		case dag.StepStatusQueued, dag.StepStatusRunning,
			dag.StepStatusCompleted, dag.StepStatusFailed,
			dag.StepStatusSkipped:
			result[id] = true
		}
	}
	return result
}

// copySteps shallow-copies the steps map so Advance does not mutate
// the caller's original.
func copySteps(
	orig map[string]dag.StepState,
) map[string]dag.StepState {
	cp := make(map[string]dag.StepState, len(orig))
	for k, v := range orig {
		cp[k] = v
	}
	return cp
}
