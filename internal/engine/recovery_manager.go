// engine/recovery_manager.go
// RecoveryManager owns failure recovery: on-failure handlers, saga
// compensation chains, and dead-letter publishing. Extracted from
// Orchestrator to reduce its surface area. No behavioral change.
package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// RecoveryManager handles step failure recovery: on-failure
// handlers, saga compensation chains, and dead-letter publishing.
// Delegates snapshot persistence and workflow-level transitions
// back to the Orchestrator via callbacks.
//
// Callback protocol: failFn triggers workflow-level failure,
// saveFn persists the run snapshot after state mutations,
// notifyFn propagates failure to a parent workflow, and
// releaseSlotFn frees task concurrency. RecoveryManager
// modifies run.Steps in-place, then calls saveFn so the caller
// persists. Compensation is sequential — one step at a time in
// reverse topological order. Dead-letter publish is
// fire-and-forget: errors are silently dropped because the
// workflow is already in a terminal failure state.
type RecoveryManager struct {
	js        jetstream.JetStream
	publisher *TaskPublisher
	tracer    trace.Tracer

	// Metrics for compensation failures.
	runsActive metric.Int64UpDownCounter
	runsFailed metric.Int64Counter
}

// NewRecoveryManager creates a RecoveryManager with the given
// dependencies. All parameters are required.
func NewRecoveryManager(
	js jetstream.JetStream,
	publisher *TaskPublisher,
	tracer trace.Tracer,
	runsActive metric.Int64UpDownCounter,
	runsFailed metric.Int64Counter,
) *RecoveryManager {
	if js == nil {
		panic("NewRecoveryManager: js must not be nil")
	}
	if publisher == nil {
		panic(
			"NewRecoveryManager: publisher must not be nil",
		)
	}
	if tracer == nil {
		panic(
			"NewRecoveryManager: tracer must not be nil",
		)
	}
	return &RecoveryManager{
		js:         js,
		publisher:  publisher,
		tracer:     tracer,
		runsActive: runsActive,
		runsFailed: runsFailed,
	}
}

// FailWorkflowFunc transitions a workflow to a failed terminal
// state. The Orchestrator provides this so RecoveryManager can
// trigger failure without owning the workflow lifecycle. The
// implementation is responsible for persisting and notifying.
type FailWorkflowFunc func(
	ctx context.Context,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
) error

// NotifyParentFunc propagates a child workflow failure to its
// parent so the parent can handle or cascade the failure.
// Called after the child run has already been persisted in its
// terminal state.
type NotifyParentFunc func(
	ctx context.Context,
	run dag.WorkflowRun,
	childErr error,
) error

// ReleaseTaskSlotFunc releases a task concurrency slot so other
// runs can claim it. Called early in failure handling before any
// recovery logic, because the failed step no longer needs the slot.
type ReleaseTaskSlotFunc func(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	stepID string,
)

// HandlePermanentFailure handles a step whose retries are
// exhausted or that was marked non-retriable. Checks aux steps,
// on-failure handlers, compensation chains, then fails workflow.
// Callback order: releaseSlotFn first, then one of: saveFn →
// notifyFn (aux failure), saveFn → publish (on-failure/compensate),
// or failFn (no recovery path).
func (rm *RecoveryManager) HandlePermanentFailure(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
	stepID string,
	saveFn SaveSnapshotFunc,
	failFn FailWorkflowFunc,
	notifyFn NotifyParentFunc,
	releaseSlotFn ReleaseTaskSlotFunc,
) error {
	if stepID == "" {
		panic(
			"HandlePermanentFailure: stepID must not be empty",
		)
	}
	if run.RunID == "" {
		panic(
			"HandlePermanentFailure: RunID must not be empty",
		)
	}

	// Release task concurrency slot if configured.
	releaseSlotFn(ctx, wfDef, stepID)

	// If this is an auxiliary step (compensate target) failing,
	// the compensation itself failed — critical state.
	if wfDef.AuxSteps[stepID] {
		return rm.failAuxStep(
			ctx, run, stepDef, state, saveFn, notifyFn,
		)
	}

	// Check for on-failure handler before failing the workflow.
	if stepDef.OnFailure != "" {
		handled, err := rm.TryOnFailure(
			ctx, wfDef, run, stepDef, state, stepID, saveFn,
		)
		if err != nil {
			return err
		}
		if handled {
			return nil
		}
	}

	// No on-failure handler — check for compensation.
	completed := completedSet(run)
	chain := dag.ResolveCompensateChain(
		wfDef, completed, stepID,
	)
	if len(chain) > 0 {
		return rm.StartCompensation(
			ctx, wfDef, &run, stepID, state.Error, saveFn,
		)
	}

	// No compensation either — fail the workflow.
	return failFn(ctx, run, stepDef, state)
}

// failAuxStep handles failure of an auxiliary (compensate) step.
// Marks the run as CompensateFailed and publishes a dead letter.
func (rm *RecoveryManager) failAuxStep(
	ctx context.Context,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
	saveFn SaveSnapshotFunc,
	notifyFn NotifyParentFunc,
) error {
	if run.RunID == "" {
		panic("failAuxStep: RunID must not be empty")
	}
	if stepDef.ID == "" {
		panic("failAuxStep: stepDef.ID must not be empty")
	}
	run.Status = dag.RunStatusCompensateFailed
	if err := saveFn(ctx, run); err != nil {
		return err
	}
	rm.runsActive.Add(ctx, -1)
	rm.runsFailed.Add(ctx, 1)
	rm.PublishDeadLetter(ctx, run.RunID, stepDef, state)
	return notifyFn(
		ctx, run,
		fmt.Errorf("compensation failed: %s", state.Error),
	)
}

// TryOnFailure attempts to run the on-failure handler for a
// failed step. Returns (true, nil) if the handler was enqueued.
// Callback order: modify step Queued → saveFn → publish task.
func (rm *RecoveryManager) TryOnFailure(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
	stepID string,
	saveFn SaveSnapshotFunc,
) (bool, error) {
	if stepDef.OnFailure == "" {
		panic(
			"TryOnFailure: OnFailure must not be empty",
		)
	}
	if stepID == "" {
		panic("TryOnFailure: stepID must not be empty")
	}
	onFailStep, found := findStepDef(
		wfDef, stepDef.OnFailure,
	)
	if !found {
		return false, nil
	}
	ofState := run.Steps[onFailStep.ID]
	ofState.Status = dag.StepStatusQueued
	run.Steps[onFailStep.ID] = ofState
	if err := saveFn(ctx, run); err != nil {
		return false, err
	}
	errorInput := []byte(fmt.Sprintf(
		`{"failed_step":"%s","error":%q}`,
		stepID, state.Error,
	))
	err := rm.publisher.Publish(
		ctx, run.RunID, onFailStep, errorInput, 0,
	)
	return err == nil, err
}

// StartCompensation begins the saga compensation chain.
// Resolves compensate steps in reverse topo order and enqueues
// the first one. Subsequent steps are published one at a time
// by HandleCompensateCompleted as each completes.
// Callback order: modify all compensate steps Queued → saveFn →
// publish first step.
func (rm *RecoveryManager) StartCompensation(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	failedStepID string,
	failedError string,
	saveFn SaveSnapshotFunc,
) error {
	completed := completedSet(*run)
	chain := dag.ResolveCompensateChain(
		wfDef, completed, failedStepID,
	)
	if len(chain) == 0 {
		return nil
	}

	// Mark compensate steps as queued
	for _, step := range chain {
		state := run.Steps[step.ID]
		state.Status = dag.StepStatusQueued
		run.Steps[step.ID] = state
	}
	if err := saveFn(ctx, *run); err != nil {
		return err
	}

	// Build input for the first compensate step
	first := chain[0]
	originalID := findCompensateSource(wfDef, first.ID)
	input := buildCompensateInput(
		originalID, run.Steps[originalID].Output,
		failedStepID, failedError,
	)
	return rm.publisher.Publish(
		ctx, run.RunID, first, input, 0,
	)
}

// HandleCompensateCompleted checks if the completed step is
// part of a compensation chain. If the chain is done, marks
// the workflow as Compensated. If more steps remain, publishes
// the next one. Returns true if this was a compensate step.
// Callback order: saveFn → publish next (or saveFn to finalize).
func (rm *RecoveryManager) HandleCompensateCompleted(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	stepID string,
	saveFn SaveSnapshotFunc,
) bool {
	if !wfDef.AuxSteps[stepID] {
		return false
	}
	// Only handle steps that are Compensate targets,
	// not OnFailure.
	if findCompensateSource(wfDef, stepID) == "" {
		return false
	}

	// Find the next queued compensate step
	for _, step := range wfDef.Steps {
		if step.Compensate == "" {
			continue
		}
		compState := run.Steps[step.Compensate]
		if compState.Status != dag.StepStatusQueued {
			continue
		}
		// Found a queued compensate step — publish it
		compDef, _ := findStepDef(wfDef, step.Compensate)

		// Find the original failure for error context
		var failedStepID, failedError string
		for _, s := range wfDef.Steps {
			st := run.Steps[s.ID]
			if st.Status == dag.StepStatusFailed {
				failedStepID = s.ID
				failedError = st.Error
				break
			}
		}

		input := buildCompensateInput(
			step.ID, run.Steps[step.ID].Output,
			failedStepID, failedError,
		)
		saveFn(ctx, *run)
		rm.publisher.Publish(
			ctx, run.RunID, compDef, input, 0,
		)
		return true
	}

	// All compensate steps done — mark workflow Compensated
	run.Status = dag.RunStatusCompensated
	saveFn(ctx, *run)
	rm.runsActive.Add(ctx, -1)
	return true
}

// PublishDeadLetter publishes failed step info to the
// dead-letter queue for manual inspection. Fire-and-forget:
// publish errors are silently dropped because the workflow is
// already in a terminal state.
func (rm *RecoveryManager) PublishDeadLetter(
	ctx context.Context,
	runID string,
	stepDef dag.StepDef,
	state dag.StepState,
) {
	if runID == "" {
		panic(
			"PublishDeadLetter: runID must not be empty",
		)
	}
	if stepDef.ID == "" {
		panic(
			"PublishDeadLetter: stepDef.ID must not be empty",
		)
	}
	payload, err := json.Marshal(map[string]any{
		"run_id":   runID,
		"step_id":  stepDef.ID,
		"task":     stepDef.Task,
		"error":    state.Error,
		"attempts": state.Attempts,
	})
	if err != nil {
		return
	}
	subject := fmt.Sprintf("dead.%s.%s.%s",
		stepDef.Task, runID, stepDef.ID)
	rm.js.Publish(ctx, subject, payload)
}

// RecoverIfOnFailure checks if stepID is an OnFailure target
// for a failed step. If so, transitions the failed step to
// Recovered and marks dependents as Skipped. No callbacks —
// modifies run.Steps in-place; caller must persist afterward.
func (rm *RecoveryManager) RecoverIfOnFailure(
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	completedStepID string,
) {
	for _, stepDef := range wfDef.Steps {
		if stepDef.OnFailure != completedStepID {
			continue
		}
		failedState := run.Steps[stepDef.ID]
		if failedState.Status != dag.StepStatusFailed {
			continue
		}
		// Mark the original failed step as recovered
		failedState.Status = dag.StepStatusRecovered
		run.Steps[stepDef.ID] = failedState

		// Skip dependents of the failed step — they can't
		// run without its output
		for _, s := range wfDef.Steps {
			for _, dep := range s.DependsOn {
				if dep == stepDef.ID {
					depState := run.Steps[s.ID]
					if depState.Status ==
						dag.StepStatusPending {
						depState.Status =
							dag.StepStatusSkipped
						run.Steps[s.ID] = depState
					}
				}
			}
		}
		return
	}
}

func jsonOrNull(b []byte) string {
	if len(b) == 0 {
		return "null"
	}
	return string(b)
}

// findCompensateSource returns the step ID whose Compensate
// field points to the given compensate step ID.
func findCompensateSource(
	wfDef dag.WorkflowDef, compStepID string,
) string {
	for _, step := range wfDef.Steps {
		if step.Compensate == compStepID {
			return step.ID
		}
	}
	return ""
}

// buildCompensateInput creates the JSON input for a compensate
// step containing original step context and failure details.
func buildCompensateInput(
	originalID string, originalOutput []byte,
	failedStepID string, failedError string,
) []byte {
	return []byte(fmt.Sprintf(
		`{"original_step":%q,"original_output":%s,`+
			`"trigger_step":%q,"trigger_error":%q}`,
		originalID,
		jsonOrNull(originalOutput),
		failedStepID,
		failedError,
	))
}
