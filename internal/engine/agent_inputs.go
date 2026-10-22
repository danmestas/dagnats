// internal/engine/agent_inputs.go
// Durable waits and input wakeups for agent loops (ADR-027). A
// step.continue may ask the engine to wait before the next iteration:
// until a time (a loop_wait timer on SLEEP_TIMERS, which survives a
// restart like loop_delay), or until an input is queued (the step is
// saved as waiting and the API's step.input.queued event wakes it), or
// both, whichever comes first. No worker holds a claim meanwhile. Both
// wake sources are history events handled under the run lock, so a
// wake is applied once however many engines share the consumer, and a
// stale one (the step already woke, or moved on) is dropped by the
// saved wait flags and the dispatch identity.
//
// No lost wakeup: an input queued before the step entered its wait is
// found by the engine's own check when it enters the wait (the SDK
// refused to commit the wait with inputs queued, but an input can
// land between its check and the engine's save); one queued after is
// its own event. An input submitted to a step that has finished is
// settled unanswered by the save that ends the step, or by its own
// event when it arrives later.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/protocol"
)

// settleSaveAttemptsMax bounds the revision-checked save that clears
// HasInputs after a sweep.
const settleSaveAttemptsMax = 3

// continueWait returns the wait a step.continue asks for, nil for
// none. A wait is a durable request and a malformed one is a bug in
// the worker, so a wait whose time is already past is kept (the
// timer fires at once) rather than dropped.
func continueWait(evt protocol.Event) (*protocol.ContinueWait, error) {
	if evt.Type != protocol.EventStepContinue {
		panic("continueWait: event must be a step.continue")
	}
	if evt.StepID == "" {
		panic("continueWait: StepID must not be empty")
	}
	if !evt.HasIdentity() || len(evt.Payload) == 0 {
		return nil, nil
	}
	var payload protocol.StepContinuePayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		return nil, fmt.Errorf("decode step.continue payload: %w", err)
	}
	if payload.Wait.IsZero() {
		return nil, nil
	}
	return payload.Wait, nil
}

// inputsStore binds the submissions bucket once. nil when the bucket
// is not provisioned (an older deployment): waits for input then wake
// only on their time component, and nothing is swept.
func (o *Orchestrator) inputsStore(ctx context.Context) *inputs.Store {
	if o.js == nil {
		panic("inputsStore: js must not be nil")
	}
	if ctx == nil {
		panic("inputsStore: ctx must not be nil")
	}
	o.inputsOnce.Do(func() {
		store, err := inputs.Open(ctx, o.js)
		if err != nil && !errors.Is(err, inputs.ErrNotProvisioned) {
			slog.WarnContext(ctx, "open submissions bucket; inputs disabled", "error", err)
		}
		o.inputs = store
	})
	return o.inputs
}

// applyWait records wait on the step about to be dispatched, after the
// engine's own check for inputs already queued (no lost wakeup, rule
// 2): a wait for input with inputs queued is not entered at all.
func (o *Orchestrator) applyWait(
	ctx context.Context, run dag.WorkflowRun, stepID string, state *dag.StepState,
	wait *protocol.ContinueWait,
) {
	if state == nil {
		panic("applyWait: state must not be nil")
	}
	if stepID == "" {
		panic("applyWait: stepID must not be empty")
	}
	state.WaitUntil, state.WaitInput = nil, false
	if wait.IsZero() {
		return
	}
	if at, ok := wait.WakeAt(time.Now().UTC()); ok {
		state.WaitUntil = &at
	}
	if !wait.Input {
		return
	}
	state.WaitInput = true
	store := o.inputsStore(ctx)
	if store == nil {
		return
	}
	queued, err := store.Queued(ctx, run.RunID, stepID)
	if err != nil {
		slog.WarnContext(ctx, "check queued inputs before waiting; waiting anyway",
			"run_id", run.RunID, "step_id", stepID, "error", err)
		return
	}
	if len(queued) > 0 {
		slog.InfoContext(ctx, "inputs already queued; sending the iteration instead of waiting",
			"run_id", run.RunID, "step_id", stepID, "queued", len(queued))
		state.WaitUntil, state.WaitInput, state.HasInputs = nil, false, true
	}
}

// waiting reports whether the step's saved state holds a wait.
func waiting(state dag.StepState) bool {
	return state.WaitUntil != nil || state.WaitInput
}

// sendWait is the send of a waiting iteration: the loop_wait timer for
// its time component. An input-only wait sends nothing; the step's
// saved flags are the waiter and the input event is the wakeup.
func (o *Orchestrator) sendWait(
	ctx context.Context, run dag.WorkflowRun, stepDef dag.StepDef, state dag.StepState,
) error {
	if !waiting(state) {
		panic("sendWait: step is not waiting")
	}
	if run.RunID == "" {
		panic("sendWait: RunID must not be empty")
	}
	if state.WaitUntil == nil {
		return nil
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:        TimerActionLoopWait,
		RunID:         run.RunID,
		StepID:        stepDef.ID,
		DurationMs:    max(time.Until(*state.WaitUntil).Milliseconds(), 1),
		Attempt:       state.Attempts,
		Iteration:     state.Iterations,
		DispatchNonce: state.DispatchNonce,
	})
}

// handleInputQueued is the API's announcement of a submission. A step
// waiting for input is woken; one still unfinished remembers that it
// has inputs to settle; one that has finished (or whose run has)
// settles the submission unanswered now.
func (o *Orchestrator) handleInputQueued(ctx context.Context, evt protocol.Event) error {
	if evt.RunID == "" || evt.StepID == "" {
		panic("handleInputQueued: run and step must not be empty")
	}
	if evt.Type != protocol.EventStepInputQueued {
		panic("handleInputQueued: event must be a step.input.queued")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		slog.WarnContext(ctx, "input queued for an unknown run; ignoring",
			"run_id", evt.RunID, "step_id", evt.StepID, "error", err)
		return nil
	}
	state, ok := run.Steps[evt.StepID]
	stepDef, found := findStepDef(wfDef, evt.StepID)
	if !ok || !found {
		slog.WarnContext(ctx, "input queued for an unknown step; ignoring",
			"run_id", evt.RunID, "step_id", evt.StepID)
		return nil
	}
	if run.Status.IsTerminal() || stepFinished(state) {
		o.settleInputs(ctx, run.RunID, evt.StepID)
		return nil
	}
	if state.WaitInput {
		return o.wakeStep(ctx, run, stepDef, state, "input")
	}
	if state.HasInputs {
		return nil
	}
	state.HasInputs = true
	run.Steps[evt.StepID] = state
	return o.saveSnapshot(ctx, run, evt.StepID)
}

// handleWaitElapsed is the loop_wait timer's wakeup. It names the
// dispatch it was armed for; a step that already woke (the flags are
// cleared) or moved on (another iteration or nonce) ignores it.
func (o *Orchestrator) handleWaitElapsed(ctx context.Context, evt protocol.Event) error {
	if evt.RunID == "" || evt.StepID == "" {
		panic("handleWaitElapsed: run and step must not be empty")
	}
	if evt.Type != protocol.EventStepWaitElapsed {
		panic("handleWaitElapsed: event must be a step.wait.elapsed")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}
	state := run.Steps[evt.StepID]
	stepDef, found := findStepDef(wfDef, evt.StepID)
	if !found || run.Status.IsTerminal() || stepFinished(state) || state.WaitUntil == nil ||
		!evt.HasIdentity() || *evt.Iteration != state.Iterations ||
		evt.DispatchNonce != state.DispatchNonce {
		slog.InfoContext(ctx, "dropping stale wait.elapsed",
			"run_id", evt.RunID, "step_id", evt.StepID,
			"step_iteration", state.Iterations, "waiting", waiting(state))
		return nil
	}
	return o.wakeStep(ctx, run, stepDef, state, "timer")
}

// wakeStep ends the step's wait and sends its iteration: the flags are
// cleared in the save that marks the dispatch unsent, then the task is
// published and the mark cleared, as dispatchNextIteration does.
func (o *Orchestrator) wakeStep(
	ctx context.Context, run dag.WorkflowRun, stepDef dag.StepDef, state dag.StepState,
	reason string,
) error {
	if !waiting(state) {
		panic("wakeStep: step is not waiting")
	}
	if reason == "" {
		panic("wakeStep: reason must not be empty")
	}
	slog.InfoContext(ctx, "waking agent loop", "run_id", run.RunID,
		"step_id", stepDef.ID, "iteration", state.Iterations, "reason", reason)
	state.WaitUntil, state.WaitInput = nil, false
	state.HasInputs = state.HasInputs || reason == "input"
	state.DispatchUnsent = true
	run.Steps[stepDef.ID] = state
	if err := o.saveSnapshot(ctx, run, stepDef.ID); err != nil {
		return err
	}
	if err := o.publishContinueTask(ctx, run, stepDef, state); err != nil {
		return err
	}
	_, err := o.markSent(ctx, run, []string{stepDef.ID})
	return err
}

// stepFinished reports a step no input can reach any more.
func stepFinished(state dag.StepState) bool {
	switch state.Status {
	case dag.StepStatusCompleted, dag.StepStatusFailed, dag.StepStatusCancelled:
		return true
	}
	return false
}

// settleInputs is the completion sweep for one step: every queued
// submission becomes unanswered. Best effort; a failure is logged and
// the step's HasInputs mark, when set, keeps the next save retrying.
func (o *Orchestrator) settleInputs(ctx context.Context, runID, stepID string) bool {
	if runID == "" || stepID == "" {
		panic("settleInputs: run and step must not be empty")
	}
	if ctx == nil {
		panic("settleInputs: ctx must not be nil")
	}
	store := o.inputsStore(ctx)
	if store == nil {
		return true
	}
	settled, err := store.SettleUnanswered(ctx, runID, stepID)
	if err != nil {
		slog.WarnContext(ctx, "settle inputs of finished step",
			"run_id", runID, "step_id", stepID, "error", err)
		return false
	}
	if settled > 0 {
		slog.InfoContext(ctx, "settled queued inputs as unanswered",
			"run_id", runID, "step_id", stepID, "count", settled)
	}
	return true
}

// settleFinishedSteps runs after a save: each step that has finished
// with inputs to settle is swept, and the mark cleared in a second,
// revision-checked save so later saves do not sweep it again. The
// sweep follows the save that ends the step, so a submission created
// after that save is found either here or by the API's own check
// against the saved step.
func (o *Orchestrator) settleFinishedSteps(ctx context.Context, run dag.WorkflowRun) {
	if run.RunID == "" {
		panic("settleFinishedSteps: RunID must not be empty")
	}
	if run.Steps == nil {
		panic("settleFinishedSteps: Steps must not be nil")
	}
	var swept []string
	for id, state := range run.Steps {
		if state.HasInputs && stepFinished(state) && o.settleInputs(ctx, run.RunID, id) {
			swept = append(swept, id)
		}
	}
	if len(swept) == 0 {
		return
	}
	for attempt := 0; attempt < settleSaveAttemptsMax; attempt++ {
		for _, id := range swept {
			state := run.Steps[id]
			state.HasInputs = false
			run.Steps[id] = state
		}
		err := o.store.Save(ctx, run)
		if !errors.Is(err, ErrSnapshotConflict) {
			if err != nil {
				slog.WarnContext(ctx, "clear inputs mark after sweep", "run_id", run.RunID, "error", err)
			}
			return
		}
		reloaded, loadErr := o.store.Load(ctx, run.RunID)
		if loadErr != nil {
			return
		}
		run = reloaded
	}
}
