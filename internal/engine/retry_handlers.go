// internal/engine/retry_handlers.go
// Step-failure and retry/recovery handlers, extracted from orchestrator.go
// per issue #599 (item 1). handleStepFailed splits retriable from
// non-retriable failures; the retriable branch schedules a backoff or a
// retry-after delay via NATS timers, and scheduleStepTimeout/fireStepTimeout
// arm and fire per-step deadlines. dispatchEvent in orchestrator.go remains
// the single switch that routes to them under the run lock. Methods stay on
// *Orchestrator (a same-package file split, not a behavior change).
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
)

// parseFailPayload parses a StepFailedPayload from event payload.
// Falls back to treating raw strings as retriable errors for
// backward compatibility with old workers that send plain strings.
func parseFailPayload(
	data json.RawMessage,
) protocol.StepFailedPayload {
	if len(data) == 0 {
		return protocol.StepFailedPayload{
			FailureType: protocol.FailureTypeRetriable,
		}
	}
	var payload protocol.StepFailedPayload
	if err := json.Unmarshal(data, &payload); err == nil &&
		payload.Error != "" {
		if payload.FailureType == "" {
			payload.FailureType = protocol.FailureTypeRetriable
		}
		return payload
	}
	// Backward compat: raw quoted string
	var rawErr string
	if err := json.Unmarshal(data, &rawErr); err == nil {
		return protocol.StepFailedPayload{
			Error:       rawErr,
			FailureType: protocol.FailureTypeRetriable,
		}
	}
	return protocol.StepFailedPayload{
		Error:       string(data),
		FailureType: protocol.FailureTypeRetriable,
	}
}

// handleStepFailed processes a step failure event. Parses the
// structured StepFailedPayload and branches on FailureType:
// non-retriable skips retries, retry-after schedules exact delay,
// retriable uses existing backoff behavior.
func (o *Orchestrator) handleStepFailed(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepFailed: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepFailed: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	// Idempotency guard (#196). Same shape as the guard in
	// handleStepCompleted — a step.failed for a terminal run is a
	// redelivery and re-running the failure path would double-fire
	// failWorkflow + DLQ publish + runsFailed metric.
	if lateEventForTerminalRun(ctx, run, evt) {
		return nil
	}

	// Check if this is a map instance failure.
	if isMapInstanceID(evt.StepID) {
		return o.handleMapInstanceFailed(
			ctx, wfDef, run, evt,
		)
	}

	// Fence (#801): a failure from a dispatch the step has left (a
	// stale worker, a watchdog armed for an earlier attempt, a
	// redelivery) must not schedule another retry.
	if !o.fenceStepEvent(ctx, run, evt) {
		return nil
	}

	// Attempts is owned by step.queued / step.started lifecycle events
	// (max() rule in handleStepQueued/handleStepStarted). step.failed
	// fires within an attempt and must not raise the counter, with one
	// floor: a failure means at least one attempt ran. In a cluster the
	// engine handling a legacy step.failed (no identity, possibly no
	// attempt number) can load the step before step.queued is applied,
	// with Attempts still 0; the retry backoff needs attempts >= 1.
	state := run.Steps[evt.StepID]
	if state.Attempts < 1 {
		state.Attempts = 1
		run.Steps[evt.StepID] = state
	}

	failPayload := parseFailPayload(evt.Payload)
	state.Error = failPayload.Error

	stepDef, _ := findStepDef(wfDef, evt.StepID)
	policy := dag.ResolveRetryPolicy(wfDef, stepDef)

	if failPayload.FailureType ==
		protocol.FailureTypeNonRetriable {
		return o.dispatchNonRetriableFailure(
			ctx, wfDef, run, stepDef, evt, failPayload,
		)
	}

	// Retry-after: schedule exact delay if retries remain.
	if failPayload.FailureType ==
		protocol.FailureTypeRetryAfter {
		o.metrics.failRetryAfter.Add(ctx, 1)
		return o.handleRetryAfter(
			ctx, wfDef, &run, stepDef, &state,
			evt.StepID, failPayload.RetryAfterMs, policy,
		)
	}

	return o.dispatchRetriableFailure(
		ctx, wfDef, run, state, stepDef, evt, policy,
	)
}

// dispatchNonRetriableFailure is the inner branch of handleStepFailed for
// failure_type=non_retriable: increments metrics, runs the pure-core
// Advance to record the terminal step state, preserves run.Status so
// on-failure recovery handlers can intercept, and delegates the rest to
// the recovery manager.
func (o *Orchestrator) dispatchNonRetriableFailure(
	ctx context.Context, wfDef dag.WorkflowDef, run dag.WorkflowRun,
	stepDef dag.StepDef, evt protocol.Event,
	failPayload protocol.StepFailedPayload,
) error {
	if evt.RunID == "" {
		panic("dispatchNonRetriableFailure: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("dispatchNonRetriableFailure: StepID must not be empty")
	}

	// Non-retriable: use pure core for step state transition,
	// then delegate to recovery manager for failure handling.
	// Advance sets run.Status=Failed, but recovery may intercept
	// with an on-failure handler, so preserve the original status.
	o.metrics.failNonRetriable.Add(ctx, 1)
	slog.InfoContext(ctx,
		"step failed permanently (non-retriable)",
		"run_id", evt.RunID,
		"step_id", evt.StepID,
	)
	advEvt := Event{
		Type:   EventStepFailed,
		StepID: evt.StepID,
		FailPayload: FailPayload{
			Error:       failPayload.Error,
			FailureType: FailureTypeNonRetriable,
		},
	}
	prevStatus := run.Status
	run, _ = Advance(wfDef, run, advEvt)
	// Recovery may handle the failure with an on-failure
	// handler — don't prematurely mark the run Failed.
	run.Status = prevStatus
	state := run.Steps[evt.StepID]
	return o.recovery.HandlePermanentFailure(
		ctx, wfDef, run, stepDef, state, evt.StepID,
		o.saveSnapshot, o.failWorkflow,
		o.notifyParentIfChild, o.releaseTaskSlot,
	)
}

// dispatchRetriableFailure is the inner branch of handleStepFailed for
// failure_type=retriable: if attempts remain, save snapshot and schedule
// retry backoff; if exhausted, transition step to Failed and call
// HandlePermanentFailure. Pre-#147 this branch silently saved without
// scheduling — the explicit name pins the post-#147 contract.
func (o *Orchestrator) dispatchRetriableFailure(
	ctx context.Context, wfDef dag.WorkflowDef, run dag.WorkflowRun,
	state dag.StepState, stepDef dag.StepDef, evt protocol.Event,
	policy *dag.RetryPolicy,
) error {
	if evt.RunID == "" {
		panic("dispatchRetriableFailure: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("dispatchRetriableFailure: StepID must not be empty")
	}

	// Retriable (default): schedule the next attempt via the durable
	// SLEEP_TIMERS path. dag.CalculateDelay drives the wait; the
	// timer re-publishes the task so step.queued / step.started will
	// fire fresh for the new attempt. Without this, attempts were
	// recorded but never re-dispatched (issue #147).
	if policy != nil && state.Attempts <= policy.MaxAttempts {
		// A retry is a new dispatch: a fresh nonce (#801) fences off
		// every event the failed attempt may still send, and the
		// timer carries it so the retried task passes the fence.
		stampDispatch(&state, time.Now().UTC())
		markRetryPending(&state, time.Now().UTC(),
			retryBackoffDelayMs(*policy, state.Attempts))
		run.Steps[evt.StepID] = state
		if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
			return err
		}
		return o.scheduleRetryBackoff(
			ctx, evt.RunID, evt.StepID, stepDef, policy, run, wfDef.Name,
		)
	}

	state.Status = dag.StepStatusFailed
	run.Steps[evt.StepID] = state
	return o.recovery.HandlePermanentFailure(
		ctx, wfDef, run, stepDef, state, evt.StepID,
		o.saveSnapshot, o.failWorkflow,
		o.notifyParentIfChild, o.releaseTaskSlot,
	)
}

// handleRetryAfter handles a retry-after failure: schedules an
// exact delay if retries remain, otherwise permanent failure.
func (o *Orchestrator) handleRetryAfter(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run *dag.WorkflowRun,
	stepDef dag.StepDef,
	state *dag.StepState,
	stepID string,
	retryAfterMs int64,
	policy *dag.RetryPolicy,
) error {
	if stepID == "" {
		panic("handleRetryAfter: stepID must not be empty")
	}
	if run.RunID == "" {
		panic("handleRetryAfter: RunID must not be empty")
	}
	if policy != nil && state.Attempts <= policy.MaxAttempts {
		// A fresh nonce per retry, as in dispatchRetriableFailure (#801).
		stampDispatch(state, time.Now().UTC())
		markRetryPending(state, time.Now().UTC(),
			retryAfterDelayMs(retryAfterMs))
		run.Steps[stepID] = *state
		if err := o.saveSnapshot(ctx, *run, stepID); err != nil {
			return err
		}
		return o.scheduleRetryAfter(
			ctx, run.RunID, stepID, stepDef,
			retryAfterMs, *run, wfDef.Name,
		)
	}
	state.Status = dag.StepStatusFailed
	run.Steps[stepID] = *state
	return o.recovery.HandlePermanentFailure(
		ctx, wfDef, *run, stepDef, *state, stepID,
		o.saveSnapshot, o.failWorkflow,
		o.notifyParentIfChild, o.releaseTaskSlot,
	)
}

// scheduleRetryAfter schedules a timer to re-publish the task
// after the worker-requested delay via SLEEP_TIMERS.
func (o *Orchestrator) scheduleRetryAfter(
	ctx context.Context,
	runID string, stepID string,
	stepDef dag.StepDef,
	retryAfterMs int64,
	run dag.WorkflowRun,
	workflowName string,
) error {
	if runID == "" {
		panic("scheduleRetryAfter: runID must not be empty")
	}
	if stepID == "" {
		panic("scheduleRetryAfter: stepID must not be empty")
	}
	retryAfterMs = retryAfterDelayMs(retryAfterMs)
	input, err := dag.ResolveInput(stepDef, run.Steps, run.Input)
	if err != nil {
		return fmt.Errorf(
			"resolve input for retry-after step %q: %w",
			stepID, err,
		)
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:        TimerActionRetryAfter,
		RunID:         runID,
		StepID:        stepID,
		DurationMs:    retryAfterMs,
		TaskType:      stepDef.Task,
		Subject:       o.publisher.StepSubject(stepDef, runID),
		Metadata:      stepDef.Metadata,
		Input:         input,
		Attempt:       run.Steps[stepID].Attempts,
		WorkflowName:  workflowName,
		DispatchNonce: run.Steps[stepID].DispatchNonce,
	})
}

// scheduleRetryBackoff schedules a timer that re-publishes the task
// after the policy-derived delay. Mirrors scheduleRetryAfter; the
// only difference is the delay source (dag.CalculateDelay vs the
// worker-supplied retryAfterMs) and the timer Action. Both ride the
// same SLEEP_TIMERS plumbing, which keeps the retry path durable
// across orchestrator restarts.
func (o *Orchestrator) scheduleRetryBackoff(
	ctx context.Context,
	runID string, stepID string,
	stepDef dag.StepDef,
	policy *dag.RetryPolicy,
	run dag.WorkflowRun,
	workflowName string,
) error {
	if runID == "" {
		panic("scheduleRetryBackoff: runID must not be empty")
	}
	if stepID == "" {
		panic("scheduleRetryBackoff: stepID must not be empty")
	}
	if policy == nil {
		panic("scheduleRetryBackoff: policy must not be nil")
	}
	// state.Attempts is 1-indexed and counts attempts that have
	// already started (see handleStepStarted's max() rule). The
	// upcoming attempt is Attempts+1, so the delay before the next
	// attempt is CalculateDelay(policy, Attempts) — e.g. for
	// Attempts=1 with exponential the delay is InitialDelay (the
	// 1st retry), and for Attempts=2 it is InitialDelay*Multiplier.
	attempts := run.Steps[stepID].Attempts
	if attempts < 1 {
		panic("scheduleRetryBackoff: attempts must be >= 1")
	}
	// Unreachable for any def that passed dag.Validate (it bounds
	// every policy's MaxAttempts); tripping it means corrupted run
	// state, and failing loudly beats minting unbounded timer
	// msg-ids. The bridge's taskAttemptCountMax mirrors this bound.
	if attempts > dag.RetryAttemptCountMax {
		panic("scheduleRetryBackoff: attempts exceeds RetryAttemptCountMax")
	}
	delayMs := retryBackoffDelayMs(*policy, attempts)
	input, err := dag.ResolveInput(stepDef, run.Steps, run.Input)
	if err != nil {
		return fmt.Errorf(
			"resolve input for retry-backoff step %q: %w",
			stepID, err,
		)
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:        TimerActionRetryBackoff,
		RunID:         runID,
		StepID:        stepID,
		DurationMs:    delayMs,
		TaskType:      stepDef.Task,
		Subject:       o.publisher.StepSubject(stepDef, runID),
		Metadata:      stepDef.Metadata,
		Input:         input,
		Attempt:       attempts,
		WorkflowName:  workflowName,
		DispatchNonce: run.Steps[stepID].DispatchNonce,
	})
}

// scheduleStepTimeout schedules a watchdog timer that fires a
// synthetic step.failed (retriable) if the step is still on the
// same attempt when stepDef.Timeout elapses (issue #140). Caller
// gates on stepDef.Timeout > 0 — entering with zero is a bug.
//
// elapsed is how long the attempt has already been running: zero on
// its first start, more when a redelivery re-arms the watchdog, which
// then waits only for the rest of the timeout (#743).
//
// The Attempt field carries the attempt number that was current
// when the timer was scheduled. fireStepTimeout drops the fire if
// the step has since moved to a later attempt or terminal status.
// MsgId encodes Attempt so a step that runs N attempts gets N
// independent timers, none deduped against the others.
//
// The timer carries the attempt only. An agent loop keeps its attempt
// across iterations but gets a fresh dispatch nonce on every continue,
// and the watchdog is armed once per attempt (a later iteration's
// step.started dedups against the first), so the fire stamps the
// identity the snapshot holds at fire time, not the one at arm time
// (#801).
func (o *Orchestrator) scheduleStepTimeout(
	ctx context.Context,
	runID, stepID string,
	stepDef dag.StepDef,
	attempt int,
	elapsed time.Duration,
) error {
	if runID == "" {
		panic("scheduleStepTimeout: runID must not be empty")
	}
	if elapsed < 0 {
		panic("scheduleStepTimeout: elapsed must not be negative")
	}
	if stepID == "" {
		panic("scheduleStepTimeout: stepID must not be empty")
	}
	if stepDef.Timeout <= 0 {
		panic("scheduleStepTimeout: Timeout must be > 0")
	}
	if attempt < 1 {
		panic("scheduleStepTimeout: attempt must be >= 1")
	}
	// A redelivery after the deadline already passed still arms a timer
	// (1ms) rather than none, so the attempt is failed promptly.
	delayMs := (stepDef.Timeout - elapsed).Milliseconds()
	if delayMs < 1 {
		delayMs = 1
	}
	if delayMs > 24*60*60*1000 {
		delayMs = 24 * 60 * 60 * 1000
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:     TimerActionStepTimeout,
		RunID:      runID,
		StepID:     stepID,
		DurationMs: delayMs,
		TaskType:   stepDef.Task,
		Attempt:    attempt,
	})
}

// fireStepTimeout publishes a synthetic step.failed (retriable)
// for a step whose stepDef.Timeout elapsed while it was still
// running on the same attempt that scheduled the timer.
//
// Staleness is the load-bearing invariant: by the time the timer
// fires, the step may have completed, failed via worker, been
// cancelled, or progressed to a later attempt. Any of those means
// the timer is observing a prior life of the step — drop it.
//
// AttemptNumber on the synthetic event piggybacks on Event.NATSMsgID,
// scoping JetStream dedup to (run, step, attempt) so the timer fire
// can coexist with a worker step.failed that landed concurrently —
// engine treats both arrivals as one logical failure for that attempt.
func (o *Orchestrator) fireStepTimeout(tm TimerMessage) {
	if tm.RunID == "" {
		panic("fireStepTimeout: RunID must not be empty")
	}
	if tm.StepID == "" {
		panic("fireStepTimeout: StepID must not be empty")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	run, err := o.store.Load(ctx, tm.RunID)
	if err != nil {
		return // No state to act on — nothing to fail.
	}
	state, ok := run.Steps[tm.StepID]
	if !ok {
		return // Unknown step — drop.
	}
	// Staleness: only fire if the step is still Running on the
	// exact attempt the timer was scheduled for.
	if state.Status != dag.StepStatusRunning {
		return
	}
	if state.Attempts != tm.Attempt {
		return
	}
	// A re-armed timer's delay is only what remained of the timeout, so
	// report how long the attempt ran when that is the longer figure.
	dur := time.Duration(tm.DurationMs) * time.Millisecond
	if state.AttemptStartedAt != nil {
		dur = max(dur, time.Since(*state.AttemptStartedAt).Round(time.Millisecond))
	}
	payload := protocol.StepFailedPayload{
		Error: fmt.Sprintf(
			"step timeout exceeded (%s)", dur,
		),
		FailureType: protocol.FailureTypeRetriable,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepFailed,
		tm.RunID, tm.StepID, data,
	)
	// The dispatch the step is on now (#801): the attempt check above
	// proved it is the one the timer was armed for, and a loop's
	// iteration and nonce have moved on since without ending the attempt.
	evt.SetIdentity(state.Iterations, tm.Attempt, state.DispatchNonce)
	if err := publishLifecycleEvent(ctx, o.tp, evt); err != nil {
		slog.WarnContext(ctx,
			"step timeout: publish step.failed",
			"run_id", tm.RunID,
			"step_id", tm.StepID,
			"error", err,
		)
	}
}

// retryAfterDelayMs bounds a worker-requested retry-after delay to
// [100ms, 1h].
func retryAfterDelayMs(retryAfterMs int64) int64 {
	const delayMsMin, delayMsMax = 100, 3_600_000
	if retryAfterMs <= 0 {
		return delayMsMin
	}
	if retryAfterMs > delayMsMax {
		return delayMsMax
	}
	return retryAfterMs
}

// retryBackoffDelayMs is the policy-derived delay before the attempt
// after attempts, bounded to [1ms, 1h].
func retryBackoffDelayMs(policy dag.RetryPolicy, attempts int) int64 {
	const delayMsMin, delayMsMax = 1, 3_600_000
	if attempts < 1 {
		panic("retryBackoffDelayMs: attempts must be >= 1")
	}
	delayMs := dag.CalculateDelay(policy, attempts).Milliseconds()
	return min(max(delayMs, delayMsMin), delayMsMax)
}

// markRetryPending resets the step-timeout clock for the retry about to
// be scheduled (#743): the retry is a new attempt and gets the whole
// timeout. Until its task is published, delayMs from now, any
// step.started is a leftover redelivery of the failed attempt and must
// not start the retry's clock; see startAttemptClock.
func markRetryPending(state *dag.StepState, now time.Time, delayMs int64) {
	if state == nil {
		panic("markRetryPending: state must not be nil")
	}
	if delayMs < 1 {
		panic("markRetryPending: delayMs must be positive")
	}
	dispatchAt := now.Add(time.Duration(delayMs) * time.Millisecond)
	state.AttemptStartedAt = nil
	state.RetryDispatchAt = &dispatchAt
}
