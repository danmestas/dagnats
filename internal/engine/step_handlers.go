// internal/engine/step_handlers.go
// Ordinary step-lifecycle handlers, extracted from orchestrator.go per
// issue #599 (item 1). These cover the queued -> started -> completed
// transitions and the loop/continue re-enqueue path for iterating steps.
// dispatchEvent in orchestrator.go remains the single switch that routes to
// them under the run lock. Methods stay on *Orchestrator (a same-package
// file split, not a behavior change).
package engine

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// handleStepCompleted marks the step output in the snapshot, then checks
// whether the workflow is fully complete or new steps have become unblocked.
func (o *Orchestrator) handleStepCompleted(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepCompleted: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepCompleted: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	// Idempotency guard (#196). A step.completed event for a run
	// already in a terminal state is a redelivery from a JetStream
	// history replay. Without this guard, Advance would re-mark the
	// step Completed and call completeWorkflow, double-decrementing
	// runsActive and republishing workflow.completed.
	if run.Status.IsTerminal() {
		slog.InfoContext(ctx,
			"skipping step.completed for terminal run",
			"run_id", evt.RunID,
			"step_id", evt.StepID,
			"run_status", run.Status.String(),
		)
		return nil
	}

	// Map instances have their own completion logic.
	if isMapInstanceID(evt.StepID) {
		return o.handleMapInstanceCompleted(
			ctx, wfDef, run, evt,
		)
	}

	// Planner steps must materialize output before DAG
	// resolution — short-circuit before Advance.
	stepDef, foundStep := findStepDef(wfDef, evt.StepID)
	if foundStep && stepDef.Type == dag.StepTypePlanner {
		state := run.Steps[evt.StepID]
		state.Status = dag.StepStatusCompleted
		state.Output = evt.Payload
		run.Steps[evt.StepID] = state
		o.releaseTaskSlot(ctx, wfDef, evt.StepID)
		return o.materializePlannerOutput(
			ctx, wfDef, run, stepDef, evt.Payload,
		)
	}

	// Pure core: compute state transition and side effects.
	advEvt := Event{
		Type:    EventStepCompleted,
		StepID:  evt.StepID,
		Payload: evt.Payload,
	}
	run, _ = Advance(wfDef, run, advEvt)

	// Orchestrator-only I/O that Advance cannot handle.
	o.releaseTaskSlot(ctx, wfDef, evt.StepID)
	o.sticky.CreateBinding(ctx, wfDef, run, evt)
	o.recovery.RecoverIfOnFailure(wfDef, &run, evt.StepID)

	if o.recovery.HandleCompensateCompleted(
		ctx, wfDef, &run, evt.StepID, o.saveSnapshot,
	) {
		return nil
	}

	// Recovery may have changed run state (e.g. marking a step
	// Recovered), so use orchestrator's enqueueReady which
	// respects post-recovery state.
	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}

// handleStepContinue re-enqueues an agent-loop step for another iteration.
// Uses Advance for iteration increment and MaxIterations check, then
// applies LoopStartedAt tracking, MaxDuration enforcement, and
// LoopDelay scheduling that only the orchestrator can do.
func (o *Orchestrator) handleStepContinue(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepContinue: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepContinue: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}
	stepDef, found := findStepDef(wfDef, evt.StepID)
	if !found {
		return fmt.Errorf(
			"step %q not found in workflow def", evt.StepID,
		)
	}

	// Pure core: increment iterations and check MaxIterations.
	advEvt := Event{
		Type:   EventStepContinue,
		StepID: evt.StepID,
	}
	run, effects := Advance(wfDef, run, advEvt)

	// If Advance produced a FailWorkflow effect, MaxIterations
	// was exceeded — fail via orchestrator's full failure path.
	if hasEffect[FailWorkflow](effects) {
		state := run.Steps[evt.StepID]
		return o.failLoopStep(
			ctx, run, evt.StepID, state, state.Error,
		)
	}

	// Orchestrator-only: track loop start time and enforce
	// MaxDuration, which the pure core does not handle.
	state := run.Steps[evt.StepID]
	if state.Iterations == 1 {
		state.LoopStartedAt = time.Now().UTC()
	}
	if exceeded, reason := checkLoopBounds(
		stepDef, state,
	); exceeded {
		return o.failLoopStep(
			ctx, run, evt.StepID, state, reason,
		)
	}
	// Re-stamp a fresh per-dispatch nonce and StartedAt for this iteration
	// (#380, #626): both ride this snapshot save (no extra write); the
	// nonce is threaded onto the PublishIteration payload so the
	// iteration's control-plane calls bind to this dispatch.
	stampDispatch(&state, time.Now().UTC())
	run.Steps[evt.StepID] = state

	if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
		return err
	}
	return o.publishContinueTask(
		ctx, run, stepDef, state,
	)
}

// publishContinueTask resolves input and publishes the next
// iteration task, with optional LoopDelay scheduling.
func (o *Orchestrator) publishContinueTask(
	ctx context.Context,
	run dag.WorkflowRun,
	stepDef dag.StepDef,
	state dag.StepState,
) error {
	if stepDef.ID == "" {
		panic("publishContinueTask: stepDef.ID must not be empty")
	}
	if run.RunID == "" {
		panic("publishContinueTask: RunID must not be empty")
	}
	input, err := dag.ResolveInput(stepDef, run.Steps, run.Input)
	if err != nil {
		return fmt.Errorf(
			"resolve input for step %q: %w", stepDef.ID, err,
		)
	}
	loopCfg, _ := dag.ParseAgentLoopConfig(stepDef)
	// #624 review round 4: dispatchIdentity(dispatchSameAttempt) — a
	// Continue iteration reuses the SAME attempt; only Iterations
	// increments (already reflected in run, since Advance() bumped it
	// before this call — see dispatchIdentity's doc comment). iteration
	// here always equals state.Iterations by construction; using the
	// builder's return value (not state.Iterations directly) keeps
	// this call site honest about routing through the single builder.
	attempt, iteration := dispatchIdentity(run, stepDef.ID, dispatchSameAttempt)
	// state.DispatchNonce was stamped fresh by handleContinue before the
	// snapshot save, so it is already persisted; thread it (with the run's
	// workflow name) through both the delayed and immediate re-enqueue (#380).
	if loopCfg.LoopDelay > 0 {
		return o.scheduleDelayedIteration(
			ctx, run.RunID, run.WorkflowID, stepDef, input,
			attempt, iteration, loopCfg.LoopDelay, state.DispatchNonce,
		)
	}
	return o.publisher.PublishIteration(
		ctx, run.RunID, stepDef, input, attempt, iteration,
		run.WorkflowID, state.DispatchNonce,
	)
}

// scheduleDelayedIteration defers re-enqueue via a context-aware
// timer goroutine. Cancels cleanly if context expires.
func (o *Orchestrator) scheduleDelayedIteration(
	ctx context.Context,
	runID string,
	workflowName string,
	stepDef dag.StepDef,
	input []byte,
	attempt int,
	iteration int,
	delay time.Duration,
	dispatchNonce string,
) error {
	if runID == "" {
		panic(
			"scheduleDelayedIteration: runID must not be empty",
		)
	}
	if delay <= 0 {
		panic(
			"scheduleDelayedIteration: delay must be positive",
		)
	}
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			pubErr := o.publisher.PublishIteration(
				ctx, runID, stepDef, input, attempt, iteration,
				workflowName, dispatchNonce,
			)
			if pubErr != nil {
				slog.ErrorContext(ctx,
					"delayed iteration publish failed",
					"error", pubErr,
					"run_id", runID,
					"step_id", stepDef.ID,
				)
			}
		}
	}()
	return nil
}

// checkLoopBounds returns (true, reason) when the step has hit its
// MaxIterations or MaxDuration ceiling. Both limits are checked.
func checkLoopBounds(
	stepDef dag.StepDef, state dag.StepState,
) (bool, string) {
	cfg, err := dag.ParseAgentLoopConfig(stepDef)
	if err != nil {
		return false, ""
	}
	if cfg.MaxIterations > 0 &&
		state.Iterations >= cfg.MaxIterations {
		return true, fmt.Sprintf(
			"agent loop exceeded max iterations (%d)",
			cfg.MaxIterations,
		)
	}
	if cfg.MaxDuration > 0 &&
		!state.LoopStartedAt.IsZero() &&
		time.Since(state.LoopStartedAt) >= cfg.MaxDuration {
		return true, fmt.Sprintf(
			"agent loop exceeded max duration (%s)",
			cfg.MaxDuration,
		)
	}
	return false, ""
}

// failLoopStep marks the step and run as failed, saves state, publishes
// a workflow.failed event, and adjusts metrics.
func (o *Orchestrator) failLoopStep(
	ctx context.Context,
	run dag.WorkflowRun,
	stepID string,
	state dag.StepState,
	reason string,
) error {
	if stepID == "" {
		panic("failLoopStep: stepID must not be empty")
	}
	if reason == "" {
		panic("failLoopStep: reason must not be empty")
	}
	state.Status = dag.StepStatusFailed
	state.Error = reason
	run.Steps[stepID] = state
	run, err := finalizeRun(
		ctx, o.tp, o.saveSnapshot, run, dag.RunStatusFailed, stepID,
		func(ctx context.Context) error {
			wfAttr := metric.WithAttributes(
				attribute.String("workflow", run.WorkflowID),
			)
			o.metrics.runsActive.Add(ctx, -1, wfAttr)
			o.metrics.runsFailed.Add(ctx, 1, wfAttr)
			// releaseAdmission (#648) also releases a held singleton
			// lock -- this path never did that before, a pre-existing
			// gap (a singleton-locked workflow whose loop step hit its
			// bound iteration/duration ceiling kept the lock forever).
			// ReleaseSingletonLock is a no-op when run holds none, so
			// this is safe for the common no-singleton case too.
			return o.releaseAdmission(ctx, run)
		},
	)
	if err != nil {
		return err
	}
	return o.notifyParentIfChild(ctx, run, fmt.Errorf("%s", reason))
}

// handleStepStarted transitions the step from Queued to Running and
// updates the attempt counter. Monotonic: refuses to regress a
// terminal state — a stale step.started arriving after the engine
// already saw step.completed/step.failed is logged and ignored.
//
// Attempts uses max() rule so out-of-order delivery cannot decrement
// the counter; a higher AttemptNumber wins.
func (o *Orchestrator) handleStepStarted(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepStarted: evt.RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepStarted: evt.StepID must not be empty")
	}

	run, err := o.store.Load(ctx, evt.RunID)
	if err != nil {
		return fmt.Errorf("load run %q: %w", evt.RunID, err)
	}
	state, ok := run.Steps[evt.StepID]
	if !ok {
		slog.WarnContext(ctx,
			"step.started for unknown step",
			"run_id", evt.RunID,
			"step_id", evt.StepID,
		)
		return nil
	}

	// Monotonic guard — don't regress a terminal state.
	if state.Status == dag.StepStatusCompleted ||
		state.Status == dag.StepStatusFailed {
		slog.WarnContext(ctx,
			"stale step.started ignored — step is terminal",
			"run_id", evt.RunID,
			"step_id", evt.StepID,
			"current_status", state.Status,
			"event_attempt", evt.AttemptNumber,
		)
		return nil
	}

	attemptCountBefore := state.Attempts
	state.Status = dag.StepStatusRunning
	if evt.AttemptNumber > state.Attempts {
		state.Attempts = evt.AttemptNumber
	}
	// Postcondition: the max() rule above is what keeps per-attempt
	// retry timer msg-ids distinct (#381) — a regression to "assign"
	// would let out-of-order step.started decrement the counter.
	if state.Attempts < attemptCountBefore {
		panic("handleStepStarted: Attempts must be non-decreasing")
	}
	run.Steps[evt.StepID] = state
	if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
		return err
	}
	// Schedule the per-step watchdog (issue #140). Every
	// step.started arms a fresh timer; stale fires from prior
	// attempts are dropped by fireStepTimeout's staleness guard.
	// Skipping the watchdog on def-load failure is acceptable: the
	// run is already saved as Running, and the next step.started
	// (e.g. on retry) will re-arm.
	wfDef, err := o.loadDef(ctx, run.WorkflowID)
	if err != nil {
		return nil
	}
	stepDef, found := findStepDef(wfDef, evt.StepID)
	if !found || stepDef.Timeout <= 0 {
		return nil
	}
	return o.scheduleStepTimeout(
		ctx, evt.RunID, evt.StepID, stepDef, state.Attempts,
	)
}

// handleStepQueued is mostly a no-op during normal operation — the
// engine's dispatch path already set Status to Queued before it
// emitted this event. The handler exists for state recovery on
// engine restart, where the history stream is replayed and the
// engine reconstructs run state from events alone.
//
// Monotonic: refuses to roll back from Running, Completed, Failed.
func (o *Orchestrator) handleStepQueued(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleStepQueued: evt.RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleStepQueued: evt.StepID must not be empty")
	}

	run, err := o.store.Load(ctx, evt.RunID)
	if err != nil {
		return fmt.Errorf("load run %q: %w", evt.RunID, err)
	}
	state, ok := run.Steps[evt.StepID]
	if !ok {
		slog.WarnContext(ctx,
			"step.queued for unknown step",
			"run_id", evt.RunID, "step_id", evt.StepID,
		)
		return nil
	}
	if state.Status == dag.StepStatusCompleted ||
		state.Status == dag.StepStatusFailed ||
		state.Status == dag.StepStatusRunning {
		// Already past Queued — don't roll back.
		return nil
	}
	attemptCountBefore := state.Attempts
	state.Status = dag.StepStatusQueued
	if evt.AttemptNumber > state.Attempts {
		state.Attempts = evt.AttemptNumber
	}
	// Postcondition: same max()-rule guard as handleStepStarted —
	// Attempts is the input to per-attempt retry timer msg-ids (#381).
	if state.Attempts < attemptCountBefore {
		panic("handleStepQueued: Attempts must be non-decreasing")
	}
	run.Steps[evt.StepID] = state
	return o.saveSnapshot(ctx, run, evt.StepID)
}
