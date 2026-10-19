// internal/engine/wait_handlers.go
// Wait-for-event step-kind handlers, extracted from orchestrator.go per
// issue #599 (item 1). Enqueue registers a Correlator watch and arms a
// durable timeout; the matched event is routed through handleStepCompleted
// while the timeout event lands in handleWaitTimeout. dispatchEvent in
// orchestrator.go remains the single switch that routes to them under the
// run lock. Methods stay on *Orchestrator (a same-package file split, not
// a behavior change).
package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// planWaitForEventStep marks the step Running (the caller saves) and
// returns its send: see waitForEventSend.
func (o *Orchestrator) planWaitForEventStep(
	run *dag.WorkflowRun,
	step dag.StepDef,
) (dispatchEffect, error) {
	if step.Type != dag.StepTypeWaitForEvent {
		panic("planWaitForEventStep: wrong step type")
	}
	if run.RunID == "" {
		panic("planWaitForEventStep: RunID must not be empty")
	}
	effect, err := o.waitForEventSend(*run, step)
	if err != nil {
		return nil, err
	}
	state := run.Steps[step.ID]
	state.Status = dag.StepStatusRunning
	run.Steps[step.ID] = state
	return effect, nil
}

// waitForEventSend resolves the match condition and returns the effect
// that publishes WaitStarted, registers the waiter with the correlator,
// and schedules the timeout timer. The timeout runs from the step's
// dispatch stamp, so sending it again after a crash (resendUnsent)
// keeps the original deadline. Every part is idempotent: the waiter is
// keyed by run and step, the timer and event dedup on their msg-ids.
func (o *Orchestrator) waitForEventSend(
	run dag.WorkflowRun, step dag.StepDef,
) (dispatchEffect, error) {
	if step.Type != dag.StepTypeWaitForEvent {
		panic("waitForEventSend: wrong step type")
	}
	if run.RunID == "" {
		panic("waitForEventSend: RunID must not be empty")
	}
	opts, err := dag.ParseWaitForEventConfig(step)
	if err != nil {
		return nil, fmt.Errorf(
			"step %q: WaitForEvent config is nil", step.ID,
		)
	}
	resolvedMatch, err := o.resolveWaitMatch(opts.Match, &run)
	if err != nil {
		return nil, fmt.Errorf(
			"resolve match for step %q: %w", step.ID, err,
		)
	}
	timeout := opts.Timeout
	if dispatchedAt := run.Steps[step.ID].StartedAt; dispatchedAt != nil {
		timeout -= time.Since(*dispatchedAt)
	}
	waiter := EventWaiter{
		RunID:     run.RunID,
		StepID:    step.ID,
		EventType: opts.Event,
		Match:     resolvedMatch,
	}
	return func(ctx context.Context) error {
		return o.startWaitForEvent(ctx, waiter, timeout)
	}, nil
}

// resolveWaitMatch resolves a builder-time Match to a runtime
// ResolvedMatch using step outputs and workflow input.
func (o *Orchestrator) resolveWaitMatch(
	match dag.Match,
	run *dag.WorkflowRun,
) (dag.ResolvedMatch, error) {
	if run == nil {
		panic("resolveWaitMatch: run must not be nil")
	}
	if run.Steps == nil {
		panic("resolveWaitMatch: run.Steps must not be nil")
	}
	stepOutputs := make(map[string][]byte, len(run.Steps))
	for id, state := range run.Steps {
		if state.Output != nil {
			stepOutputs[id] = state.Output
		}
	}
	return match.Resolve(stepOutputs, run.Input)
}

// startWaitForEvent is a wait step's post-save dispatch: publishes
// WaitStarted, registers the correlator waiter, and schedules the
// timeout timer.
func (o *Orchestrator) startWaitForEvent(
	ctx context.Context, waiter EventWaiter, timeout time.Duration,
) error {
	if waiter.RunID == "" {
		panic("startWaitForEvent: RunID must not be empty")
	}
	if waiter.StepID == "" {
		panic("startWaitForEvent: StepID must not be empty")
	}
	o.publishWaitStarted(ctx, waiter.RunID, waiter.StepID)
	if err := o.correlator.AddWaiter(ctx, waiter); err != nil {
		return fmt.Errorf("add waiter: %w", err)
	}
	return o.scheduleWaitTimeout(ctx, waiter.RunID, waiter.StepID, timeout)
}

// scheduleWaitTimeout schedules a timer for the wait-for-event
// timeout. Uses the same SleepTimer infrastructure as sleep steps.
func (o *Orchestrator) scheduleWaitTimeout(
	ctx context.Context,
	runID string, stepID string, timeout time.Duration,
) error {
	if runID == "" {
		panic("scheduleWaitTimeout: runID must not be empty")
	}
	if stepID == "" {
		panic("scheduleWaitTimeout: stepID must not be empty")
	}
	durationMs := timeout.Milliseconds()
	if durationMs <= 0 {
		durationMs = 1
	}
	return o.sleepTimer.Schedule(ctx, TimerMessage{
		Action:     TimerActionWaitTimeout,
		RunID:      runID,
		StepID:     stepID,
		DurationMs: durationMs,
	})
}

// publishWaitStarted publishes an EventStepWaitStarted event.
func (o *Orchestrator) publishWaitStarted(
	ctx context.Context, runID string, stepID string,
) {
	if runID == "" {
		panic("publishWaitStarted: runID must not be empty")
	}
	if stepID == "" {
		panic("publishWaitStarted: stepID must not be empty")
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepWaitStarted,
		runID, stepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	o.tp.JSPublish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
}

// handleWaitTimeout marks the wait step as completed with a timeout
// output so downstream steps can branch on it. Timeout is not a
// failure — it completes the step with {"timeout": true}.
func (o *Orchestrator) handleWaitTimeout(
	ctx context.Context, evt protocol.Event,
) error {
	if evt.RunID == "" {
		panic("handleWaitTimeout: RunID must not be empty")
	}
	if evt.StepID == "" {
		panic("handleWaitTimeout: StepID must not be empty")
	}
	wfDef, run, err := o.loadRunAndDef(ctx, evt.RunID)
	if err != nil {
		return err
	}

	state := run.Steps[evt.StepID]
	// Only process if the step is still Running (not already matched).
	if state.Status != dag.StepStatusRunning {
		return nil
	}

	state.Status = dag.StepStatusCompleted
	state.Output = []byte(`{"timeout":true}`)
	run.Steps[evt.StepID] = state

	// Remove the waiter since the step timed out.
	if o.correlator != nil {
		o.correlator.RemoveWaitersForRun(ctx, evt.RunID)
	}

	completed := completedSet(run)
	if dag.IsComplete(wfDef, completed) {
		return o.completeWorkflow(ctx, run)
	}
	if err := o.saveSnapshot(ctx, run, evt.StepID); err != nil {
		return err
	}
	return o.enqueueReady(ctx, wfDef, run)
}
