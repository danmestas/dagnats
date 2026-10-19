// internal/engine/sleep_handlers.go
// sleepHandler owns the sleep step-kind lifecycle, extracted from
// Orchestrator per issue #565. No worker is involved: the step is
// marked Running, a started event is published for observability,
// and a durable timer (NakWithDelay via SleepTimer) fires the
// completion event directly. Dispatch-time config/resolution errors
// are non-retryable, so the step fails the run rather than looping.
package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// sleepHandler drives sleep steps. It depends on the runMutator
// port for persistence/failure and on the SleepTimer +
// TracingPublisher subsystems it publishes through directly. It
// deliberately does not hold the whole Orchestrator.
type sleepHandler struct {
	mutator    runMutator
	sleepTimer *SleepTimer
	tp         *natsutil.TracingPublisher
}

// newSleepHandler wires a sleepHandler. All dependencies are
// required — a nil dependency is a wiring bug, not a runtime state.
func newSleepHandler(
	mutator runMutator,
	sleepTimer *SleepTimer,
	tp *natsutil.TracingPublisher,
) *sleepHandler {
	if mutator == nil {
		panic("newSleepHandler: mutator must not be nil")
	}
	if sleepTimer == nil {
		panic("newSleepHandler: sleepTimer must not be nil")
	}
	if tp == nil {
		panic("newSleepHandler: tp must not be nil")
	}
	return &sleepHandler{
		mutator:    mutator,
		sleepTimer: sleepTimer,
		tp:         tp,
	}
}

// plan marks the step Running with its wake time (the caller saves)
// and returns the effect that publishes SleepStarted and schedules the
// durable timer. No worker is involved — the timer fires the
// completion event directly. A deterministic config error fails the
// run here, which saves on its own; plan then reports errRunFinalized.
func (sh *sleepHandler) plan(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
) (dispatchEffect, error) {
	if step.Type != dag.StepTypeSleep {
		panic("sleepHandler.plan: step is not a Sleep step")
	}
	if run.RunID == "" {
		panic("sleepHandler.plan: RunID must not be empty")
	}

	// Both the config parse and the resolution below read only the def
	// and the run input, which are immutable for the life of the run. A
	// failure here is therefore deterministic: every redelivery would
	// reproduce it identically, so retrying can never succeed. Fail the
	// step instead of returning an error that leaves it queued forever.
	sleepCfg, err := dag.ParseSleepConfig(step)
	if err != nil {
		return nil, sh.failFinal(ctx, run, step, err.Error())
	}

	// Cron and until_input_path forms are only knowable now; the run
	// input (not the step's resolved input) is the deadline source.
	now := time.Now()
	if sleepCfg.Cron != "" {
		if _, zoneErr := dag.CronSleepLocation(sleepCfg, run.Input); zoneErr != nil {
			slog.WarnContext(ctx, "sleep: cron zone falls back to UTC",
				"run_id", run.RunID, "step_id", step.ID, "error", zoneErr)
		}
	}
	sleepFor, err := dag.ResolveSleepDuration(sleepCfg, run.Input, now)
	if err != nil {
		return nil, sh.failFinal(ctx, run, step, err.Error())
	}

	// Mark step as Running and record wake time.
	state := run.Steps[step.ID]
	state.Status = dag.StepStatusRunning
	wakeAt := now.Add(sleepFor)
	state.WakeAt = &wakeAt
	run.Steps[step.ID] = state

	return sh.sendTimer(run.RunID, step.ID, sleepFor), nil
}

// resume rebuilds a planned sleep's effect from its saved wake time,
// for a step whose send may never have happened (resendUnsent). The
// timer is due at the same wake time; one already past fires at once.
func (sh *sleepHandler) resume(run dag.WorkflowRun, step dag.StepDef) dispatchEffect {
	if step.Type != dag.StepTypeSleep {
		panic("sleepHandler.resume: step is not a Sleep step")
	}
	state := run.Steps[step.ID]
	if state.WakeAt == nil {
		panic("sleepHandler.resume: a planned sleep has a wake time")
	}
	return sh.sendTimer(run.RunID, step.ID, time.Until(*state.WakeAt))
}

// sendTimer returns the effect that publishes SleepStarted and
// schedules the durable timer due after sleepFor (at least 1ms).
func (sh *sleepHandler) sendTimer(
	runID, stepID string, sleepFor time.Duration,
) dispatchEffect {
	if runID == "" {
		panic("sleepHandler.sendTimer: runID must not be empty")
	}
	if stepID == "" {
		panic("sleepHandler.sendTimer: stepID must not be empty")
	}
	durationMs := max(sleepFor.Milliseconds(), 1)
	return func(ctx context.Context) error {
		// Publish sleep started event for observability.
		sh.publishStarted(ctx, runID, stepID)
		// Schedule durable timer via NakWithDelay.
		return sh.sleepTimer.Schedule(ctx, TimerMessage{
			Action:     TimerActionSleepComplete,
			RunID:      runID,
			StepID:     stepID,
			DurationMs: durationMs,
		})
	}
}

// failFinal fails the step and the run (see fail) and, when that
// succeeded, reports errRunFinalized so the dispatch stops.
func (sh *sleepHandler) failFinal(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
	reason string,
) error {
	if reason == "" {
		panic("sleepHandler.failFinal: reason must not be empty")
	}
	if run == nil {
		panic("sleepHandler.failFinal: run must not be nil")
	}
	if err := sh.fail(ctx, run, step, reason); err != nil {
		return err
	}
	return errRunFinalized
}

// fail marks a sleep step Failed and fails the run. Used for
// dispatch-time errors that no retry can clear, mirroring
// failPlannerStep: the config and the run input cannot change, so
// the alternative is a run wedged with the step stuck in Queued.
func (sh *sleepHandler) fail(
	ctx context.Context,
	run *dag.WorkflowRun,
	step dag.StepDef,
	reason string,
) error {
	if run.RunID == "" {
		panic("sleepHandler.fail: RunID must not be empty")
	}
	if step.ID == "" {
		panic("sleepHandler.fail: step.ID must not be empty")
	}

	slog.ErrorContext(ctx,
		"sleep step failed",
		"error", reason,
		"run_id", run.RunID,
		"step_id", step.ID,
	)

	state := run.Steps[step.ID]
	state.Status = dag.StepStatusFailed
	state.Error = reason
	run.Steps[step.ID] = state

	return sh.mutator.failWorkflow(ctx, *run, step, state)
}

// publishStarted publishes an EventStepSleepStarted event.
func (sh *sleepHandler) publishStarted(
	ctx context.Context, runID string, stepID string,
) {
	if runID == "" {
		panic("sleepHandler.publishStarted: runID must not be empty")
	}
	if stepID == "" {
		panic("sleepHandler.publishStarted: stepID must not be empty")
	}
	evt := protocol.NewStepEvent(
		protocol.EventStepSleepStarted,
		runID, stepID, nil,
	)
	data, err := evt.Marshal()
	if err != nil {
		return
	}
	sh.tp.JSPublish(
		ctx, evt.NATSSubject(), data,
		jetstream.WithMsgID(evt.NATSMsgID()),
	)
}
