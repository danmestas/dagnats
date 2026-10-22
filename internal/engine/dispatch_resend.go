// engine/dispatch_resend.go
// Crash recovery for dispatch. enqueueReady saves every ready step's
// dispatch state in one save, and only then sends the step's task,
// timer or waiter. An engine that dies between the two leaves a step
// saved queued (or running, for a sleep or wait) that nothing will ever
// deliver: a redelivered event finds it already dispatched and moves
// on. The save therefore marks each such step DispatchUnsent, a second
// save clears the mark once the send landed (markSent), and any handler
// that later loads the run with a mark still set sends the step again
// (resendUnsent): every history event for the run, and the reconciler.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// taskProbeScanMax bounds the TASK_QUEUES messages taskStillQueued
// reads on one subject. The subject is per task type and run, so only
// that run's own queued tasks of that type are on it.
const taskProbeScanMax = 256

// resendable reports whether a step of type t can be sent again from
// its saved state, and so is marked unsent until its send lands. The
// other types keep their pre-mark behavior.
func resendable(t dag.StepType) bool {
	switch t {
	case dag.StepTypeNormal, dag.StepTypeAgentLoop,
		dag.StepTypeSleep, dag.StepTypeWaitForEvent:
		return true
	}
	return false
}

// hasUnsentStep reports a running run with a step still marked unsent.
func hasUnsentStep(run dag.WorkflowRun) bool {
	if run.Status != dag.RunStatusRunning {
		return false
	}
	for _, state := range run.Steps {
		if state.DispatchUnsent {
			return true
		}
	}
	return false
}

// markSent clears the unsent mark of each step in sent and saves,
// returning the run as saved. The save is revision-checked: on a
// conflict it reloads and clears again, but only a mark that still
// belongs to the same dispatch (same DispatchNonce), so a newer
// dispatch of the step keeps its own mark.
func (o *Orchestrator) markSent(
	ctx context.Context, run dag.WorkflowRun, sent []string,
) (dag.WorkflowRun, error) {
	if run.RunID == "" {
		panic("markSent: RunID must not be empty")
	}
	if ctx == nil {
		panic("markSent: ctx must not be nil")
	}
	nonces := make(map[string]string, len(sent))
	for _, stepID := range sent {
		nonces[stepID] = run.Steps[stepID].DispatchNonce
	}
	for attempt := 0; attempt < snapshotConflictAttemptsMax; attempt++ {
		if !clearUnsent(run, nonces) {
			return run, nil
		}
		err := o.saveSnapshot(ctx, run, "")
		if !errors.Is(err, ErrSnapshotConflict) {
			return run, err
		}
		run, err = o.store.Load(ctx, run.RunID)
		if err != nil {
			return run, fmt.Errorf("reload run to mark sent: %w", err)
		}
	}
	return run, fmt.Errorf("mark %d steps sent: %w", len(sent), ErrSnapshotConflict)
}

// clearUnsent clears the unsent mark of each step whose dispatch nonce
// is the one in nonces, reporting whether it cleared any.
func clearUnsent(run dag.WorkflowRun, nonces map[string]string) bool {
	if run.Steps == nil {
		panic("clearUnsent: run.Steps must not be nil")
	}
	if len(nonces) > len(run.Steps) {
		panic("clearUnsent: more sent steps than the run has")
	}
	cleared := false
	for stepID, nonce := range nonces {
		state, ok := run.Steps[stepID]
		if !ok || !state.DispatchUnsent || state.DispatchNonce != nonce {
			continue
		}
		state.DispatchUnsent = false
		run.Steps[stepID] = state
		cleared = true
	}
	return cleared
}

// resendUnsent sends again every step of run still marked unsent, then
// marks them sent. A no-op for a run with none, which is every run
// whose dispatches all finished. Callers hold the run lock and pass a
// freshly loaded run.
func (o *Orchestrator) resendUnsent(ctx context.Context, run dag.WorkflowRun) error {
	if run.RunID == "" {
		panic("resendUnsent: RunID must not be empty")
	}
	if ctx == nil {
		panic("resendUnsent: ctx must not be nil")
	}
	if !hasUnsentStep(run) {
		return nil
	}
	wfDef, err := o.loadPinnedOrLatestDef(ctx, run)
	if err != nil {
		return fmt.Errorf("load def to resend unsent steps: %w", err)
	}
	wfDef = dag.EffectiveDef(wfDef, run)
	plan, err := o.planResend(ctx, wfDef, run)
	if err != nil {
		return err
	}
	slog.WarnContext(ctx, "resending steps saved as dispatched but never sent",
		"run_id", run.RunID,
		"tasks", len(plan.normalSteps), "timers_and_waiters", len(plan.effects),
		"already_sent", len(plan.alreadySent))
	sent, sendErr := o.sendPlan(ctx, wfDef, run, plan)
	_, markErr := o.markSent(ctx, run, sent)
	return errors.Join(sendErr, markErr)
}

// planResend rebuilds each unsent step's send from its saved state.
// A task still waiting in TASK_QUEUES under this dispatch's nonce, or
// a task step a worker already started, was sent: it is only marked.
func (o *Orchestrator) planResend(
	ctx context.Context, wfDef dag.WorkflowDef, run dag.WorkflowRun,
) (dispatchPlan, error) {
	if run.RunID == "" {
		panic("planResend: RunID must not be empty")
	}
	if len(wfDef.Steps) == 0 {
		panic("planResend: def must have steps")
	}
	var plan dispatchPlan
	for _, step := range wfDef.Steps {
		state := run.Steps[step.ID]
		if !state.DispatchUnsent {
			continue
		}
		switch step.Type {
		case dag.StepTypeSleep:
			plan.effects = append(plan.effects, stepEffect{
				stepID: step.ID, send: o.sleepHandler.resume(run, step),
			})
		case dag.StepTypeWaitForEvent:
			send, err := o.waitForEventSend(run, step)
			if err != nil {
				return dispatchPlan{}, err
			}
			plan.effects = append(plan.effects, stepEffect{stepID: step.ID, send: send})
		default:
			if err := o.planTaskResend(ctx, run, step, &plan); err != nil {
				return dispatchPlan{}, err
			}
		}
	}
	return plan, nil
}

// planTaskResend adds a task step to plan: to its tasks when its send
// may never have landed, to alreadySent when it provably did.
func (o *Orchestrator) planTaskResend(
	ctx context.Context, run dag.WorkflowRun, step dag.StepDef, plan *dispatchPlan,
) error {
	if plan == nil {
		panic("planTaskResend: plan must not be nil")
	}
	if !resendable(step.Type) {
		panic("planTaskResend: step type is never marked unsent")
	}
	state := run.Steps[step.ID]
	// A mark on an agent loop past iteration 0 is a continue whose
	// next task may never have been sent (#801), whatever the step's
	// status: the loop stays Running between iterations, and it is
	// still Queued when the continue was accepted before the
	// delivery's step.started was applied (#814). Planning that resend
	// as a first dispatch gave it the first dispatch's msg-id, which
	// TASK_QUEUES deduplicated (ADR-026, finding 3). The resend carries
	// the state the continue committed, straight from the snapshot.
	loopIteration := step.Type == dag.StepTypeAgentLoop && state.Iterations > 0
	if state.Status != dag.StepStatusQueued && !loopIteration {
		plan.alreadySent = append(plan.alreadySent, step.ID)
		return nil
	}
	queued, err := o.taskStillQueued(ctx, run, step)
	if err != nil {
		return err
	}
	if queued {
		plan.alreadySent = append(plan.alreadySent, step.ID)
		return nil
	}
	if loopIteration {
		plan.effects = append(plan.effects, stepEffect{
			stepID: step.ID,
			send: func(ctx context.Context) error {
				return o.publishContinueTask(ctx, run, step, state)
			},
		})
		return nil
	}
	plan.normalSteps = append(plan.normalSteps, step)
	return nil
}

// taskStillQueued reports whether TASK_QUEUES holds step's task from
// its current dispatch (matched by DispatchNonce). TASK_QUEUES is a
// work queue: a message stays until a worker acks it, which a worker
// does only after running the step. A message found means the send
// landed and sending again would queue a second copy, which the
// stream's msg-id dedup no longer drops once its window has passed.
// Past the scan bound it reports false: a duplicate task is the
// lesser harm than a step that never runs.
func (o *Orchestrator) taskStillQueued(
	ctx context.Context, run dag.WorkflowRun, step dag.StepDef,
) (bool, error) {
	if run.RunID == "" {
		panic("taskStillQueued: RunID must not be empty")
	}
	if o.js == nil {
		panic("taskStillQueued: js must not be nil")
	}
	stream, err := o.js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		return false, fmt.Errorf("open TASK_QUEUES: %w", err)
	}
	subject := o.publisher.StepSubject(step, run.RunID)
	nonce := run.Steps[step.ID].DispatchNonce
	seq := uint64(1)
	for scanned := 0; scanned < taskProbeScanMax; scanned++ {
		msg, err := stream.GetMsg(ctx, seq, jetstream.WithGetMsgSubject(subject))
		if errors.Is(err, jetstream.ErrMsgNotFound) {
			return false, nil
		}
		if err != nil {
			return false, fmt.Errorf("probe %s: %w", subject, err)
		}
		var payload protocol.TaskPayload
		if json.Unmarshal(msg.Data, &payload) == nil &&
			payload.StepID == step.ID && payload.DispatchNonce == nonce {
			return true, nil
		}
		seq = msg.Sequence + 1
	}
	return false, nil
}
