// advance_exec.go
// I/O shell that executes side effects produced by the pure Advance
// function. Pattern-matches on concrete SideEffect types and delegates
// to Orchestrator methods for real-world actions (publish tasks,
// complete/fail workflows). Keeps all I/O out of the pure core.
package engine

import (
	"context"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/runid"
)

// executeSideEffects applies the effects produced by Advance to the
// real world. Called after Advance computes the new run state and
// after the snapshot has been saved.
func (o *Orchestrator) executeSideEffects(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	effects []SideEffect,
) error {
	if run.RunID == "" {
		panic("executeSideEffects: RunID must not be empty")
	}
	if effects == nil {
		panic("executeSideEffects: effects must not be nil")
	}

	for i, effect := range effects {
		if i > maxSideEffects {
			return fmt.Errorf(
				"too many side effects (%d)", len(effects),
			)
		}
		if err := o.executeOneEffect(
			ctx, wfDef, run, effect,
		); err != nil {
			return err
		}
	}
	return nil
}

// maxSideEffects is the upper bound on effects per Advance call.
// A workflow with 1000 steps is the practical ceiling.
const maxSideEffects = 1000

// hasEffect returns true if any effect in the slice is of the
// given concrete type. Used by the orchestrator to branch on
// Advance's output without importing test helpers.
func hasEffect[T SideEffect](effects []SideEffect) bool {
	for _, e := range effects {
		if _, ok := e.(T); ok {
			return true
		}
	}
	return false
}

// executeOneEffect dispatches a single side effect to the
// appropriate Orchestrator I/O method.
func (o *Orchestrator) executeOneEffect(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	effect SideEffect,
) error {
	if effect == nil {
		panic("executeOneEffect: effect must not be nil")
	}

	switch e := effect.(type) {
	case EnqueueTask:
		// Reuse the nonce the snapshot carries for this step; if the
		// planner-driven path did not stamp one, mint a fresh nonce so the
		// dispatch is still run-bound (#380). The grant strip keys on the
		// run's workflow name.
		nonce := run.Steps[e.Step.ID].DispatchNonce
		if nonce == "" {
			nonce = runid.New()
		}
		return o.publisher.Publish(
			ctx, run.RunID, e.Step, e.Input, 0,
			run.WorkflowID, nonce,
		)
	case CompleteWorkflow:
		return o.completeWorkflow(ctx, run)
	case FailWorkflow:
		stepDef, _ := findStepDef(wfDef, e.StepID)
		state := run.Steps[e.StepID]
		return o.failWorkflow(ctx, run, stepDef, state)
	case SkipStep:
		// Already applied to run state by Advance.
		return nil
	default:
		panic(fmt.Sprintf(
			"executeOneEffect: unknown effect type %T", effect,
		))
	}
}
