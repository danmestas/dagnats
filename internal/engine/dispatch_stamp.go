// internal/engine/dispatch_stamp.go

// Per-step dispatch/completion stamping (#626). Consolidates the
// DispatchNonce minting that used to be duplicated inline at several call
// sites (enqueueReady, handleStepContinue, TryOnFailure,
// StartCompensation, HandleCompensateCompleted, runMapOnFailure) and adds
// StartedAt/CompletedAt alongside it. Both timestamps ride the snapshot
// write the caller was already making — no extra KV write.
// enqueueReadySteps (the unused sibling of enqueueReady) was deleted as
// dead code during the #625 review (it had no non-test caller).
package engine

import (
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/runid"
)

// stampDispatch marks state as freshly dispatched: a new per-dispatch
// nonce (#380) and the engine's dispatch-decision time, both riding the
// caller's already-planned snapshot write. StartedAt is when the engine
// decided to dispatch, not when the worker later picks the task up. A
// retry re-stamps both the nonce and StartedAt, and clears CompletedAt so
// a re-dispatched step (retry, on-failure handler, compensation) does not
// carry a stale completion time from a prior attempt.
func stampDispatch(state *dag.StepState, now time.Time) {
	if state == nil {
		panic("stampDispatch: state must not be nil")
	}
	if now.IsZero() {
		panic("stampDispatch: now must not be zero")
	}
	state.DispatchNonce = runid.New()
	startedAt := now
	state.StartedAt = &startedAt
	state.CompletedAt = nil
	if state.DispatchNonce == "" {
		panic("stampDispatch: nonce must be non-empty after stamping")
	}
}

// stampCompleted records the wall-clock time state reached its terminal
// status. Callers must set Status to Completed or Failed first.
func stampCompleted(state *dag.StepState, now time.Time) {
	if state == nil {
		panic("stampCompleted: state must not be nil")
	}
	if state.Status != dag.StepStatusCompleted &&
		state.Status != dag.StepStatusFailed {
		panic("stampCompleted: state.Status must be Completed or Failed")
	}
	completedAt := now
	state.CompletedAt = &completedAt
}

// stampTerminalSteps stamps CompletedAt for every step in steps that has
// just reached a terminal status (Completed or Failed) but has not yet
// been stamped. Called once per snapshot write (see saveSnapshot) so
// every terminal transition — regardless of which of the engine's several
// completion/failure branches produced it — gets a CompletedAt without
// each branch having to remember to call stampCompleted itself.
// Idempotent: a step whose CompletedAt is already set is left untouched,
// so it keeps the time of its original terminal transition.
func stampTerminalSteps(steps map[string]dag.StepState, now time.Time) {
	if steps == nil {
		panic("stampTerminalSteps: steps must not be nil")
	}
	if now.IsZero() {
		panic("stampTerminalSteps: now must not be zero")
	}
	for id, state := range steps {
		if state.CompletedAt != nil {
			continue
		}
		if state.Status != dag.StepStatusCompleted &&
			state.Status != dag.StepStatusFailed {
			continue
		}
		stampCompleted(&state, now)
		steps[id] = state
	}
}
