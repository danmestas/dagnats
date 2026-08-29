// internal/engine/task_publish_test.go
// Tests for dispatchIdentity (#624 review round 4): the single builder
// every TaskPayload construction in this package routes through for
// both Attempt and Iteration. Replaces the round-3 nextDispatchAttempt
// /currentAttempt pair, which each computed only Attempt and left
// every call site to set (or, on the retry path, forget to set)
// Iteration by hand — exactly the bug this round fixes.
// Methodology: pure unit tests, no NATS required.
package engine

import (
	"testing"

	"github.com/danmestas/dagnats/dag"
)

func TestDispatchIdentity_NewAttempt(t *testing.T) {
	run := dag.WorkflowRun{
		RunID: "run-1",
		Steps: map[string]dag.StepState{
			"never-started": {Status: dag.StepStatusPending, Attempts: 0, Iterations: 0},
			"started-once":  {Status: dag.StepStatusFailed, Attempts: 1, Iterations: 0},
			"started-twice": {Status: dag.StepStatusFailed, Attempts: 2, Iterations: 0},
			// The round-4 regression: an agent-loop step that Continued
			// to iteration 3 and then failed its CURRENT attempt.
			// Iterations is never reset on retry, so the retry's
			// dispatch identity must carry iteration 3 forward, not
			// reset it to 0 — the reader's default (attempt=Attempts,
			// iteration=Iterations) must land on exactly this pair.
			"loop-retry": {Status: dag.StepStatusFailed, Attempts: 1, Iterations: 3},
			// A map-fan-out instance: same shape as any other step
			// key, just with a "#"-suffixed stepID — dispatchIdentity
			// does not special-case it.
			"fanout#2": {Status: dag.StepStatusFailed, Attempts: 1, Iterations: 0},
		},
	}
	cases := []struct {
		stepID        string
		wantAttempt   int
		wantIteration int
	}{
		// Positive: a step that never started dispatches at attempt 0
		// (the worker/bridge NumDelivered-fallback signal), iteration 0.
		{"never-started", 0, 0},
		// Positive: a step that already started once dispatches its
		// NEXT attempt (Attempts + 1), not the same AttemptNumber again.
		{"started-once", 2, 0},
		{"started-twice", 3, 0},
		// The blocker regression: iteration carries forward across a
		// retry instead of resetting to the Go zero value.
		{"loop-retry", 2, 3},
		{"fanout#2", 2, 0},
		// Negative: an unknown stepID (zero-value StepState) behaves
		// like never-started — no panic, no bogus attempt.
		{"unknown-step", 0, 0},
	}
	for _, tc := range cases {
		attempt, iteration := dispatchIdentity(run, tc.stepID, dispatchNewAttempt)
		if attempt != tc.wantAttempt || iteration != tc.wantIteration {
			t.Errorf(
				"dispatchIdentity(%q, dispatchNewAttempt) = (%d, %d), want (%d, %d)",
				tc.stepID, attempt, iteration, tc.wantAttempt, tc.wantIteration,
			)
		}
	}
}

func TestDispatchIdentity_SameAttempt(t *testing.T) {
	run := dag.WorkflowRun{
		RunID: "run-1",
		Steps: map[string]dag.StepState{
			"never-started": {Status: dag.StepStatusPending, Attempts: 0, Iterations: 0},
			"mid-loop":      {Status: dag.StepStatusRunning, Attempts: 1, Iterations: 2},
			"retried-loop":  {Status: dag.StepStatusRunning, Attempts: 3, Iterations: 1},
		},
	}
	cases := []struct {
		stepID        string
		wantAttempt   int
		wantIteration int
	}{
		// Positive: dispatchSameAttempt returns Attempts bare — NEVER
		// +1 — because a Continue iteration reuses the SAME attempt,
		// not a new one; iteration is the just-incremented value
		// Advance() already wrote into the snapshot.
		{"mid-loop", 1, 2},
		{"retried-loop", 3, 1},
		// Negative: an unstarted step's identity is (0, 0) — Continue
		// only ever fires after a step has started, but the function
		// itself makes no such assumption and must not panic.
		{"never-started", 0, 0},
	}
	for _, tc := range cases {
		attempt, iteration := dispatchIdentity(run, tc.stepID, dispatchSameAttempt)
		if attempt != tc.wantAttempt || iteration != tc.wantIteration {
			t.Errorf(
				"dispatchIdentity(%q, dispatchSameAttempt) = (%d, %d), want (%d, %d)",
				tc.stepID, attempt, iteration, tc.wantAttempt, tc.wantIteration,
			)
		}
	}
}

func TestDispatchIdentity_PanicsOnEmptyStepID(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("dispatchIdentity(\"\") did not panic")
		}
	}()
	dispatchIdentity(dag.WorkflowRun{}, "", dispatchNewAttempt)
}
