// internal/engine/task_publish_test.go
// Tests for the shared TaskPayload.Attempt builders (#624 review round
// 3, nit 6): nextDispatchAttempt (for a NEW dispatch — initial or
// retry) and currentAttempt (for continuing the SAME attempt — the
// agent-loop Continue re-enqueue). collectReadyMessages, the function
// these tests used to cover, was dead code (no production caller —
// the live dispatch path is TaskPublisher.Publish/doPublish) using the
// PRE-#624-review-round-2 "bare Attempts, no +1" semantic; it has been
// deleted along with its now-unused taskSubject helper.
// Methodology: pure unit tests, no NATS required.
package engine

import (
	"testing"

	"github.com/danmestas/dagnats/dag"
)

func TestNextDispatchAttempt(t *testing.T) {
	run := dag.WorkflowRun{
		RunID: "run-1",
		Steps: map[string]dag.StepState{
			"never-started": {Status: dag.StepStatusPending, Attempts: 0},
			"started-once":  {Status: dag.StepStatusFailed, Attempts: 1},
			"started-twice": {Status: dag.StepStatusFailed, Attempts: 2},
		},
	}
	// Positive: a step that never started dispatches at 0 (the
	// worker/bridge NumDelivered-fallback signal), not 1.
	if got := nextDispatchAttempt(run, "never-started"); got != 0 {
		t.Errorf("nextDispatchAttempt(never-started) = %d, want 0", got)
	}
	// Positive: a step that already started once dispatches its NEXT
	// attempt (Attempts + 1), not the same AttemptNumber again.
	if got := nextDispatchAttempt(run, "started-once"); got != 2 {
		t.Errorf("nextDispatchAttempt(started-once) = %d, want 2", got)
	}
	if got := nextDispatchAttempt(run, "started-twice"); got != 3 {
		t.Errorf("nextDispatchAttempt(started-twice) = %d, want 3", got)
	}
	// Negative: an unknown stepID (zero-value StepState, Attempts=0)
	// behaves exactly like never-started — no panic, no bogus attempt.
	if got := nextDispatchAttempt(run, "unknown-step"); got != 0 {
		t.Errorf("nextDispatchAttempt(unknown-step) = %d, want 0", got)
	}
}

func TestCurrentAttempt(t *testing.T) {
	run := dag.WorkflowRun{
		RunID: "run-1",
		Steps: map[string]dag.StepState{
			"never-started": {Status: dag.StepStatusPending, Attempts: 0},
			"mid-loop":      {Status: dag.StepStatusRunning, Attempts: 1},
			"retried-loop":  {Status: dag.StepStatusRunning, Attempts: 3},
		},
	}
	// Positive: currentAttempt returns Attempts bare — NEVER +1, unlike
	// nextDispatchAttempt — because a Continue iteration reuses the
	// SAME attempt, not a new one.
	if got := currentAttempt(run, "mid-loop"); got != 1 {
		t.Errorf("currentAttempt(mid-loop) = %d, want 1 (bare, no +1)", got)
	}
	if got := currentAttempt(run, "retried-loop"); got != 3 {
		t.Errorf("currentAttempt(retried-loop) = %d, want 3 (bare, no +1)", got)
	}
	// Negative: an unstarted step's current attempt is 0 — Continue
	// only ever fires after a step has started, but the function itself
	// makes no such assumption and must not panic.
	if got := currentAttempt(run, "never-started"); got != 0 {
		t.Errorf("currentAttempt(never-started) = %d, want 0", got)
	}
}
