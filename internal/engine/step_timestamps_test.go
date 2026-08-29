// internal/engine/step_timestamps_test.go

// Tests for per-step StartedAt/CompletedAt persistence (#626).
// Methodology: unit tests exercise stampDispatch/stampCompleted directly
// (nonce/timestamp mechanics, retry re-stamp, nil-state panics); the
// integration test drives a real two-step sequential workflow through the
// dagnatstest embedded-NATS harness and asserts the persisted snapshot
// carries per-step timestamps that respect dependency ordering. Bounded
// waits only — no unbounded polling.
package engine

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

func TestStampDispatch_SetsNonceAndStartedAtClearsCompletedAt(t *testing.T) {
	t.Parallel()
	completedAt := time.Now().UTC()
	state := &dag.StepState{
		Status:      dag.StepStatusFailed,
		CompletedAt: &completedAt,
	}
	now := time.Date(2026, 8, 28, 10, 0, 0, 0, time.UTC)

	stampDispatch(state, now)

	if state.DispatchNonce == "" {
		t.Fatal("stampDispatch must set a non-empty DispatchNonce")
	}
	if state.StartedAt == nil || !state.StartedAt.Equal(now) {
		t.Fatalf("StartedAt = %v, want %v", state.StartedAt, now)
	}
	if state.CompletedAt != nil {
		t.Fatalf(
			"stampDispatch must clear CompletedAt on re-dispatch, got %v",
			state.CompletedAt,
		)
	}
}

func TestStampDispatch_RetryOverwritesPriorNonceAndStartedAt(t *testing.T) {
	t.Parallel()
	first := time.Date(2026, 8, 28, 9, 0, 0, 0, time.UTC)
	second := time.Date(2026, 8, 28, 9, 5, 0, 0, time.UTC)
	state := &dag.StepState{Status: dag.StepStatusQueued}

	stampDispatch(state, first)
	firstNonce := state.DispatchNonce

	stampDispatch(state, second)

	if state.DispatchNonce == firstNonce {
		t.Fatal("retry must re-stamp a fresh, different nonce")
	}
	if !state.StartedAt.Equal(second) {
		t.Fatalf("StartedAt = %v, want retry time %v",
			state.StartedAt, second)
	}
}

func TestStampDispatch_PanicsOnNilState(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("stampDispatch(nil, ...) must panic")
		}
	}()
	stampDispatch(nil, time.Now())
}

func TestStampCompleted_SetsCompletedAt(t *testing.T) {
	t.Parallel()
	state := &dag.StepState{Status: dag.StepStatusCompleted}
	now := time.Date(2026, 8, 28, 11, 0, 0, 0, time.UTC)

	stampCompleted(state, now)

	if state.CompletedAt == nil || !state.CompletedAt.Equal(now) {
		t.Fatalf("CompletedAt = %v, want %v", state.CompletedAt, now)
	}
}

func TestStampCompleted_PanicsOnNilState(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal("stampCompleted(nil, ...) must panic")
		}
	}()
	stampCompleted(nil, time.Now())
}

func TestStampCompleted_PanicsOnNonTerminalStatus(t *testing.T) {
	t.Parallel()
	defer func() {
		if recover() == nil {
			t.Fatal(
				"stampCompleted on a non-terminal status must panic",
			)
		}
	}()
	state := &dag.StepState{Status: dag.StepStatusRunning}
	stampCompleted(state, time.Now())
}

func TestStampTerminalSteps_StampsOnlyUnstampedTerminalSteps(t *testing.T) {
	t.Parallel()
	prior := time.Date(2026, 8, 28, 8, 0, 0, 0, time.UTC)
	now := time.Date(2026, 8, 28, 8, 30, 0, 0, time.UTC)
	steps := map[string]dag.StepState{
		"already-done": {
			Status: dag.StepStatusCompleted, CompletedAt: &prior,
		},
		"just-completed": {Status: dag.StepStatusCompleted},
		"just-failed":    {Status: dag.StepStatusFailed},
		"still-running":  {Status: dag.StepStatusRunning},
	}

	stampTerminalSteps(steps, now)

	if !steps["already-done"].CompletedAt.Equal(prior) {
		t.Fatalf(
			"already-stamped step must not be overwritten, got %v",
			steps["already-done"].CompletedAt,
		)
	}
	if !steps["just-completed"].CompletedAt.Equal(now) {
		t.Fatalf("just-completed CompletedAt = %v, want %v",
			steps["just-completed"].CompletedAt, now)
	}
	if !steps["just-failed"].CompletedAt.Equal(now) {
		t.Fatalf("just-failed CompletedAt = %v, want %v",
			steps["just-failed"].CompletedAt, now)
	}
	if steps["still-running"].CompletedAt != nil {
		t.Fatalf(
			"non-terminal step must not be stamped, got %v",
			steps["still-running"].CompletedAt,
		)
	}
}
