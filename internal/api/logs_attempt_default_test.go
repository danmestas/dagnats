// internal/api/logs_attempt_default_test.go
// Tests the default attempt a bare GET .../logs resolves to before the
// step has started. The snapshot holds Attempts=0 until step.started
// lands, but a producer writes its first attempt's logs to attempt 1
// (resolveAttemptNumber in worker/context.go), so a follow attached
// in that window must still land on attempt 1's subject.
// Methodology: pure unit tests over attemptParam and dispatchIsDone;
// no NATS needed because both read only the step snapshot.
package api

import (
	"net/http/httptest"
	"testing"

	"github.com/danmestas/dagnats/dag"
)

func TestAttemptParam_UnstartedStepResolvesToFirstAttempt(t *testing.T) {
	bare := httptest.NewRequest("GET", "/runs/r/logs?step=a", nil)
	unstarted := dag.StepState{Status: dag.StepStatusPending}
	if got := attemptParam(bare, unstarted); got != 1 {
		t.Fatalf("unstarted step attempt = %d, want 1", got)
	}
	started := dag.StepState{Status: dag.StepStatusRunning, Attempts: 3}
	if got := attemptParam(bare, started); got != 3 {
		t.Fatalf("started step attempt = %d, want 3", got)
	}
	explicit := httptest.NewRequest("GET", "/runs/r/logs?step=a&attempt=0", nil)
	if got := attemptParam(explicit, unstarted); got != 0 {
		t.Fatalf("explicit attempt = %d, want 0", got)
	}
}

func TestDispatchIsDone_UnstartedStepTreatsFirstAttemptAsCurrent(t *testing.T) {
	pending := dag.StepState{Status: dag.StepStatusPending}
	if dispatchIsDone(1, 0, pending) {
		t.Fatal("first attempt of a pending step reported done")
	}
	skipped := dag.StepState{Status: dag.StepStatusSkipped}
	if !dispatchIsDone(1, 0, skipped) {
		t.Fatal("first attempt of a skipped step never reports done")
	}
	if !dispatchIsDone(0, 0, pending) {
		t.Fatal("attempt 0 is never written and must read as done")
	}
}
