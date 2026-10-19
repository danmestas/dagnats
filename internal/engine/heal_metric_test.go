// internal/engine/heal_metric_test.go
// Tests noStepDispatched, the guard that keeps healRun from counting a
// run in runsActive twice when it re-drives a run mid-flight.
// Methodology: pure unit test over a WorkflowRun value; no NATS.
package engine

import (
	"testing"

	"github.com/danmestas/dagnats/dag"
)

func TestNoStepDispatched(t *testing.T) {
	fresh := dag.WorkflowRun{
		RunID:  "r1",
		Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusPending},
			"b": {Status: dag.StepStatusPending},
		},
	}
	if !noStepDispatched(fresh) {
		t.Fatal("all-Pending run must count as never dispatched")
	}
	midFlight := dag.WorkflowRun{
		RunID:  "r2",
		Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted},
			"b": {Status: dag.StepStatusPending},
		},
	}
	if noStepDispatched(midFlight) {
		t.Fatal("a run with a completed step was already dispatched")
	}
}
