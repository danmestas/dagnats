// internal/engine/step_timestamps_e2e_test.go

// Integration test for per-step StartedAt/CompletedAt persistence (#626).
// Methodology: real embedded NATS via dagnatstest.Harness. Drive a
// two-step sequential workflow (b depends on a) through the actual worker
// path to completion, then assert the persisted snapshot carries per-step
// timestamps and that they respect dependency ordering — b cannot start
// before a completes. Bounded wait via RunAndWait's own timeout.
package engine_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
)

func TestEngine_TwoStepRun_PersistsPerStepTimestamps(t *testing.T) {
	h := dagnatstest.NewHarness(t)
	h.Handle(t, "ts-task-a", dagnatstest.PassHandler())
	h.Handle(t, "ts-task-b", dagnatstest.PassHandler())
	h.Start(t)

	wfName := fmt.Sprintf("ts-two-step-%d", time.Now().UnixNano())
	wb := dag.NewWorkflow(wfName)
	a := wb.Task("a", "ts-task-a")
	wb.Task("b", "ts-task-b").After(a)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	got := h.RegisterAndRun(t, def, nil, 10*time.Second)
	if got.Status != dag.RunStatusCompleted {
		t.Fatalf("run status = %v, want Completed", got.Status)
	}

	stepA := got.Steps["a"]
	stepB := got.Steps["b"]

	if stepA.StartedAt == nil {
		t.Fatal("step a: StartedAt must not be nil")
	}
	if stepA.CompletedAt == nil {
		t.Fatal("step a: CompletedAt must not be nil")
	}
	if stepA.CompletedAt.Before(*stepA.StartedAt) {
		t.Fatalf("step a: CompletedAt %v before StartedAt %v",
			stepA.CompletedAt, stepA.StartedAt)
	}

	if stepB.StartedAt == nil {
		t.Fatal("step b: StartedAt must not be nil")
	}
	if stepB.CompletedAt == nil {
		t.Fatal("step b: CompletedAt must not be nil")
	}
	if stepB.CompletedAt.Before(*stepB.StartedAt) {
		t.Fatalf("step b: CompletedAt %v before StartedAt %v",
			stepB.CompletedAt, stepB.StartedAt)
	}

	// Dependency ordering must show in the timestamps: b cannot be
	// dispatched before a has reached its terminal state.
	if stepB.StartedAt.Before(*stepA.CompletedAt) {
		t.Fatalf(
			"step b StartedAt %v is before step a CompletedAt %v "+
				"(dependency ordering violated)",
			stepB.StartedAt, stepA.CompletedAt,
		)
	}
}
