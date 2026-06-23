// engine/compensate_failed_terminal_test.go
// Regression test for the compensate-failed terminal gap (#443/#453).
// Methodology: real embedded NATS server, one per test. failAuxStep
// persisted RunStatusCompensateFailed (a terminal status) WITHOUT routing
// through markTerminal, leaving CompletedAt nil — so the run had no honest
// completion time AND leaked past the retention sweeper forever. This test
// drives failAuxStep through the orchestrator's RecoveryManager and asserts
// the persisted run now carries CompletedAt AND is reachable by
// PruneTerminal once older than the window.
package engine

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestFailAuxStep_StampsCompletedAtAndIsPrunable(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	jsNew, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := NewSnapshotStore(jsNew)

	orch := NewOrchestrator(nc)

	run := dag.WorkflowRun{
		RunID:      "comp-fail-run",
		WorkflowID: "wf",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{"comp": {Status: dag.StepStatusRunning}},
		CreatedAt:  time.Now().UTC().Add(-time.Hour),
	}
	wfDef := dag.WorkflowDef{
		Name: "wf", Version: "1",
		AuxSteps: map[string]bool{"comp": true},
	}
	stepDef := dag.StepDef{ID: "comp"} // no Task → no StepSubject wiring needed
	state := dag.StepState{Status: dag.StepStatusFailed, Error: "boom"}

	var persisted dag.WorkflowRun
	saveFn := func(ctx context.Context, r dag.WorkflowRun, _ string) error {
		persisted = r
		return store.Save(ctx, r)
	}
	notifyFn := func(_ context.Context, _ dag.WorkflowRun, _ error) error {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = orch.recovery.failAuxStep(
		ctx, run, wfDef, stepDef, state, saveFn, notifyFn,
	)
	if err != nil {
		t.Fatalf("failAuxStep failed: %v", err)
	}

	if persisted.Status != dag.RunStatusCompensateFailed {
		t.Fatalf("Status = %v, want CompensateFailed", persisted.Status)
	}
	// The bug: CompletedAt stayed nil for this terminal transition.
	if persisted.CompletedAt == nil {
		t.Fatal("CompletedAt is nil; compensate-failed must stamp it")
	}
	if !persisted.Status.IsTerminal() {
		t.Fatal("CompensateFailed must be terminal")
	}

	// And once older than the window, the sweeper must reach it (no leak).
	old := time.Now().UTC().Add(-48 * time.Hour)
	persisted.CompletedAt = &old
	if err := store.Save(ctx, persisted); err != nil {
		t.Fatalf("re-save with aged CompletedAt failed: %v", err)
	}
	deleted, err := store.PruneTerminal(ctx, 24*time.Hour, 100)
	if err != nil {
		t.Fatalf("PruneTerminal failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("deleted = %d, want 1 (compensate-failed must be prunable)",
			deleted)
	}
	if _, err := store.Load(ctx, "comp-fail-run"); err != ErrRunNotFound {
		t.Fatalf("run should be pruned; Load err = %v, want ErrRunNotFound", err)
	}
}
