// dagnatstest/run_fixture_test.go
// Methodology: real embedded NATS through NewHarness. The harness's
// engine is stopped before a run is started, so the run is known only
// by its workflow.started (GetRun answers it pending, #1006); a fresh
// engine is started a moment later. RunFixture.Snapshot must return the
// engine's snapshot, which carries a store revision, not the pending
// view built from the start event, which carries none.
package dagnatstest

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
)

func TestRunFixtureSnapshotWaitsForTheEnginesSnapshot(t *testing.T) {
	h := NewHarness(t)
	f := NewRunFixture(h)
	wb := dag.NewWorkflow("snapshot-wait-wf")
	wb.Task("a", "snapshot-wait-task")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	ctx := context.Background()
	if err := h.Svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	h.Engine.Stop()
	runID, err := h.Svc.StartRun(ctx, def.Name, nil)
	if err != nil {
		t.Fatalf("StartRun: %v", err)
	}
	pending, err := h.Svc.GetRun(ctx, runID)
	if err != nil || pending.Revision != nil {
		t.Fatalf("GetRun before the engine = rev %v err=%v, want the revisionless pending view",
			pending.Revision, err)
	}

	late := engine.NewOrchestrator(h.NC)
	t.Cleanup(late.Stop)
	go func() {
		time.Sleep(300 * time.Millisecond)
		if err := late.Start(); err != nil {
			t.Errorf("late engine Start: %v", err)
		}
	}()
	run := f.Snapshot(t, runID)
	if run.Revision == nil || run.Status != dag.RunStatusRunning {
		t.Fatalf("Snapshot = %s rev %v, want the engine's running snapshot",
			run.Status, run.Revision)
	}
}
