// internal/engine/idempotency_e2e_test.go
// Methodology: real embedded NATS via dagnatstest.Harness. Drive a
// single-step workflow to Completed through the actual worker path,
// then publish a stale step.started for the same (run, step,
// attempt). Assert the engine's idempotent-discard guard fires
// exactly once via slog and that no KV state mutates as a result.
//
// This is the regression test for issue #195. The engine's existing
// guard in handleStepStarted ("stale step.started ignored — step is
// terminal") is load-bearing safety: any future change that lets a
// post-terminal step.started overwrite Steps[i].Status must fail
// this test. The test passes on current main per the triage analysis;
// it locks in that contract.
package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
)

func TestEngine_DuplicateStepStartedAfterCompleted_NoStateMutation(t *testing.T) {
	// Install the log capture BEFORE the harness boots so any
	// engine init logging lands in the capture (and stays out of
	// neighbouring tests' captures via t.Cleanup).
	logs := dagnatstest.NewLogCapture(t)

	h := dagnatstest.NewHarness(t)
	runs := dagnatstest.NewRunFixture(h)

	runID, stepID := runs.RunSingleStepToCompletion(t)
	before := runs.Snapshot(t, runID)
	if before.Steps[stepID].Status != dag.StepStatusCompleted {
		t.Fatalf(
			"precondition: step %q must be Completed before "+
				"stale event; got %v",
			stepID, before.Steps[stepID].Status,
		)
	}
	if before.Status != dag.RunStatusCompleted {
		t.Fatalf(
			"precondition: run must be Completed before "+
				"stale event; got %v",
			before.Status,
		)
	}

	// Baseline log count BEFORE publishing the stale event. There
	// is no legitimate path from a happy-path completion that
	// would emit this warning, so the baseline is expected to be
	// zero — but assert against the delta to avoid being fragile
	// to upstream logging changes elsewhere.
	const warnMsg = "stale step.started ignored"
	baseline := logs.Hits(warnMsg)
	if baseline != 0 {
		t.Fatalf(
			"baseline %q hits = %d, want 0 (happy path "+
				"should not emit the stale-event warning)",
			warnMsg, baseline,
		)
	}

	// Publish the stale event using the same attempt the engine
	// already terminated. This is exactly the shape JetStream
	// would redeliver if a worker crashed-before-ack on its first
	// step.started publish.
	ctx, cancel := context.WithTimeout(
		t.Context(), 30*time.Second,
	)
	defer cancel()
	runs.PublishStaleStepStarted(ctx, t, runID, stepID, 1)

	// Bounded wait: the engine processes WORKFLOW_HISTORY events
	// inline; 500ms is comfortably above observed handler latency
	// without slowing CI noticeably.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if logs.Hits(warnMsg) > baseline {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	after := runs.Snapshot(t, runID)
	if after.Steps[stepID].Status != dag.StepStatusCompleted {
		t.Fatalf(
			"step %q status = %v after stale event, want "+
				"Completed (idempotency contract broken)",
			stepID, after.Steps[stepID].Status,
		)
	}
	if after.Status != before.Status {
		t.Fatalf(
			"run status mutated across stale event: "+
				"before=%v after=%v",
			before.Status, after.Status,
		)
	}
	hits := logs.Hits(warnMsg) - baseline
	if hits != 1 {
		t.Fatalf(
			"stale-step warning hits = %d after one stale "+
				"event, want exactly 1 (log path must fire "+
				"once per dropped event)",
			hits,
		)
	}
}
