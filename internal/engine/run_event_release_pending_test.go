// internal/engine/run_event_release_pending_test.go
// White-box (package engine) tests for #648: when afterPersist (the
// caller's admission-release logic) fails after the terminal snapshot
// is already saved, finalizeRun must still publish both terminal
// events and durably record the debt (WorkflowRun.ReleasePending) so
// the reconciler can recover the release later. A normal finalize
// (afterPersist nil or succeeding) must never set the flag.
// Methodology: real embedded NATS/JetStream (SnapshotStore + a real
// TracingPublisher), matching run_event_finalize_test.go's harness.
package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go"
)

func TestFinalizeRun_AfterPersistError_PublishesEventsAndRecordsDebt(t *testing.T) {
	tp, store, nc := newFinalizeRunTestDeps(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	run := dag.WorkflowRun{
		RunID:      "run-release-pending-1",
		WorkflowID: "release-pending-wf",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{},
		CreatedAt:  time.Now().UTC(),
	}

	runEventSub, err := js.SubscribeSync(
		runEventSubject(run.WorkflowID, run.RunID, "completed"),
		nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync event.run.*: %v", err)
	}
	historySub, err := js.SubscribeSync(
		"history."+run.RunID, nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync history.*: %v", err)
	}

	releaseErr := errors.New("simulated admission release failure")
	afterPersist := func(ctx context.Context) error {
		return releaseErr
	}

	_, err = finalizeRun(
		context.Background(), tp, saveFn(store), run,
		dag.RunStatusCompleted, "", afterPersist,
	)
	// Positive: the release failure is converted into a durably
	// recorded debt, not propagated as a caller-visible error — a
	// caller-visible error here would trigger the exact redelivery
	// path that the #648 bug report shows never actually retries the
	// release (Status is already terminal by the time redelivery
	// hits the caller's early-return).
	if err != nil {
		t.Fatalf("finalizeRun: got error %v, want nil (debt recorded instead)", err)
	}

	// Positive: both terminal events were still published — the run
	// IS terminal and consumers must hear it regardless of the
	// release outcome.
	if _, subErr := runEventSub.NextMsg(5 * time.Second); subErr != nil {
		t.Fatalf("expected event.run.* despite afterPersist error: %v", subErr)
	}
	if _, subErr := historySub.NextMsg(5 * time.Second); subErr != nil {
		t.Fatalf("expected history.* despite afterPersist error: %v", subErr)
	}

	// Positive: the debt is durably recorded on the persisted run.
	persisted, loadErr := store.Load(context.Background(), run.RunID)
	if loadErr != nil {
		t.Fatalf("Load: %v", loadErr)
	}
	if !persisted.ReleasePending {
		t.Fatal("persisted run: ReleasePending = false, want true after afterPersist error")
	}
	if persisted.Status != dag.RunStatusCompleted {
		t.Fatalf("persisted status = %v, want Completed", persisted.Status)
	}
}

func TestFinalizeRun_NormalPath_NeverSetsReleasePending(t *testing.T) {
	tp, store, _ := newFinalizeRunTestDeps(t)

	run := dag.WorkflowRun{
		RunID:      "run-release-pending-2",
		WorkflowID: "release-pending-wf",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{},
		CreatedAt:  time.Now().UTC(),
	}

	afterPersistCalled := false
	afterPersist := func(ctx context.Context) error {
		afterPersistCalled = true
		return nil
	}

	if _, err := finalizeRun(
		context.Background(), tp, saveFn(store), run,
		dag.RunStatusCompleted, "", afterPersist,
	); err != nil {
		t.Fatalf("finalizeRun: %v", err)
	}
	if !afterPersistCalled {
		t.Fatal("afterPersist was never called")
	}

	persisted, err := store.Load(context.Background(), run.RunID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	// Positive: a run whose release succeeded is not flagged.
	if persisted.ReleasePending {
		t.Fatal("persisted run: ReleasePending = true, want false after a normal finalize")
	}
	// Negative: a nil afterPersist (no release logic at all, e.g. the
	// admissionSkip / failed-start finalizeRun sites) must also never
	// set the flag.
	run2 := run
	run2.RunID = "run-release-pending-3"
	if _, err := finalizeRun(
		context.Background(), tp, saveFn(store), run2,
		dag.RunStatusFailed, "", nil,
	); err != nil {
		t.Fatalf("finalizeRun(nil afterPersist): %v", err)
	}
	persisted2, err := store.Load(context.Background(), run2.RunID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if persisted2.ReleasePending {
		t.Fatal("persisted run: ReleasePending = true, want false when afterPersist is nil")
	}
}
