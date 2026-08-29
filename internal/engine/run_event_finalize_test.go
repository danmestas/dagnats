// internal/engine/run_event_finalize_test.go
// White-box (package engine) tests for finalizeRun's two review-round-2
// fixes (#625): afterPersist runs strictly before either publish, and a
// second terminal transition for an already-terminal run is a no-op
// (first status wins) rather than double-publishing or drifting the
// snapshot behind the one event already sent.
// Methodology: real embedded NATS/JetStream (SnapshotStore + a real
// TracingPublisher). Ordering is asserted deterministically — not via a
// wall-clock race — by having afterPersist itself probe the EVENTS
// subscription with a short bounded MaxWait and assert no message has
// arrived yet, which can only be true if finalizeRun calls afterPersist
// before it publishes.
package engine

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func newFinalizeRunTestDeps(t *testing.T) (
	*natsutil.TracingPublisher, *SnapshotStore, *nats.Conn,
) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	jsc, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tp := natsutil.NewTracingPublisher(nc, jsc)
	store := NewSnapshotStore(jsc)
	return tp, store, nc
}

// saveFn adapts *SnapshotStore.Save (ctx, run) to SaveSnapshotFunc's
// (ctx, run, stepID) shape for tests — stepID is only used elsewhere
// for per-step latency labeling, irrelevant to these workflow-scoped
// saves (mirrors the "" stepID finalizeRun callers already pass here).
func saveFn(store *SnapshotStore) SaveSnapshotFunc {
	return func(
		ctx context.Context, run dag.WorkflowRun, stepID string,
	) error {
		return store.Save(ctx, run)
	}
}

func TestFinalizeRun_AfterPersistRunsBeforeEitherPublish(t *testing.T) {
	tp, store, nc := newFinalizeRunTestDeps(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	run := dag.WorkflowRun{
		RunID:      "run-order-1",
		WorkflowID: "order-wf",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{},
		CreatedAt:  time.Now().UTC(),
	}
	subject := runEventSubject(run.WorkflowID, run.RunID, "completed")
	sub, err := js.SubscribeSync(subject, nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	afterPersistCalled := false
	afterPersist := func(ctx context.Context) error {
		afterPersistCalled = true
		// Positive: the snapshot is already durable by the time
		// afterPersist runs (finalizeRun calls saveFn before this).
		loaded, loadErr := store.Load(ctx, run.RunID)
		if loadErr != nil {
			t.Fatalf("Load during afterPersist: %v", loadErr)
		}
		if loaded.Status != dag.RunStatusCompleted {
			t.Fatalf(
				"during afterPersist: persisted status = %v, want Completed",
				loaded.Status,
			)
		}
		// Negative: no event.run.* message has been published yet — if
		// finalizeRun published before calling afterPersist, this would
		// find a message instead of timing out.
		if _, err := sub.NextMsg(50 * time.Millisecond); err == nil {
			t.Fatal(
				"afterPersist observed a published event.run.* message " +
					"— publish must not happen before afterPersist runs",
			)
		}
		return nil
	}

	if _, err := finalizeRun(
		context.Background(), tp, store, saveFn(store), run,
		dag.RunStatusCompleted, "", afterPersist,
	); err != nil {
		t.Fatalf("finalizeRun: %v", err)
	}
	if !afterPersistCalled {
		t.Fatal("afterPersist was never called")
	}

	// Now that finalizeRun has returned, the event must exist.
	if _, err := sub.NextMsg(5 * time.Second); err != nil {
		t.Fatalf("expected event.run.* message after finalizeRun returned: %v", err)
	}
}

func TestFinalizeRun_AlreadyTerminalRun_FirstStatusWins(t *testing.T) {
	tp, store, nc := newFinalizeRunTestDeps(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	run := dag.WorkflowRun{
		RunID:      "run-double-terminal-1",
		WorkflowID: "double-wf",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{},
		CreatedAt:  time.Now().UTC(),
	}

	failedSub, err := js.SubscribeSync(
		runEventSubject(run.WorkflowID, run.RunID, "failed"),
		nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync failed-subject: %v", err)
	}
	cancelledSub, err := js.SubscribeSync(
		runEventSubject(run.WorkflowID, run.RunID, "cancelled"),
		nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync cancelled-subject: %v", err)
	}

	ctx := context.Background()
	afterFailed, err := finalizeRun(
		ctx, tp, store, saveFn(store), run, dag.RunStatusFailed, "", nil,
	)
	if err != nil {
		t.Fatalf("finalizeRun(Failed): %v", err)
	}

	// Second terminal transition for the SAME already-terminal run
	// (simulates cancel-vs-fail racing to finalize the same run).
	afterCancelled, err := finalizeRun(
		ctx, tp, store, saveFn(store), afterFailed, dag.RunStatusCancelled, "", nil,
	)
	if err != nil {
		t.Fatalf("finalizeRun(Cancelled): %v", err)
	}

	// Positive: the first status wins, both in the returned run and in
	// what's persisted.
	if afterCancelled.Status != dag.RunStatusFailed {
		t.Fatalf(
			"second finalizeRun call status = %v, want Failed (first wins)",
			afterCancelled.Status,
		)
	}
	persisted, err := store.Load(ctx, run.RunID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if persisted.Status != dag.RunStatusFailed {
		t.Fatalf("persisted status = %v, want Failed", persisted.Status)
	}

	// Positive: exactly one run.failed event was published.
	if _, err := failedSub.NextMsg(5 * time.Second); err != nil {
		t.Fatalf("expected one run.failed event: %v", err)
	}
	if _, err := failedSub.NextMsg(300 * time.Millisecond); err == nil {
		t.Fatal("expected only one run.failed event, got a second")
	}

	// Negative: no run.cancelled event was ever published.
	if _, err := cancelledSub.NextMsg(300 * time.Millisecond); err == nil {
		t.Fatal("expected no run.cancelled event — first status (Failed) must win")
	}
}
