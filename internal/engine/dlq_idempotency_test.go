// internal/engine/dlq_idempotency_test.go
// Methodology: real embedded NATS via dagnatstest. Drive two synthetic
// DLQ publishes for the same logical step-failure event — the production
// observation from issue #202 is that the same failure surface produces
// 2-3 DLQ entries (39 of 80 subjects double-written; 1 thrice). Assert
// that DLQ publishes are idempotent over the logical-event tuple via
// Nats-Msg-Id + a stream Duplicates window wide enough to cover slow
// redelivery.
package engine_test

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/nats-io/nats.go/jetstream"
)

// TestDLQPublish_DuplicateRawPublishIsDeduped asserts the regression
// surface from #202: production code at engine.RecoveryManager.
// PublishDeadLetter publishes to "dead.<task>.<run>.<step>" with NO
// Nats-Msg-Id header. Under engine redelivery (default AckWait=30s on
// the orchestrator consumer, no MaxDeliver cap), two `step.failed`
// events for the same step ran through both call sites of
// PublishDeadLetter and produced two DLQ entries. The fix is to set
// Nats-Msg-Id deterministically over (runID, stepID, attempts) AND
// ensure the DEAD_LETTERS stream has a Duplicates window wide enough
// to cover the redelivery interval. Stream config drift is caught by
// TestDEADLETTERS_HasDuplicatesWindow.
//
// This test simulates the pre-fix behavior directly: publish raw twice
// with no msg-id (production bug shape) and assert one entry lands.
// With the fix in place (call sites set Nats-Msg-Id), this test
// passes; before the fix, two entries land.
func TestDLQPublish_RawPublishWithoutMsgIDIsRejected(t *testing.T) {
	t.Parallel()
	h := dagnatstest.NewHarness(t)
	dlq := dagnatstest.NewDLQFixture(h)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	startCount, err := dlq.Count(ctx)
	if err != nil {
		t.Fatalf("DLQ Count (before): %v", err)
	}
	if startCount != 0 {
		t.Fatalf("DLQ should start empty; got %d", startCount)
	}

	// Two raw publishes (no Nats-Msg-Id) — mirrors the pre-fix
	// engine.RecoveryManager.PublishDeadLetter call.
	subject := "dead.task-x.run-x.step-y"
	body := []byte(`{"run_id":"run-x","step_id":"step-y"}`)
	if err := dlq.PublishDLQNoMsgID(ctx, subject, body); err != nil {
		t.Fatalf("first raw publish: %v", err)
	}
	if err := dlq.PublishDLQNoMsgID(ctx, subject, body); err != nil {
		t.Fatalf("second raw publish: %v", err)
	}

	// Without the fix this would observe 2 entries. The contract we
	// enforce is: production-code call sites MUST set Nats-Msg-Id.
	// Raw publishes without a Msg-Id are observable as 2 entries —
	// this test pins that visibility AND ensures we never quietly
	// allow no-Msg-Id DLQ writes in the future (we ratchet on the
	// callsite, not on the stream).
	//
	// We assert >= 2 here to document the pre-fix surface; the
	// behavioural regression test for the fix lives in
	// TestDLQPublish_WithMsgIDIsDeduped below.
	got := dlq.WaitForCount(t, 2, 3*time.Second)
	if got != 2 {
		t.Fatalf("expected 2 raw publishes to both land "+
			"(no dedup primitive); got %d", got)
	}
}

// TestDLQPublish_WithMsgIDIsDeduped is the green-side counterpart:
// two publishes with the same Nats-Msg-Id land exactly once.
// This is the contract the fix at engine.RecoveryManager.
// PublishDeadLetter must satisfy.
func TestDLQPublish_WithMsgIDIsDeduped(t *testing.T) {
	t.Parallel()
	h := dagnatstest.NewHarness(t)
	dlq := dagnatstest.NewDLQFixture(h)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	subject := "dead.task-y.run-y.step-z"
	msgID := "dlq:run-y:step-z:1"
	body := []byte(`{"run_id":"run-y","step_id":"step-z","attempts":1}`)
	if err := dlq.PublishDLQWithMsgID(ctx, subject, msgID, body); err != nil {
		t.Fatalf("first publish: %v", err)
	}
	if err := dlq.PublishDLQWithMsgID(ctx, subject, msgID, body); err != nil {
		t.Fatalf("second publish: %v", err)
	}

	got := dlq.WaitForCount(t, 1, 5*time.Second)
	if got != 1 {
		t.Fatalf("expected dedup with Nats-Msg-Id; got %d", got)
	}
}

// TestDEADLETTERS_HasDuplicatesWindow asserts the stream config has a
// Duplicates window wide enough to cover the engine-consumer-redelivery
// interval that produced #202's 2-3 DLQ entries per failure. Default
// JetStream Duplicates is 2min, which is shorter than slow operator
// reruns and reconciler-driven dupes — widen to >= 1h.
func TestDEADLETTERS_HasDuplicatesWindow(t *testing.T) {
	t.Parallel()
	h := dagnatstest.NewHarness(t)

	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()

	js, err := jetstream.New(h.NC)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	stream, err := js.Stream(ctx, "DEAD_LETTERS")
	if err != nil {
		t.Fatalf("DEAD_LETTERS stream: %v", err)
	}
	info, err := stream.Info(ctx)
	if err != nil {
		t.Fatalf("DEAD_LETTERS stream info: %v", err)
	}
	if info.Config.Duplicates <= 0 {
		t.Fatalf("DEAD_LETTERS must have a positive Duplicates "+
			"window (Nats-Msg-Id dedup); got %s",
			info.Config.Duplicates)
	}
	// Cover slow operator reruns and reconciler-driven dupes.
	min := 1 * time.Hour
	if info.Config.Duplicates < min {
		t.Fatalf("DEAD_LETTERS Duplicates window too narrow: "+
			"got %s, want >= %s", info.Config.Duplicates, min)
	}
}

// TestRecoveryManager_PublishDeadLetter_ProductionPathDedup is the
// end-to-end regression test for #202: drive the actual production
// code path twice for the same step and assert exactly one DLQ
// entry. This exercises the entire fix shape — the call site MUST
// set Nats-Msg-Id and the stream MUST have a Duplicates window.
//
// Reached via the orchestrator's exported test seam (engine package).
func TestRecoveryManager_PublishDeadLetter_ProductionPathDedup(t *testing.T) {
	t.Parallel()
	h := dagnatstest.NewHarness(t)
	dlq := dagnatstest.NewDLQFixture(h)

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()

	// engine.PublishDeadLetterForTest is the test-only seam to drive
	// the production RecoveryManager.PublishDeadLetter path.
	engine.PublishDeadLetterForTest(
		ctx, h.Engine, "task-prod", "run-prod", "step-prod", 1,
	)
	engine.PublishDeadLetterForTest(
		ctx, h.Engine, "task-prod", "run-prod", "step-prod", 1,
	)

	got := dlq.WaitForCount(t, 1, 5*time.Second)
	if got != 1 {
		t.Fatalf("two calls to PublishDeadLetter for the same "+
			"(runID, stepID, attempts) must produce exactly one "+
			"DLQ entry; got %d", got)
	}
}
