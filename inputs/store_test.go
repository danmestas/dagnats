// inputs/store_test.go
// The submissions store (ADR-027): a submission is created at most once
// per request_id (KV Create), a repeat returns the original, withdrawal
// works only while queued, status moves by compare-and-swap, a step's
// submissions are enumerated from the bucket's stream, the completion
// sweep settles what is still queued, and a long-poll returns on the
// first status change or at its deadline.
//
// Methodology: one embedded NATS server per test with the bucket
// provisioned by SetupAll. Every test asserts the positive outcome and
// the negative one (the duplicate is refused, the placed input is not
// withdrawn, the untouched input is untouched). Every wait is bounded.
package inputs

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

const testTimeout = 10 * time.Second

func openStore(t *testing.T) (*Store, context.Context) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)
	t.Cleanup(cancel)
	store, err := Open(ctx, js)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return store, ctx
}

func submission(requestID string, mode Mode, content string) Submission {
	return Submission{
		RunID: "run-1", StepID: "agent", RequestID: requestID, Mode: mode,
		Content: json.RawMessage(content),
	}
}

func TestSubmit_CreatesOnceAndRepeatReturnsOriginal(t *testing.T) {
	store, ctx := openStore(t)
	first, created, err := store.Submit(ctx, submission("req-1", ModeSteer, `"a"`))
	if err != nil || !created {
		t.Fatalf("first Submit: created=%v err=%v", created, err)
	}
	if first.Status != StatusQueued || first.CreatedAt.IsZero() {
		t.Fatalf("first = %+v, want queued with a creation time", first)
	}
	again, created, err := store.Submit(ctx, submission("req-1", ModeFollowUp, `"b"`))
	if err != nil || created {
		t.Fatalf("repeat Submit: created=%v err=%v", created, err)
	}
	if string(again.Content) != `"a"` || again.Mode != ModeSteer {
		t.Errorf("repeat returned %+v, want the original submission", again)
	}
	entries, err := store.List(ctx, "run-1", "agent")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("List = %d entries, want 1", len(entries))
	}
}

func TestSubmit_RejectsInvalidRequestIDAndMode(t *testing.T) {
	store, ctx := openStore(t)
	cases := []Submission{
		submission("bad id", ModeSteer, `1`),
		submission("", ModeSteer, `1`),
		submission("ok", Mode("nudge"), `1`),
		submission("ok", ModeSteer, `not json`),
	}
	for _, sub := range cases {
		if _, _, err := store.Submit(ctx, sub); err == nil {
			t.Errorf("Submit(%+v) accepted, want an error", sub)
		}
	}
	if _, _, err := store.Submit(ctx, submission("ok-1_A", ModeSteer, `1`)); err != nil {
		t.Errorf("Submit of a valid request id failed: %v", err)
	}
}

func TestWithdraw_OnlyWhileQueued(t *testing.T) {
	store, ctx := openStore(t)
	if _, _, err := store.Submit(ctx, submission("w1", ModeSteer, `1`)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	if err := store.Withdraw(ctx, "run-1", "agent", "w1"); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if _, err := store.Get(ctx, "run-1", "agent", "w1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get after withdraw = %v, want ErrNotFound", err)
	}
	if err := store.Withdraw(ctx, "run-1", "agent", "w1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("second Withdraw = %v, want ErrNotFound", err)
	}
	// A withdrawn request id can be submitted again.
	if _, created, err := store.Submit(ctx, submission("w1", ModeSteer, `2`)); err != nil || !created {
		t.Fatalf("resubmit after withdraw: created=%v err=%v", created, err)
	}
	entry, err := store.Get(ctx, "run-1", "agent", "w1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.Transition(ctx, entry, StatusPlaced, 2); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if err := store.Withdraw(ctx, "run-1", "agent", "w1"); !errors.Is(err, ErrNotQueued) {
		t.Errorf("Withdraw of a placed input = %v, want ErrNotQueued", err)
	}
}

func TestTransition_CompareAndSwap(t *testing.T) {
	store, ctx := openStore(t)
	if _, _, err := store.Submit(ctx, submission("t1", ModeSteer, `1`)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	stale, err := store.Get(ctx, "run-1", "agent", "t1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	placed, err := store.Transition(ctx, stale, StatusPlaced, 3)
	if err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if placed.Status != StatusPlaced || placed.Iteration != 3 || placed.Revision <= stale.Revision {
		t.Fatalf("placed = %+v, want placed at iteration 3 with a newer revision", placed)
	}
	// The stale entry asks for a different status: refused.
	if _, err := store.Transition(ctx, stale, StatusUnanswered, 0); !errors.Is(err, ErrConflict) {
		t.Errorf("stale Transition = %v, want ErrConflict", err)
	}
	// The stale entry asks for the status already stored: idempotent.
	same, err := store.Transition(ctx, stale, StatusPlaced, 3)
	if err != nil || same.Status != StatusPlaced {
		t.Errorf("idempotent Transition = %+v, %v", same, err)
	}
}

func TestList_EnumeratesAStepAndQueuedFilters(t *testing.T) {
	store, ctx := openStore(t)
	for _, id := range []string{"c", "a", "b"} {
		if _, _, err := store.Submit(ctx, submission(id, ModeSteer, `1`)); err != nil {
			t.Fatalf("Submit %s: %v", id, err)
		}
	}
	other := submission("z", ModeSteer, `1`)
	other.StepID = "other"
	if _, _, err := store.Submit(ctx, other); err != nil {
		t.Fatalf("Submit other step: %v", err)
	}
	a, err := store.Get(ctx, "run-1", "agent", "a")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.Transition(ctx, a, StatusDone, 1); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	all, err := store.List(ctx, "run-1", "agent")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(all) != 3 || all[0].RequestID != "c" || all[2].RequestID != "b" {
		t.Fatalf("List = %+v, want c, a, b in creation order", all)
	}
	queued, err := store.Queued(ctx, "run-1", "agent")
	if err != nil {
		t.Fatalf("Queued: %v", err)
	}
	if len(queued) != 2 || queued[0].RequestID != "c" || queued[1].RequestID != "b" {
		t.Errorf("Queued = %+v, want c then b", queued)
	}
}

func TestSettleUnanswered_SweepsQueuedOnly(t *testing.T) {
	store, ctx := openStore(t)
	for _, id := range []string{"q1", "q2", "p1"} {
		if _, _, err := store.Submit(ctx, submission(id, ModeFollowUp, `1`)); err != nil {
			t.Fatalf("Submit %s: %v", id, err)
		}
	}
	p1, err := store.Get(ctx, "run-1", "agent", "p1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.Transition(ctx, p1, StatusPlaced, 1); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	n, err := store.SettleUnanswered(ctx, "run-1", "agent", nil)
	if err != nil || n != 2 {
		t.Fatalf("SettleUnanswered = %d, %v; want 2", n, err)
	}
	for id, want := range map[string]Status{"q1": StatusUnanswered, "q2": StatusUnanswered, "p1": StatusPlaced} {
		e, err := store.Get(ctx, "run-1", "agent", id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if e.Status != want {
			t.Errorf("%s status = %s, want %s", id, e.Status, want)
		}
	}
	again, err := store.SettleUnanswered(ctx, "run-1", "agent", nil)
	if err != nil || again != 0 {
		t.Errorf("second sweep settled %d, %v; want 0", again, err)
	}
}

func TestAwait_ReturnsOnChangeOrDeadline(t *testing.T) {
	store, ctx := openStore(t)
	if _, _, err := store.Submit(ctx, submission("a1", ModeSteer, `1`)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	start := time.Now()
	same, err := store.Await(ctx, "run-1", "agent", "a1", StatusQueued, 300*time.Millisecond)
	if err != nil || same.Status != StatusQueued {
		t.Fatalf("Await at deadline = %+v, %v; want still queued", same, err)
	}
	if time.Since(start) < 250*time.Millisecond {
		t.Errorf("Await returned after %s without a change", time.Since(start))
	}
	go func() {
		time.Sleep(100 * time.Millisecond)
		e, err := store.Get(ctx, "run-1", "agent", "a1")
		if err != nil {
			t.Errorf("Get: %v", err)
			return
		}
		if _, err := store.Transition(ctx, e, StatusPlaced, 1); err != nil {
			t.Errorf("Transition: %v", err)
		}
	}()
	changed, err := store.Await(ctx, "run-1", "agent", "a1", StatusQueued, 5*time.Second)
	if err != nil || changed.Status != StatusPlaced {
		t.Fatalf("Await = %+v, %v; want placed", changed, err)
	}
}

func TestClaim_IsAStrictCompareAndSwap(t *testing.T) {
	store, ctx := openStore(t)
	if _, _, err := store.Submit(ctx, submission("c1", ModeSteer, `1`)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	stale, err := store.Get(ctx, "run-1", "agent", "c1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	claimed, err := store.Claim(ctx, stale, 2)
	if err != nil || claimed.Status != StatusPlaced || claimed.Iteration != 2 {
		t.Fatalf("Claim = %+v, %v; want placed at iteration 2", claimed, err)
	}
	// A rival holding the stale revision is refused even though the
	// stored status is the one it asks for: two claimers must not both
	// believe they own the input.
	if _, err := store.Claim(ctx, stale, 2); !errors.Is(err, ErrConflict) {
		t.Errorf("stale Claim = %v, want ErrConflict", err)
	}
	if err := store.Withdraw(ctx, "run-1", "agent", "c1"); !errors.Is(err, ErrNotQueued) {
		t.Errorf("Withdraw after Claim = %v, want ErrNotQueued", err)
	}
	// A withdrawn input cannot be claimed.
	if _, _, err := store.Submit(ctx, submission("c2", ModeSteer, `1`)); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	read, err := store.Get(ctx, "run-1", "agent", "c2")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := store.Withdraw(ctx, "run-1", "agent", "c2"); err != nil {
		t.Fatalf("Withdraw: %v", err)
	}
	if _, err := store.Claim(ctx, read, 1); !errors.Is(err, ErrConflict) && !errors.Is(err, ErrNotFound) {
		t.Errorf("Claim of a withdrawn input = %v, want a refusal", err)
	}
}

func TestSubmit_CapsContent(t *testing.T) {
	store, ctx := openStore(t)
	big := submission("big", ModeSteer, `"`+string(make([]byte, ContentMaxBytes))+`"`)
	for i := range big.Content {
		if big.Content[i] == 0 {
			big.Content[i] = 'x'
		}
	}
	_, _, err := store.Submit(ctx, big)
	if !errors.Is(err, ErrContentTooLarge) {
		t.Fatalf("Submit over the cap = %v, want ErrContentTooLarge", err)
	}
	fits := submission("fits", ModeSteer, `"`+string(bytesOf('y', ContentMaxBytes-2))+`"`)
	if _, created, err := store.Submit(ctx, fits); err != nil || !created {
		t.Fatalf("Submit at the cap: created=%v err=%v", created, err)
	}
}

func bytesOf(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

func TestAwait_UnknownKeyFailsAtOnce(t *testing.T) {
	store, ctx := openStore(t)
	start := time.Now()
	_, err := store.Await(ctx, "run-1", "agent", "never", StatusQueued, 5*time.Second)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Await of an unknown key = %v, want ErrNotFound", err)
	}
	if time.Since(start) > time.Second {
		t.Errorf("Await waited %s for a key that does not exist", time.Since(start))
	}
}

func TestSettleUnanswered_SettlesUnrecordedClaims(t *testing.T) {
	store, ctx := openStore(t)
	for _, id := range []string{"dead", "live"} {
		if _, _, err := store.Submit(ctx, submission(id, ModeSteer, `1`)); err != nil {
			t.Fatalf("Submit %s: %v", id, err)
		}
		entry, err := store.Get(ctx, "run-1", "agent", id)
		if err != nil {
			t.Fatalf("Get %s: %v", id, err)
		}
		if _, err := store.Claim(ctx, entry, 1); err != nil {
			t.Fatalf("Claim %s: %v", id, err)
		}
	}
	recorded := func(id string) bool { return id == "live" }
	n, err := store.SettleUnanswered(ctx, "run-1", "agent", recorded)
	if err != nil || n != 1 {
		t.Fatalf("SettleUnanswered = %d, %v; want 1", n, err)
	}
	for id, want := range map[string]Status{"dead": StatusUnanswered, "live": StatusPlaced} {
		e, err := store.Get(ctx, "run-1", "agent", id)
		if err != nil || e.Status != want {
			t.Errorf("%s = %s (%v), want %s", id, e.Status, err, want)
		}
	}
	// Without a record set only queued inputs are settled.
	if n, err := store.SettleUnanswered(ctx, "run-1", "agent", nil); err != nil || n != 0 {
		t.Errorf("nil recorded settled %d, %v; want 0", n, err)
	}
}
