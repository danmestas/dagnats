// worker/inputs_test.go
// The worker side of agent inputs and durable waits (ADR-027):
// PlaceInputs moves a mode's queued inputs into the journal exactly
// once across a crash between the journal append and the status
// projection and across a redelivery; the projection moves to placed
// and, on commit, to done; Continue refuses UntilInput while inputs
// are queued; Delay, Until and UntilInput ride the step.continue
// payload and the commit, and a republished commit carries them too.
//
// Methodology: taskContexts built from a TaskPayload the way the
// Worker builds one (journal_test.go's delivery helper), against an
// embedded NATS server with the journal and the submissions bucket
// provisioned. The API is played by writing submissions through the
// inputs store. Every wait is bounded.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

func inputsStore(t *testing.T, js jetstream.JetStream) *inputs.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	store, err := inputs.Open(ctx, js)
	if err != nil {
		t.Fatalf("inputs.Open: %v", err)
	}
	return store
}

func submit(t *testing.T, store *inputs.Store, runID, requestID string, mode inputs.Mode) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	_, created, err := store.Submit(ctx, inputs.Submission{
		RunID: runID, StepID: "s", RequestID: requestID, Mode: mode,
		Content: json.RawMessage(`"` + requestID + `"`),
	})
	if err != nil || !created {
		t.Fatalf("Submit %s: created=%v err=%v", requestID, created, err)
	}
}

func status(t *testing.T, store *inputs.Store, runID, requestID string) inputs.Entry {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	entry, err := store.Get(ctx, runID, "s", requestID)
	if err != nil {
		t.Fatalf("Get %s: %v", requestID, err)
	}
	return entry
}

func countPlaced(records []journal.Record, requestID string) int {
	n := 0
	for _, r := range records {
		if r.Type != journal.TypeInputPlaced {
			continue
		}
		var p journal.InputPlaced
		if json.Unmarshal(r.Data, &p) == nil && p.RequestID == requestID {
			n++
		}
	}
	return n
}

func TestInputs_PlaceByModeProjectsPlacedThenDone(t *testing.T) {
	nc, js := journalServer(t)
	store := inputsStore(t, js)
	submit(t, store, "run-ip", "steer-1", inputs.ModeSteer)
	submit(t, store, "run-ip", "follow-1", inputs.ModeFollowUp)
	tc := delivery(t, nc, journalPayload("run-ip", 0), 1)
	if _, err := tc.PlaceInputs(inputs.ModeSteer); !errors.Is(err, ErrNoJournal) {
		t.Fatalf("PlaceInputs without a journal = %v, want ErrNoJournal", err)
	}
	if _, err := tc.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	placed, err := tc.PlaceInputs(inputs.ModeSteer)
	if err != nil {
		t.Fatalf("PlaceInputs: %v", err)
	}
	if len(placed) != 1 || placed[0].RequestID != "steer-1" || string(placed[0].Content) != `"steer-1"` {
		t.Fatalf("placed = %+v, want steer-1 with its content", placed)
	}
	if e := status(t, store, "run-ip", "steer-1"); e.Status != inputs.StatusPlaced {
		t.Errorf("steer-1 status = %s, want placed", e.Status)
	}
	if e := status(t, store, "run-ip", "follow-1"); e.Status != inputs.StatusQueued {
		t.Errorf("follow-1 status = %s, want still queued", e.Status)
	}
	if err := tc.Continue(nil); err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if e := status(t, store, "run-ip", "steer-1"); e.Status != inputs.StatusDone {
		t.Errorf("steer-1 status after commit = %s, want done", e.Status)
	}
	if countPlaced(readJournal(t, js, "run-ip"), "steer-1") != 1 {
		t.Error("journal does not hold exactly one input.placed for steer-1")
	}
}

func TestInputs_ExactlyOnceAcrossCrashAndRedelivery(t *testing.T) {
	nc, js := journalServer(t)
	store := inputsStore(t, js)
	submit(t, store, "run-xo", "q1", inputs.ModeSteer)
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	// Delivery 1 claims q1 in the bucket and dies before appending the
	// record: a dead claim, which only a claim of this step can leave.
	first := delivery(t, nc, journalPayload("run-xo", 0), 1)
	if _, err := first.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	entry, err := store.Get(ctx, "run-xo", "s", "q1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.Claim(ctx, entry, 0); err != nil {
		t.Fatalf("Claim: %v", err)
	}
	// Delivery 2 of the same dispatch reclaims it: placed in the bucket
	// but absent from the journal means the claim died.
	second := delivery(t, nc, journalPayload("run-xo", 0), 2)
	if _, err := second.Journal(); err != nil {
		t.Fatalf("Journal (redelivery): %v", err)
	}
	placed, err := second.PlaceInputs(inputs.ModeSteer)
	if err != nil {
		t.Fatalf("PlaceInputs (redelivery): %v", err)
	}
	if len(placed) != 1 || placed[0].RequestID != "q1" {
		t.Fatalf("redelivery placed %+v, want the dead claim's q1", placed)
	}
	if err := second.Continue(nil); err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if e := status(t, store, "run-xo", "q1"); e.Status != inputs.StatusDone {
		t.Errorf("q1 status = %s, want done", e.Status)
	}
	// The next iteration, and a redelivery of it, do not place it again.
	for delivered := 1; delivered <= 2; delivered++ {
		next := delivery(t, nc, journalPayload("run-xo", 1), delivered)
		if _, err := next.Journal(); err != nil {
			t.Fatalf("Journal (iteration 1, delivery %d): %v", delivered, err)
		}
		if placed, err := next.PlaceInputs(inputs.ModeSteer); err != nil || len(placed) != 0 {
			t.Fatalf("iteration 1 PlaceInputs = %+v, %v; want nothing", placed, err)
		}
	}
	if n := countPlaced(readJournal(t, js, "run-xo"), "q1"); n != 1 {
		t.Errorf("q1 has %d input.placed records, want exactly 1", n)
	}
}

func TestInputs_WithdrawRacingPlacementNeverPlaces(t *testing.T) {
	nc, js := journalServer(t)
	store := inputsStore(t, js)
	submit(t, store, "run-wr", "w1", inputs.ModeSteer)
	tc := delivery(t, nc, journalPayload("run-wr", 0), 1)
	if _, err := tc.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	// The withdrawal lands after PlaceInputs read w1 as queued and
	// before it claims it: the API answered 204, so w1 must not be placed.
	withdrawn := false
	tc.beforeClaim = func() {
		if err := store.Withdraw(ctx, "run-wr", "s", "w1"); err != nil {
			t.Errorf("Withdraw: %v", err)
			return
		}
		withdrawn = true
	}
	placed, err := tc.PlaceInputs(inputs.ModeSteer)
	if err != nil {
		t.Fatalf("PlaceInputs: %v", err)
	}
	if !withdrawn {
		t.Fatal("the withdrawal did not happen inside the placement window")
	}
	if len(placed) != 0 {
		t.Fatalf("placed %+v after the withdrawal was acknowledged", placed)
	}
	if n := countPlaced(readJournal(t, js, "run-wr"), "w1"); n != 0 {
		t.Errorf("journal holds %d input.placed for a withdrawn input", n)
	}
	if _, err := store.Get(ctx, "run-wr", "s", "w1"); !errors.Is(err, inputs.ErrNotFound) {
		t.Errorf("w1 after withdraw = %v, want ErrNotFound", err)
	}
}

func TestInputs_UnplaceableInputIsSettledUnanswered(t *testing.T) {
	nc, js := journalServer(t)
	store := inputsStore(t, js)
	submit(t, store, "run-up", "ok-1", inputs.ModeSteer)
	submit(t, store, "run-up", "huge", inputs.ModeSteer)
	tc := delivery(t, nc, journalPayload("run-up", 0), 1)
	if _, err := tc.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	// The append of "huge" fails for a reason that is not fencing.
	tc.placeFault = func(requestID string) error {
		if requestID == "huge" {
			return errors.New("nats: maximum payload exceeded")
		}
		return nil
	}
	placed, err := tc.PlaceInputs(inputs.ModeSteer)
	if err != nil {
		t.Fatalf("PlaceInputs: %v", err)
	}
	if len(placed) != 1 || placed[0].RequestID != "ok-1" {
		t.Fatalf("placed = %+v, want ok-1 only", placed)
	}
	if e := status(t, store, "run-up", "huge"); e.Status != inputs.StatusUnanswered {
		t.Errorf("huge status = %s, want unanswered", e.Status)
	}
	if n := countPlaced(readJournal(t, js, "run-up"), "huge"); n != 0 {
		t.Errorf("journal holds %d records for the unplaceable input", n)
	}
}

func TestInputs_ContinueUntilInputRefusesWhileQueued(t *testing.T) {
	nc, js := journalServer(t)
	store := inputsStore(t, js)
	submit(t, store, "run-ui", "q1", inputs.ModeFollowUp)
	tc := delivery(t, nc, journalPayload("run-ui", 0), 1)
	if _, err := tc.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	if err := tc.Continue(nil, UntilInput()); !errors.Is(err, ErrInputsQueued) {
		t.Fatalf("Continue(UntilInput) with q1 queued = %v, want ErrInputsQueued", err)
	}
	if records := readJournal(t, js, "run-ui"); len(records) != 1 {
		t.Fatalf("refused continue wrote %d records past the claim", len(records)-1)
	}
	if _, err := tc.PlaceInputs(inputs.ModeFollowUp); err != nil {
		t.Fatalf("PlaceInputs: %v", err)
	}
	until := time.Now().Add(time.Hour).UTC()
	if err := tc.Continue(nil, UntilInput(), Until(until)); err != nil {
		t.Fatalf("Continue(UntilInput, Until) after placing: %v", err)
	}
	evt := nextHistory(t, nc, "run-ui", protocol.EventStepContinue)
	var payload protocol.StepContinuePayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.Wait == nil || !payload.Wait.Input || payload.Wait.Until == nil ||
		!payload.Wait.Until.Equal(until) {
		t.Errorf("payload wait = %+v, want input and until %s", payload.Wait, until)
	}
	records := readJournal(t, js, "run-ui")
	var commit journal.Commit
	if err := json.Unmarshal(records[len(records)-1].Data, &commit); err != nil {
		t.Fatalf("decode commit: %v", err)
	}
	if commit.Wait == nil || !commit.Wait.Input {
		t.Errorf("commit wait = %+v, want input", commit.Wait)
	}
}

func TestInputs_DelayRidesTheContinueWithoutAJournal(t *testing.T) {
	nc, _ := journalServer(t)
	tc := delivery(t, nc, journalPayload("run-dl", 0), 1)
	if err := tc.Continue([]byte(`1`), Delay(1500*time.Millisecond)); err != nil {
		t.Fatalf("Continue(Delay): %v", err)
	}
	evt := nextHistory(t, nc, "run-dl", protocol.EventStepContinue)
	var payload protocol.StepContinuePayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.Wait == nil || payload.Wait.DelayMs != 1500 || payload.Wait.Input {
		t.Errorf("payload wait = %+v, want delay 1500 and no input", payload.Wait)
	}
	if !payload.StateSet {
		t.Error("a plain step still commits its state alongside the wait")
	}
}

func TestInputs_RepublishedCommitCarriesWait(t *testing.T) {
	nc, js := journalServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	// A previous holder of iteration 1 (where the engine stamps the
	// attempt, so recovery matches it) committed the wait and died
	// before publishing.
	committed, err := journal.Open(ctx, js, natsutil.NewTracingPublisher(nc, js), journal.Task{
		RunID: "run-rp", StepID: "s", Iteration: 1, Attempt: 1, DispatchNonce: "nonce",
		Delivered: 1,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if _, err := committed.Commit(ctx, journal.Commit{
		Action: journal.ActionContinue, Wait: &protocol.ContinueWait{DelayMs: 60_000},
	}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	second := delivery(t, nc, journalPayload("run-rp", 1), 2)
	if _, err := second.Journal(); !errors.Is(err, journal.ErrCommitted) {
		t.Fatalf("Journal on redelivery = %v, want ErrCommitted", err)
	}
	evt := nextHistory(t, nc, "run-rp", protocol.EventStepContinue)
	var payload protocol.StepContinuePayload
	if err := json.Unmarshal(evt.Payload, &payload); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if payload.Wait == nil || payload.Wait.DelayMs != 60_000 {
		t.Errorf("republished wait = %+v, want delay 60000", payload.Wait)
	}
}

func TestInputs_ClaimLostToStaleHolderIsReclaimed(t *testing.T) {
	nc, js := journalServer(t)
	store := inputsStore(t, js)
	submit(t, store, "run-sh", "x", inputs.ModeSteer)
	ctx, cancel := context.WithTimeout(context.Background(), journalTestTimeout)
	defer cancel()
	successor := delivery(t, nc, journalPayload("run-sh", 0), 2)
	if _, err := successor.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	// A stale holder claims x between the successor's read and its
	// claim; its append will be fenced, so x would stay placed with no
	// record unless the successor reclaims it.
	successor.beforeClaim = func() {
		entry, err := store.Get(ctx, "run-sh", "s", "x")
		if err != nil {
			t.Errorf("Get: %v", err)
			return
		}
		if _, err := store.Claim(ctx, entry, 0); err != nil {
			t.Errorf("stale Claim: %v", err)
		}
	}
	placed, err := successor.PlaceInputs(inputs.ModeSteer)
	if err != nil {
		t.Fatalf("PlaceInputs: %v", err)
	}
	if len(placed) != 1 || placed[0].RequestID != "x" {
		t.Fatalf("placed = %+v, want x reclaimed from the stale holder", placed)
	}
	if n := countPlaced(readJournal(t, js, "run-sh"), "x"); n != 1 {
		t.Errorf("x has %d input.placed records, want 1", n)
	}
}

// A redelivery rebuilds its conversation from the previous iteration's
// checkpoint, so the inputs an earlier delivery of this iteration
// placed must come back from PlaceInputs, once per delivery and only
// for their mode, or they vanish from the conversation.
func TestInputs_RedeliveryReturnsThisIterationsPlacements(t *testing.T) {
	nc, js := journalServer(t)
	store := inputsStore(t, js)
	submit(t, store, "run-rp", "f1", inputs.ModeFollowUp)
	first := delivery(t, nc, journalPayload("run-rp", 0), 1)
	if _, err := first.Journal(); err != nil {
		t.Fatalf("Journal: %v", err)
	}
	if placed, err := first.PlaceInputs(inputs.ModeFollowUp); err != nil || len(placed) != 1 {
		t.Fatalf("first PlaceInputs = %+v, %v; want f1", placed, err)
	}
	// Delivery 1 dies here, before its commit.
	second := delivery(t, nc, journalPayload("run-rp", 0), 2)
	if _, err := second.Journal(); err != nil {
		t.Fatalf("Journal (redelivery): %v", err)
	}
	if placed, err := second.PlaceInputs(inputs.ModeSteer); err != nil || len(placed) != 0 {
		t.Fatalf("redelivery steer = %+v, %v; want nothing", placed, err)
	}
	placed, err := second.PlaceInputs(inputs.ModeFollowUp)
	if err != nil || len(placed) != 1 || placed[0].RequestID != "f1" ||
		string(placed[0].Content) != `"f1"` {
		t.Fatalf("redelivery follow-up = %+v, %v; want f1 with its content", placed, err)
	}
	if again, err := second.PlaceInputs(inputs.ModeFollowUp); err != nil || len(again) != 0 {
		t.Fatalf("second call in one delivery = %+v, %v; want nothing", again, err)
	}
	if err := second.Continue(nil); err != nil {
		t.Fatalf("Continue: %v", err)
	}
	if e := status(t, store, "run-rp", "f1"); e.Status != inputs.StatusDone {
		t.Errorf("f1 status = %s, want done once the redelivery commits", e.Status)
	}
	if n := countPlaced(readJournal(t, js, "run-rp"), "f1"); n != 1 {
		t.Errorf("f1 has %d input.placed records, want exactly 1", n)
	}
}
