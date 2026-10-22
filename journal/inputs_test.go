// journal/inputs_test.go
// Placement records (ADR-027): PlaceInput appends an input.placed
// record, a journal reopened on a later iteration or delivery knows
// every request_id placed so far, and a commit carries the wait its
// lifecycle event asks for. The checker's invariant 5 compares the
// journal's placements with the submissions bucket: a placed or done
// submission has exactly one record, and a step that finished leaves
// nothing queued.
//
// Methodology: PlaceInput against an embedded server through the same
// client helpers as the conformance suite; the invariant on hand-built
// records and submissions, no NATS. Each test asserts the positive and
// the negative case.
package journal

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
)

func TestPlaceInput_ReopenedJournalKnowsPlacements(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	first := open(t, c, task("run-pi", 0, 1, 1))
	if first.InputPlaced("q1") {
		t.Fatal("fresh journal reports q1 placed")
	}
	if _, err := first.PlaceInput(ctx, InputPlaced{
		RequestID: "q1", Mode: "steer", Content: json.RawMessage(`"hi"`),
	}); err != nil {
		t.Fatalf("PlaceInput: %v", err)
	}
	if !first.InputPlaced("q1") || first.InputPlaced("q2") {
		t.Fatal("placement set must hold q1 and not q2")
	}
	if _, err := first.Commit(ctx, Commit{Action: ActionContinue}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	// The next iteration, and a redelivery of it, both know q1.
	next := open(t, c, task("run-pi", 1, 1, 1))
	if !next.InputPlaced("q1") {
		t.Error("iteration 1 does not know q1 was placed in iteration 0")
	}
	redelivered := open(t, c, task("run-pi", 1, 1, 2))
	if !redelivered.InputPlaced("q1") || redelivered.InputPlaced("q2") {
		t.Error("redelivery's placement set is wrong")
	}
	placed := redelivered.PlacedInputs()
	if len(placed) != 1 || placed[0].RequestID != "q1" || placed[0].Iteration != 0 {
		t.Errorf("PlacedInputs = %+v, want q1 at iteration 0", placed)
	}
}

func TestCommit_CarriesWait(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	c := newClient(t, nc)
	ctx, cancel := context.WithTimeout(context.Background(), caseTimeout)
	defer cancel()
	j := open(t, c, task("run-cw", 0, 1, 1))
	until := time.Now().Add(time.Hour).UTC().Truncate(time.Millisecond)
	if _, err := j.Commit(ctx, Commit{
		Action: ActionContinue, Wait: &protocol.ContinueWait{Until: &until, Input: true},
	}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	again := open(t, c, task("run-cw", 0, 1, 2))
	commit, ok := again.Committed()
	if !ok || commit.Wait == nil || !commit.Wait.Input || commit.Wait.Until == nil ||
		!commit.Wait.Until.Equal(until) {
		t.Fatalf("recovered commit = %+v, want the wait", commit)
	}
	if commit.Wait.DelayMs != 0 {
		t.Error("a delay appeared that was never committed")
	}
}

func placedRec(seq uint64, iteration int, requestID string) Record {
	return rec(seq, TypeInputPlaced, iteration, 1,
		`{"request_id":"`+requestID+`","mode":"steer"}`)
}

func sub(requestID string, status inputs.Status) inputs.Entry {
	return inputs.Entry{Submission: inputs.Submission{
		RunID: "r", StepID: "s", RequestID: requestID, Status: status,
	}}
}

func TestCheckInputs_PlacedSubmissionsHaveOneRecord(t *testing.T) {
	records := []Record{
		rec(1, TypeClaim, 0, 1, `{}`), placedRec(2, 0, "q1"), commitRec(3, 0, 1, ActionComplete),
	}
	done := dag.StepState{Status: dag.StepStatusCompleted}
	clean := checkInputs(done, records, []inputs.Entry{
		sub("q1", inputs.StatusDone), sub("q2", inputs.StatusUnanswered),
	})
	if len(clean) != 0 {
		t.Errorf("clean step flagged: %v", clean)
	}
	v := checkInputs(done, records, []inputs.Entry{sub("q3", inputs.StatusPlaced)})
	if !hasViolation(v, "q3") {
		t.Errorf("placed submission without a record not flagged: %v", v)
	}
	// A placed status without a record is a claim whose writer has not
	// appended yet (or died): fine while the step runs, not once done.
	live := checkInputs(dag.StepState{Status: dag.StepStatusRunning}, records,
		[]inputs.Entry{sub("q3", inputs.StatusPlaced)})
	if len(live) != 0 {
		t.Errorf("live claim on a running step flagged: %v", live)
	}
	v = checkInputs(done, records, []inputs.Entry{sub("q1", inputs.StatusUnanswered)})
	if !hasViolation(v, "q1") {
		t.Errorf("unanswered submission with a placement record not flagged: %v", v)
	}
}

func TestCheckInputs_FinishedStepLeavesNothingQueued(t *testing.T) {
	records := []Record{rec(1, TypeClaim, 0, 1, `{}`), commitRec(2, 0, 1, ActionComplete)}
	queued := []inputs.Entry{sub("late", inputs.StatusQueued)}
	v := checkInputs(dag.StepState{Status: dag.StepStatusFailed}, records, queued)
	if !hasViolation(v, "late") {
		t.Errorf("queued submission on a finished step not flagged: %v", v)
	}
	running := checkInputs(dag.StepState{Status: dag.StepStatusRunning}, records, queued)
	if len(running) != 0 {
		t.Errorf("queued submission on a running step flagged: %v", running)
	}
}
