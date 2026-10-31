// inputs_place_test.go
// Methodology: real embedded NATS and bridges over one server, a task
// claimed over HTTP and the inputs.place resolve action driven like a
// non-Go worker would (ADR-027). Submissions go straight into the
// submissions bucket. Positive space: a queued steer is placed and
// returned once; the commit settles it done; a redelivery of the same
// iteration gets the earlier placement back once. Negative space: a
// second place returns nothing, a follow-up is not placed under steer,
// an unknown mode is 400 invalid_mode, and an empty result is
// {"inputs":[]} with status 200. Every wait is bounded.
package bridge

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
)

type placeBody struct {
	Inputs []struct {
		RequestID string          `json:"request_id"`
		Mode      string          `json:"mode"`
		Content   json.RawMessage `json:"content"`
	} `json:"inputs"`
}

func (r *journalRig) submitMode(t *testing.T, runID, stepID, requestID string, mode inputs.Mode) *inputs.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := inputs.Open(ctx, r.js)
	if err != nil {
		t.Fatalf("inputs.Open: %v", err)
	}
	if _, _, err := store.Submit(ctx, inputs.Submission{
		RunID: runID, StepID: stepID, RequestID: requestID, Mode: mode,
		Content: json.RawMessage(`{"text":"hi"}`),
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	return store
}

func decodePlace(t *testing.T, status int, body string) placeBody {
	t.Helper()
	var out placeBody
	if status != 200 || json.Unmarshal([]byte(body), &out) != nil {
		t.Fatalf("inputs.place = %d %s, want 200 with inputs", status, body)
	}
	return out
}

func TestBridgeInputsPlaceReturnsOnceAndSettlesOnCommit(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	const runID, stepID = "run-place", "step-place"
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"})
	taskID := pollOne(t, ts)
	store := rig.submitMode(t, runID, stepID, "q1", inputs.ModeSteer)
	rig.submitMode(t, runID, stepID, "f1", inputs.ModeFollowUp)

	status, body := resolveBody(t, ts, taskID, `{"action":"inputs.place","mode":"steer"}`)
	placed := decodePlace(t, status, body)
	if len(placed.Inputs) != 1 || placed.Inputs[0].RequestID != "q1" ||
		placed.Inputs[0].Mode != "steer" || string(placed.Inputs[0].Content) != `{"text":"hi"}` {
		t.Fatalf("first place = %s, want only q1 steer with content", body)
	}
	status, body = resolveBody(t, ts, taskID, `{"action":"inputs.place","mode":"steer"}`)
	if again := decodePlace(t, status, body); len(again.Inputs) != 0 || !strings.Contains(body, `"inputs":[]`) {
		t.Fatalf("second place = %s, want {\"inputs\":[]}", body)
	}
	if status, body := resolveBody(t, ts, taskID, `{"action":"inputs.place","mode":"follow_up"}`); status != 200 ||
		len(decodePlace(t, status, body).Inputs) != 1 {
		t.Fatalf("follow_up place = %d %s, want f1", status, body)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	entry, err := store.Get(ctx, runID, stepID, "q1")
	if err != nil || entry.Status != inputs.StatusPlaced {
		t.Fatalf("q1 before commit = %+v (%v), want placed", entry.Status, err)
	}
	if status, body := resolveBody(t, ts, taskID, `{"action":"continue","output":{}}`); status != 200 {
		t.Fatalf("continue: %d %s", status, body)
	}
	for _, id := range []string{"q1", "f1"} {
		entry, err = store.Get(ctx, runID, stepID, id)
		if err != nil || entry.Status != inputs.StatusDone {
			t.Fatalf("%s after commit = %s (%v), want done", id, entry.Status, err)
		}
	}
}

func TestBridgeInputsPlaceFollowUpNotPlacedUnderSteer(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	const runID, stepID = "run-place-mode", "step-place-mode"
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"})
	taskID := pollOne(t, ts)
	store := rig.submitMode(t, runID, stepID, "f1", inputs.ModeFollowUp)

	status, body := resolveBody(t, ts, taskID, `{"action":"inputs.place","mode":"steer"}`)
	if got := decodePlace(t, status, body); len(got.Inputs) != 0 {
		t.Fatalf("steer place = %s, want nothing", body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if entry, err := store.Get(ctx, runID, stepID, "f1"); err != nil || entry.Status != inputs.StatusQueued {
		t.Fatalf("f1 = %s (%v), want still queued", entry.Status, err)
	}
	for _, mode := range []string{`"bogus"`, `""`} {
		status, body = resolveBody(t, ts, taskID, `{"action":"inputs.place","mode":`+mode+`}`)
		var got struct{ Code string }
		if status != 400 || json.Unmarshal([]byte(body), &got) != nil || got.Code != "invalid_mode" {
			t.Fatalf("mode %s = %d %s, want 400 invalid_mode", mode, status, body)
		}
	}
}

func TestBridgeInputsPlaceRedeliveryReturnsEarlierPlacementOnce(t *testing.T) {
	rig := newJournalRig(t)
	b1, ts1 := rig.front(t)
	_, ts2 := rig.front(t)
	const runID, stepID = "run-place-redo", "step-place-redo"
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"})
	task1 := pollOne(t, ts1)
	rig.submitMode(t, runID, stepID, "q1", inputs.ModeSteer)
	status, body := resolveBody(t, ts1, task1, `{"action":"inputs.place","mode":"steer"}`)
	if got := decodePlace(t, status, body); len(got.Inputs) != 1 {
		t.Fatalf("first delivery place = %s, want q1", body)
	}

	msg, ok := b1.ackMap.Load(task1)
	if !ok {
		t.Fatal("first bridge lost the claim")
	}
	if err := msg.Nak(); err != nil {
		t.Fatalf("nak: %v", err)
	}
	task2 := pollOne(t, ts2)
	status, body = resolveBody(t, ts2, task2, `{"action":"inputs.place","mode":"steer"}`)
	got := decodePlace(t, status, body)
	if len(got.Inputs) != 1 || got.Inputs[0].RequestID != "q1" {
		t.Fatalf("redelivery place = %s, want the earlier q1 once", body)
	}
	status, body = resolveBody(t, ts2, task2, `{"action":"inputs.place","mode":"steer"}`)
	if again := decodePlace(t, status, body); len(again.Inputs) != 0 {
		t.Fatalf("second place after redelivery = %s, want nothing", body)
	}
	status, body = resolveBody(t, ts1, task1, `{"action":"inputs.place","mode":"steer"}`)
	if status != 409 {
		t.Fatalf("fenced first delivery place = %d %s, want 409", status, body)
	}
}

// A transient store failure after the first claim must not lose the
// input already placed: the call answers it, and nothing is settled
// done unseen.
func TestBridgeInputsPlaceTransientErrorAfterPlacementKeepsPlacedInput(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	const runID, stepID = "run-place-fault", "step-place-fault"
	var failing = true
	b.placeSeam = func(p *journal.Placer) {
		p.ClaimFault = func(requestID string) error {
			if failing && requestID == "q2" {
				return errors.New("transient kv error")
			}
			return nil
		}
	}
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"})
	taskID := pollOne(t, ts)
	store := rig.submitMode(t, runID, stepID, "q1", inputs.ModeSteer)
	rig.submitMode(t, runID, stepID, "q2", inputs.ModeSteer)

	status, body := resolveBody(t, ts, taskID, `{"action":"inputs.place","mode":"steer"}`)
	first := decodePlace(t, status, body)
	if len(first.Inputs) != 1 || first.Inputs[0].RequestID != "q1" {
		t.Fatalf("place with q2 failing = %d %s, want 200 with q1", status, body)
	}
	failing = false
	status, body = resolveBody(t, ts, taskID, `{"action":"inputs.place","mode":"steer"}`)
	retry := decodePlace(t, status, body)
	if len(retry.Inputs) != 1 || retry.Inputs[0].RequestID != "q2" {
		t.Fatalf("retry = %s, want only q2 (q1 was already returned)", body)
	}
	if status, body := resolveBody(t, ts, taskID, `{"action":"continue","output":{}}`); status != 200 {
		t.Fatalf("continue: %d %s", status, body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, id := range []string{"q1", "q2"} {
		if e, err := store.Get(ctx, runID, stepID, id); err != nil || e.Status != inputs.StatusDone {
			t.Fatalf("%s = %s (%v), want done after being returned", id, e.Status, err)
		}
	}
}

// A delivery that dies after placing and before committing: the
// redelivery gets the placement back once, and its commit settles it.
func TestBridgeInputsPlaceCrashBeforeCommitRedeliversPlacementOnce(t *testing.T) {
	rig := newJournalRig(t)
	b1, ts1 := rig.front(t)
	_, ts2 := rig.front(t)
	const runID, stepID = "run-place-crash", "step-place-crash"
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"})
	task1 := pollOne(t, ts1)
	store := rig.submitMode(t, runID, stepID, "q1", inputs.ModeSteer)
	if status, body := resolveBody(t, ts1, task1, `{"action":"inputs.place","mode":"steer"}`); len(decodePlace(t, status, body).Inputs) != 1 {
		t.Fatalf("first place = %d %s, want q1", status, body)
	}
	msg, ok := b1.ackMap.Load(task1)
	if !ok {
		t.Fatal("first bridge lost the claim")
	}
	if err := msg.Nak(); err != nil { // the worker dies; the message comes back
		t.Fatalf("nak: %v", err)
	}

	task2 := pollOne(t, ts2)
	status, body := resolveBody(t, ts2, task2, `{"action":"inputs.place","mode":"steer"}`)
	if got := decodePlace(t, status, body); len(got.Inputs) != 1 || got.Inputs[0].RequestID != "q1" {
		t.Fatalf("redelivery place = %s, want q1 once", body)
	}
	status, body = resolveBody(t, ts2, task2, `{"action":"inputs.place","mode":"steer"}`)
	if got := decodePlace(t, status, body); len(got.Inputs) != 0 {
		t.Fatalf("second place = %s, want nothing", body)
	}
	if status, body := resolveBody(t, ts2, task2, `{"action":"continue","output":{}}`); status != 200 {
		t.Fatalf("continue: %d %s", status, body)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if e, err := store.Get(ctx, runID, stepID, "q1"); err != nil || e.Status != inputs.StatusDone {
		t.Fatalf("q1 = %s (%v), want done", e.Status, err)
	}
}
