// bridge/journal_view_test.go
// Delivery identity and the per-iteration journal view (#849): what a
// non-Go client needs to apply the ADR-026 replay rules itself.
//
// Methodology: real embedded NATS and two Bridge instances over one
// server, as in journal_test.go. Redelivery is forced by NAKing the
// first bridge's held message. Responses are decoded by their literal
// wire keys. Positive space: a poll item's delivered and stream_seq
// are the ones in the claim the bridge writes for that delivery, and
// journal.view returns exactly this iteration's records before the
// claim. Negative space: a redelivery's identity differs from the
// first delivery's, and the view leaves out earlier iterations, the
// task's own claim and what it appended after claiming.
package bridge

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
)

// journalView is the journal.view reply, by its wire keys.
type journalView struct {
	Claim   map[string]json.RawMessage `json:"claim"`
	Records []journal.Record           `json:"records"`
}

func resolveView(t *testing.T, ts *httptest.Server, taskID string) journalView {
	t.Helper()
	status, body := resolveBody(t, ts, taskID, `{"action":"journal.view"}`)
	var view journalView
	if status != 200 {
		t.Fatalf("journal.view: %d %s", status, body)
	}
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatalf("decode journal.view %s: %v", body, err)
	}
	return view
}

func TestBridgePollItemCarriesTheDeliveryIdentityOfItsClaim(t *testing.T) {
	rig := newJournalRig(t)
	b1, ts1 := rig.front(t)
	_, ts2 := rig.front(t)
	const runID, stepID = "run-jident", "step-jident"
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"})
	first := postPoll(t, ts1, "echo", 1, 3000)
	if len(first) != 1 || first[0].Delivered != 1 || first[0].StreamSeq == 0 {
		t.Fatalf("first poll = %+v, want delivered 1 and a stream_seq", first)
	}
	status, body := resolveBody(t, ts1, first[0].TaskID, `{"action":"journal.read"}`)
	var read journalReadResponse
	if status != 200 || json.Unmarshal([]byte(body), &read) != nil || len(read.Records) != 1 {
		t.Fatalf("journal.read: %d %s", status, body)
	}
	claim := read.Records[0]
	if claim.Delivered != first[0].Delivered || claim.StreamSeq != first[0].StreamSeq {
		t.Fatalf("claim %+v does not carry poll identity %+v", claim, first[0])
	}

	msg, ok := b1.ackMap.Load(first[0].TaskID)
	if !ok {
		t.Fatal("first bridge lost the claim")
	}
	if err := msg.Nak(); err != nil {
		t.Fatalf("nak: %v", err)
	}
	second := postPoll(t, ts2, "echo", 1, 3000)
	if len(second) != 1 || second[0].Delivered != 2 || second[0].StreamSeq != first[0].StreamSeq {
		t.Fatalf("redelivery = %+v, want delivered 2 on stream_seq %d", second, first[0].StreamSeq)
	}
	view := resolveView(t, ts2, second[0].TaskID)
	if string(view.Claim["delivered"]) != "2" ||
		string(view.Claim["stream_seq"]) != jsonUint(first[0].StreamSeq) {
		t.Fatalf("view claim = %v, want delivered 2 stream_seq %d", view.Claim, first[0].StreamSeq)
	}
	if len(view.Records) != 1 || view.Records[0].Delivered != 1 {
		t.Fatalf("view records = %+v, want only the first delivery's claim", view.Records)
	}
}

func TestBridgeJournalViewIsThisIterationBeforeTheClaim(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	const runID, stepID = "run-jview", "step-jview"
	ctx := context.Background()
	pub := natsutil.NewTracingPublisher(rig.nc, rig.js)
	prior, err := journal.Open(ctx, rig.js, pub, journal.Task{
		RunID: runID, StepID: stepID, Iteration: 0, Attempt: 1, DispatchNonce: "n0", Delivered: 1,
	})
	if err != nil {
		t.Fatalf("open iteration 0: %v", err)
	}
	if _, err := prior.Commit(ctx, journal.Commit{Action: journal.ActionContinue}); err != nil {
		t.Fatalf("commit iteration 0: %v", err)
	}
	// A crashed holder of iteration 1 left a claim and an intent.
	crashed, err := journal.Open(ctx, rig.js, pub, journal.Task{
		RunID: runID, StepID: stepID, Iteration: 1, Attempt: 1, DispatchNonce: "n1",
		Delivered: 1, StreamSeq: 999,
	})
	if err != nil {
		t.Fatalf("open crashed holder: %v", err)
	}
	intent := `{"name":"send","call":0,"key":"k","replay":"unsafe","tool_call_id":"tc-1"}`
	if _, err := crashed.Append(ctx, journal.TypeToolIntent, []byte(intent)); err != nil {
		t.Fatalf("append intent: %v", err)
	}
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, Iteration: 1,
		Attempt: 1, DispatchNonce: "n1"})
	status, raw := postPollRaw(t, ts, `{"task_types":["echo"],"max_tasks":1,"timeout_ms":3000}`)
	var items []map[string]json.RawMessage
	if status != 200 || json.Unmarshal([]byte(raw), &items) != nil || len(items) != 1 {
		t.Fatalf("poll: %d %s", status, raw)
	}
	if string(items[0]["delivered"]) != "1" || len(items[0]["stream_seq"]) == 0 {
		t.Fatalf("poll item %s lacks delivered 1 and stream_seq", raw)
	}
	var taskID string
	if err := json.Unmarshal(items[0]["task_id"], &taskID); err != nil {
		t.Fatalf("task_id: %v", err)
	}

	view := resolveView(t, ts, taskID)
	if len(view.Records) != 2 || view.Records[0].Type != journal.TypeClaim ||
		view.Records[1].Type != journal.TypeToolIntent {
		t.Fatalf("view records = %+v, want the crashed claim and its intent", view.Records)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(view.Records[1].Data, &body); err != nil ||
		string(body["tool_call_id"]) != `"tc-1"` {
		t.Fatalf("intent data = %s, want tool_call_id preserved", view.Records[1].Data)
	}
	for _, r := range view.Records {
		if r.Iteration != 1 {
			t.Fatalf("view carries iteration %d record %+v", r.Iteration, r)
		}
	}
	if string(view.Claim["iteration"]) != "1" || string(view.Claim["attempt"]) != "1" {
		t.Fatalf("view claim = %v, want iteration 1 attempt 1", view.Claim)
	}
	if status, body := resolveBody(t, ts, taskID,
		`{"action":"journal.append","type":"entry","data":{"k":1}}`); status != 200 {
		t.Fatalf("append: %d %s", status, body)
	}
	again := resolveView(t, ts, taskID)
	if len(again.Records) != 2 {
		t.Fatalf("view after append = %+v, want it fixed at the claim", again.Records)
	}
}

func jsonUint(v uint64) string {
	data, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(data)
}

func TestBridgeJournalViewAnswersTheFence(t *testing.T) {
	testJournalActionAnswersTheFence(t, "journal.view")
}

func TestBridgeJournalReadAnswersTheFence(t *testing.T) {
	testJournalActionAnswersTheFence(t, "journal.read")
}

// testJournalActionAnswersTheFence claims through action on a first
// delivery, lets a redelivery claim over it, and expects the replaced
// delivery's next action to answer 409 fenced while the successor's
// still answers 200.
func testJournalActionAnswersTheFence(t *testing.T, action string) {
	rig := newJournalRig(t)
	b1, ts1 := rig.front(t)
	_, ts2 := rig.front(t)
	runID := "run-fence-" + strings.ReplaceAll(action, ".", "-")
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: "step", DispatchNonce: "n1"})
	body := `{"action":"` + action + `"}`
	task1 := pollOne(t, ts1)
	if status, raw := resolveBody(t, ts1, task1, body); status != 200 {
		t.Fatalf("first %s: %d %s", action, status, raw)
	}
	msg, ok := b1.ackMap.Load(task1)
	if !ok {
		t.Fatal("first bridge lost the claim")
	}
	if err := msg.Nak(); err != nil {
		t.Fatalf("nak: %v", err)
	}
	task2 := pollOne(t, ts2)
	if status, raw := resolveBody(t, ts2, task2, body); status != 200 {
		t.Fatalf("successor %s: %d %s", action, status, raw)
	}

	status, raw := resolveBody(t, ts1, task1, body)
	if status != http.StatusConflict || !strings.Contains(raw, `"fenced"`) {
		t.Fatalf("fenced %s = %d %s, want 409 fenced", action, status, raw)
	}
	if status, raw := resolveBody(t, ts2, task2, body); status != 200 {
		t.Fatalf("successor %s after the fence: %d %s", action, status, raw)
	}
}
