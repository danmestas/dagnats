// bridge/journal_test.go
// Methodology: real embedded NATS and two Bridge instances over one
// server, standing in for two bridge processes. A task is published,
// polled over HTTP, and the journal.append / journal.read /
// load_checkpoint resolve actions are driven like a non-Go worker
// would. Redelivery is forced by NAKing the first bridge's held
// message, so the second poller sees the same dispatch with a higher
// delivery count: the claim identity the journal fences on. Negative
// space: a fenced or stale bridge worker publishes no lifecycle event,
// and the legacy state sources are untouched on a non-journal step.
package bridge

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// journalRig is one embedded server with a bridge-side HTTP front.
type journalRig struct {
	nc *nats.Conn
	js jetstream.JetStream
}

func newJournalRig(t *testing.T) *journalRig {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc, natsutil.WithStoreBudget(storeBudgetBytes)); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return &journalRig{nc: nc, js: js}
}

// front starts a bridge and its HTTP server.
func (r *journalRig) front(t *testing.T) (*Bridge, *httptest.Server) {
	t.Helper()
	b := newTestBridge(t, r.nc)
	ts := httptest.NewServer(b.Handler())
	t.Cleanup(ts.Close)
	return b, ts
}

// publish puts one task for the journal task type on the queue.
func (r *journalRig) publish(t *testing.T, payload protocol.TaskPayload) {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := r.js.Publish(ctx, "task.echo."+payload.RunID, data); err != nil {
		t.Fatalf("publish task: %v", err)
	}
}

// poll claims one task through ts and returns its task id.
func pollOne(t *testing.T, ts *httptest.Server) string {
	t.Helper()
	tasks := postPoll(t, ts, "echo", 1, 3000)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}
	return tasks[0].TaskID
}

// resolveBody posts a resolve and returns status and body.
func resolveBody(t *testing.T, ts *httptest.Server, taskID, body string) (int, string) {
	t.Helper()
	resp, err := http.Post(ts.URL+"/v1/tasks/"+taskID+"/resolve",
		"application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("resolve: %v", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read resolve body: %v", err)
	}
	return resp.StatusCode, string(raw)
}

// countHistory counts the events of wantType on the run's history.
func countHistory(t *testing.T, nc *nats.Conn, runID string, want protocol.EventType) int {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("history stream: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		FilterSubject: "history." + runID, AckPolicy: jetstream.AckNonePolicy,
		DeliverPolicy: jetstream.DeliverAllPolicy, InactiveThreshold: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("history consumer: %v", err)
	}
	info, err := cons.Info(ctx)
	if err != nil {
		t.Fatalf("consumer info: %v", err)
	}
	n := 0
	batch, err := cons.Fetch(int(info.NumPending), jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	for m := range batch.Messages() {
		var evt protocol.Event
		if err := json.Unmarshal(m.Data(), &evt); err != nil {
			t.Fatalf("unmarshal event: %v", err)
		}
		if evt.Type == want {
			n++
		}
	}
	return n
}

type journalReadResponse struct {
	Records []journal.Record `json:"records"`
}

func TestBridgeJournalAppendReadAndCommit(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	const runID, stepID = "run-jrn", "step-jrn"
	rig.publish(t, protocol.TaskPayload{
		RunID: runID, StepID: stepID, DispatchNonce: "n1", State: []byte(`{"s":0}`),
	})
	taskID := pollOne(t, ts)

	status, body := resolveBody(t, ts, taskID, `{"action":"load_checkpoint"}`)
	var before loadCheckpointResponse
	if status != 200 || json.Unmarshal([]byte(body), &before) != nil {
		t.Fatalf("load_checkpoint before: %d %s", status, body)
	}
	if before.Source != "payload" || string(before.State) != `{"s":0}` {
		t.Fatalf("non-journal state = %s from %s, want payload {\"s\":0}", before.State, before.Source)
	}

	status, body = resolveBody(t, ts, taskID,
		`{"action":"journal.append","type":"entry","data":{"k":1}}`)
	if status != 200 || !strings.Contains(body, `"seq"`) {
		t.Fatalf("journal.append: %d %s", status, body)
	}
	status, body = resolveBody(t, ts, taskID, `{"action":"journal.read"}`)
	var read journalReadResponse
	if status != 200 || json.Unmarshal([]byte(body), &read) != nil {
		t.Fatalf("journal.read: %d %s", status, body)
	}
	if len(read.Records) != 2 || read.Records[0].Type != journal.TypeClaim ||
		read.Records[1].Type != journal.TypeEntry {
		t.Fatalf("records = %+v, want claim then entry", read.Records)
	}

	status, body = resolveBody(t, ts, taskID, `{"action":"checkpoint","data":{"s":5}}`)
	if status != 200 {
		t.Fatalf("checkpoint: %d %s", status, body)
	}
	status, _ = resolveBody(t, ts, taskID, `{"action":"continue","output":{"o":1}}`)
	if status != 200 {
		t.Fatalf("continue: %d", status)
	}
	evt := consumeHistoryEvent(t, rig.nc, runID, protocol.EventStepContinue, 5*time.Second)
	recs, err := journal.Read(context.Background(), rig.js, runID, stepID)
	if err != nil {
		t.Fatalf("journal.Read: %v", err)
	}
	last := recs[len(recs)-1]
	if last.Type != journal.TypeCommit || evt.JournalSeq != last.Seq {
		t.Fatalf("event journal_seq = %d, last record %+v", evt.JournalSeq, last)
	}
}

func TestBridgeJournalLoadCheckpointReadsJournalState(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	const runID, stepID = "run-jstate", "step-jstate"
	// A previous iteration committed state {"turn":1}.
	prior := journal.Task{RunID: runID, StepID: stepID, Iteration: 0, Attempt: 1,
		DispatchNonce: "n0", Delivered: 1}
	pub := natsutil.NewTracingPublisher(rig.nc, rig.js)
	j, err := journal.Open(context.Background(), rig.js, pub, prior)
	if err != nil {
		t.Fatalf("open prior: %v", err)
	}
	if _, err := j.Commit(context.Background(), journal.Commit{
		Action: journal.ActionContinue, State: []byte(`{"turn":1}`),
	}); err != nil {
		t.Fatalf("commit prior: %v", err)
	}
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, Iteration: 1,
		DispatchNonce: "n1", State: []byte(`{"stale":true}`)})
	taskID := pollOne(t, ts)

	status, body := resolveBody(t, ts, taskID, `{"action":"journal.read"}`)
	if status != 200 {
		t.Fatalf("journal.read: %d %s", status, body)
	}
	status, body = resolveBody(t, ts, taskID, `{"action":"load_checkpoint"}`)
	var got loadCheckpointResponse
	if status != 200 || json.Unmarshal([]byte(body), &got) != nil {
		t.Fatalf("load_checkpoint: %d %s", status, body)
	}
	if got.Source != "journal" || string(got.State) != `{"turn":1}` {
		t.Fatalf("state = %s from %s, want journal {\"turn\":1}", got.State, got.Source)
	}
}

func TestBridgeJournalLoadCheckpointReadsKV(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	const runID, stepID = "run-jkv", "step-jkv"
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, State: []byte(`{"p":1}`)})
	taskID := pollOne(t, ts)
	if _, err := b.checkpointKV.Put(context.Background(), taskID, []byte(`{"kv":1}`)); err != nil {
		t.Fatalf("put kv: %v", err)
	}
	status, body := resolveBody(t, ts, taskID, `{"action":"load_checkpoint"}`)
	var got loadCheckpointResponse
	if status != 200 || json.Unmarshal([]byte(body), &got) != nil {
		t.Fatalf("load_checkpoint: %d %s", status, body)
	}
	if got.Source != "checkpoint" || string(got.State) != `{"kv":1}` {
		t.Fatalf("state = %s from %s, want checkpoint {\"kv\":1}", got.State, got.Source)
	}
}

func TestBridgeJournalRedeliveryFencesFirstWorker(t *testing.T) {
	rig := newJournalRig(t)
	b1, ts1 := rig.front(t)
	_, ts2 := rig.front(t)
	const runID, stepID = "run-jfence", "step-jfence"
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"})
	task1 := pollOne(t, ts1)
	if status, body := resolveBody(t, ts1, task1,
		`{"action":"journal.append","type":"entry","data":{"w":1}}`); status != 200 {
		t.Fatalf("first append: %d %s", status, body)
	}
	// The stalled worker's delivery lapses: the same dispatch goes to a
	// second poller with a higher delivery count.
	msg, ok := b1.ackMap.Load(task1)
	if !ok {
		t.Fatal("first bridge lost the claim")
	}
	if err := msg.Nak(); err != nil {
		t.Fatalf("nak: %v", err)
	}
	task2 := pollOne(t, ts2)
	if status, body := resolveBody(t, ts2, task2,
		`{"action":"journal.append","type":"entry","data":{"w":2}}`); status != 200 {
		t.Fatalf("second append: %d %s", status, body)
	}

	status, body := resolveBody(t, ts1, task1,
		`{"action":"journal.append","type":"entry","data":{"w":"late"}}`)
	if status != http.StatusConflict || !strings.Contains(body, `"fenced"`) {
		t.Fatalf("fenced append = %d %s, want 409 fenced", status, body)
	}
	status, body = resolveBody(t, ts1, task1, `{"action":"complete","output":{"x":1}}`)
	if status != http.StatusConflict {
		t.Fatalf("fenced complete = %d %s, want 409", status, body)
	}
	if n := countHistory(t, rig.nc, runID, protocol.EventStepCompleted); n != 0 {
		t.Fatalf("fenced worker published %d step.completed events", n)
	}
	status, body = resolveBody(t, ts2, task2, `{"action":"complete","output":{"x":2}}`)
	if status != 200 {
		t.Fatalf("successor complete = %d %s", status, body)
	}
	if n := countHistory(t, rig.nc, runID, protocol.EventStepCompleted); n != 1 {
		t.Fatalf("step.completed events = %d, want 1", n)
	}
}

func TestBridgeJournalStaleTaskIsAckedAndGone(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	const runID, stepID = "run-jstale", "step-jstale"
	pub := natsutil.NewTracingPublisher(rig.nc, rig.js)
	later := journal.Task{RunID: runID, StepID: stepID, Iteration: 2, Attempt: 1,
		DispatchNonce: "n2", Delivered: 1}
	if _, err := journal.Open(context.Background(), rig.js, pub, later); err != nil {
		t.Fatalf("open later: %v", err)
	}
	rig.publish(t, protocol.TaskPayload{
		RunID: runID, StepID: stepID, Iteration: 1, DispatchNonce: "n1",
	})
	taskID := pollOne(t, ts)
	status, body := resolveBody(t, ts, taskID,
		`{"action":"journal.append","type":"entry","data":{"a":1}}`)
	if status != http.StatusGone || !strings.Contains(body, `"stale"`) {
		t.Fatalf("stale append = %d %s, want 410 stale", status, body)
	}
	if _, ok := b.ackMap.Load(taskID); ok {
		t.Fatal("stale task still in the ack map")
	}
	recs, err := journal.Read(context.Background(), rig.js, runID, stepID)
	if err != nil || len(recs) != 1 {
		t.Fatalf("stale task wrote to the journal: %d records, err %v", len(recs), err)
	}
}

func TestBridgeJournalCommittedAttemptRepublishes(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	const runID, stepID = "run-jcommit", "step-jcommit"
	pub := natsutil.NewTracingPublisher(rig.nc, rig.js)
	prior := journal.Task{RunID: runID, StepID: stepID, Iteration: 0, Attempt: 1,
		DispatchNonce: "n1", Delivered: 1}
	j, err := journal.Open(context.Background(), rig.js, pub, prior)
	if err != nil {
		t.Fatalf("open prior: %v", err)
	}
	if _, err := j.Commit(context.Background(), journal.Commit{
		Action: journal.ActionComplete, Output: json.RawMessage(`{"done":1}`),
	}); err != nil {
		t.Fatalf("commit prior: %v", err)
	}
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"})
	taskID := pollOne(t, ts)
	status, body := resolveBody(t, ts, taskID, `{"action":"journal.read"}`)
	if status != http.StatusGone || !strings.Contains(body, `"committed"`) {
		t.Fatalf("committed open = %d %s, want 410 committed", status, body)
	}
	evt := consumeHistoryEvent(t, rig.nc, runID, protocol.EventStepCompleted, 5*time.Second)
	if string(evt.Payload) != `{"done":1}` || evt.JournalSeq == 0 {
		t.Fatalf("republished event = %+v", evt)
	}
	if _, ok := b.ackMap.Load(taskID); ok {
		t.Fatal("committed task still in the ack map")
	}
}

func TestBridgeJournalAppendRejectsBadRequests(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	rig.publish(t, protocol.TaskPayload{RunID: "run-jbad", StepID: "step-jbad", DispatchNonce: "n1"})
	taskID := pollOne(t, ts)
	for _, body := range []string{
		`{"action":"journal.append","type":"claim","data":{"a":1}}`,
		`{"action":"journal.append","type":"commit","data":{"a":1}}`,
		`{"action":"journal.append","type":"entry"}`,
	} {
		if status, out := resolveBody(t, ts, taskID, body); status != http.StatusBadRequest {
			t.Fatalf("%s = %d %s, want 400", body, status, out)
		}
	}
}

func TestBridgeJournalPauseResumesThroughLoadCheckpoint(t *testing.T) {
	rig := newJournalRig(t)
	_, ts := rig.front(t)
	const runID, stepID = "run-jpause", "step-jpause"
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"})
	taskID := pollOne(t, ts)
	if status, body := resolveBody(t, ts, taskID,
		`{"action":"journal.append","type":"entry","data":{"a":1}}`); status != 200 {
		t.Fatalf("append: %d %s", status, body)
	}
	if status, body := resolveBody(t, ts, taskID,
		`{"action":"pause","name":"nap","duration_ms":50}`); status != 200 {
		t.Fatalf("pause: %d %s", status, body)
	}
	taskID = pollOne(t, ts)
	if status, body := resolveBody(t, ts, taskID, `{"action":"journal.read"}`); status != 200 {
		t.Fatalf("journal.read: %d %s", status, body)
	}
	status, body := resolveBody(t, ts, taskID, `{"action":"load_checkpoint"}`)
	var got loadCheckpointResponse
	if status != 200 || json.Unmarshal([]byte(body), &got) != nil {
		t.Fatalf("load_checkpoint: %d %s", status, body)
	}
	if got.Source != "journal" || string(got.State) != `{"pause_resume":"nap"}` {
		t.Fatalf("state = %s from %s, want the nap resume marker", got.State, got.Source)
	}
}

// A dispatch resend is a fresh message: delivery 1 again, same nonce.
// Its claim must fence the poller still holding the original.
func TestBridgeJournalResendFencesOriginalWithSameDelivery(t *testing.T) {
	rig := newJournalRig(t)
	_, ts1 := rig.front(t)
	_, ts2 := rig.front(t)
	const runID, stepID = "run-jresend", "step-jresend"
	payload := protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"}
	rig.publish(t, payload)
	task1 := pollOne(t, ts1)
	rig.publish(t, payload)
	task2 := pollOne(t, ts2)
	if status, body := resolveBody(t, ts1, task1,
		`{"action":"journal.append","type":"entry","data":{"w":1}}`); status != 200 {
		t.Fatalf("original append: %d %s", status, body)
	}
	if status, body := resolveBody(t, ts2, task2,
		`{"action":"journal.append","type":"entry","data":{"w":2}}`); status != 200 {
		t.Fatalf("resend append: %d %s", status, body)
	}
	status, body := resolveBody(t, ts1, task1,
		`{"action":"journal.append","type":"entry","data":{"w":"late"}}`)
	if status != http.StatusConflict || !strings.Contains(body, `"fenced"`) {
		t.Fatalf("original append after resend = %d %s, want 409 fenced", status, body)
	}
}

// A complete retried after its commit landed but its publish failed
// must republish the commit's event and settle the task, so the 410
// "committed" it answers is true.
func TestBridgeJournalRetriedCompleteRepublishesCommit(t *testing.T) {
	rig := newJournalRig(t)
	b, ts := rig.front(t)
	const runID, stepID = "run-jretry", "step-jretry"
	rig.publish(t, protocol.TaskPayload{RunID: runID, StepID: stepID, DispatchNonce: "n1"})
	taskID := pollOne(t, ts)
	if status, body := resolveBody(t, ts, taskID,
		`{"action":"journal.append","type":"entry","data":{"a":1}}`); status != 200 {
		t.Fatalf("append: %d %s", status, body)
	}
	msg, ok := b.ackMap.Load(taskID)
	if !ok {
		t.Fatal("task not claimed")
	}
	// The first complete: its commit lands, then its publish "fails".
	seq, err := b.commitJournal(context.Background(), taskID, "", msg, journal.Commit{
		Action: journal.ActionComplete, Output: json.RawMessage(`{"r":1}`),
	})
	if err != nil || seq == 0 {
		t.Fatalf("first commit: seq %d err %v", seq, err)
	}
	status, body := resolveBody(t, ts, taskID, `{"action":"complete","output":{"r":1}}`)
	if status != http.StatusGone || !strings.Contains(body, `"committed"`) {
		t.Fatalf("retried complete = %d %s, want 410 committed", status, body)
	}
	evt := consumeHistoryEvent(t, rig.nc, runID, protocol.EventStepCompleted, 5*time.Second)
	if string(evt.Payload) != `{"r":1}` || evt.JournalSeq != seq {
		t.Fatalf("republished event = %+v, want payload {\"r\":1} and journal_seq %d", evt, seq)
	}
	if _, ok := b.ackMap.Load(taskID); ok {
		t.Fatal("committed task still in the ack map")
	}
}
