// internal/api/inputs_test.go
// The input endpoints (ADR-027): POST creates a submission once per
// request_id and announces it on the run's history; a repeat returns
// the original; GET reads the status, long-polls for a change, and
// repairs a status the journal contradicts; DELETE withdraws only
// while queued; a step that has finished refuses new inputs, and one
// that finishes between the create and the re-check settles the input
// unanswered. The NATS micro endpoints answer the same way.
//
// Methodology: a Service on an embedded NATS server with a run
// snapshot written directly (no engine), the REST handler behind
// httptest, and the micro service on the same connection. Each test
// checks the positive and the negative outcome. Every wait is bounded.
package api

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/inputs"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const inputsTestTimeout = 10 * time.Second

type inputsFixture struct {
	nc     *nats.Conn
	js     jetstream.JetStream
	svc    *Service
	server *httptest.Server
	store  *engine.SnapshotStore
}

func newInputsFixture(t *testing.T) inputsFixture {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	svc := NewService(nc)
	server := httptest.NewServer(NewRESTHandler(svc))
	t.Cleanup(server.Close)
	return inputsFixture{nc: nc, js: js, svc: svc, server: server, store: engine.NewSnapshotStore(js)}
}

// saveRun writes a one-step run whose agent step is in status.
func (fx inputsFixture) saveRun(t *testing.T, runID string, status dag.StepStatus) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	wfDef := dag.WorkflowDef{Name: "in-wf", Version: "1", Steps: []dag.StepDef{{
		ID: "agent", Task: "agent-task", Type: dag.StepTypeAgentLoop,
	}}}
	run := dag.NewWorkflowRun(wfDef, runID)
	run.Status = dag.RunStatusRunning
	run.Steps["agent"] = dag.StepState{Status: status, Attempts: 1, DispatchNonce: "n"}
	if err := fx.store.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}
}

func (fx inputsFixture) do(t *testing.T, method, path, body string) (int, map[string]any) {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	req, err := http.NewRequest(method, fx.server.URL+path, reader)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var decoded map[string]any
	if len(raw) > 0 && strings.HasPrefix(resp.Header.Get("Content-Type"), "application/json") {
		if err := json.Unmarshal(raw, &decoded); err != nil {
			t.Fatalf("decode %s: %v", raw, err)
		}
	}
	return resp.StatusCode, decoded
}

func (fx inputsFixture) historyHas(t *testing.T, runID string, want protocol.EventType, requestID string) bool {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	stream, err := fx.js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	for seq := uint64(1); seq < 50; seq++ {
		msg, err := stream.GetMsg(ctx, seq, jetstream.WithGetMsgSubject("history."+runID))
		if err != nil {
			return false
		}
		evt, err := protocol.UnmarshalEvent(msg.Data)
		if err == nil && evt.Type == want && evt.InputRequestID == requestID {
			return true
		}
		seq = msg.Sequence
	}
	return false
}

func TestRESTInputs_SubmitOnceAndRepeatReturnsOriginal(t *testing.T) {
	fx := newInputsFixture(t)
	fx.saveRun(t, "run-s", dag.StepStatusRunning)
	path := "/runs/run-s/steps/agent/inputs"
	code, body := fx.do(t, http.MethodPost, path,
		`{"request_id":"r1","mode":"steer","content":{"text":"go left"}}`)
	if code != http.StatusCreated || body["status"] != "queued" || body["request_id"] != "r1" {
		t.Fatalf("POST = %d %v, want 201 queued r1", code, body)
	}
	if !fx.historyHas(t, "run-s", protocol.EventStepInputQueued, "r1") {
		t.Error("submission was not announced on the run's history")
	}
	code, body = fx.do(t, http.MethodPost, path,
		`{"request_id":"r1","mode":"follow_up","content":"other"}`)
	if code != http.StatusOK || body["mode"] != "steer" {
		t.Fatalf("repeat POST = %d %v, want 200 with the original steer", code, body)
	}
	for _, bad := range []string{
		`{"mode":"steer","content":1}`, `{"request_id":"a b","mode":"steer","content":1}`,
		`{"request_id":"r2","mode":"poke","content":1}`, `not json`,
	} {
		if code, _ := fx.do(t, http.MethodPost, path, bad); code != http.StatusBadRequest {
			t.Errorf("POST %s = %d, want 400", bad, code)
		}
	}
	big := `{"request_id":"big","mode":"steer","content":"` + strings.Repeat("x", inputs.ContentMaxBytes) + `"}`
	code, _ = fx.do(t, http.MethodPost, path, big)
	if code != http.StatusBadRequest {
		t.Errorf("POST over the content cap = %d, want 400", code)
	}
	if code, _ := fx.do(t, http.MethodPost, "/runs/no-run/steps/agent/inputs",
		`{"request_id":"r1","mode":"steer","content":1}`); code != http.StatusNotFound {
		t.Errorf("POST to an unknown run = %d, want 404", code)
	}
	if code, _ := fx.do(t, http.MethodPost, "/runs/run-s/steps/nope/inputs",
		`{"request_id":"r1","mode":"steer","content":1}`); code != http.StatusNotFound {
		t.Errorf("POST to an unknown step = %d, want 404", code)
	}
}

func TestRESTInputs_FinishedStepRefusesAndSettles(t *testing.T) {
	fx := newInputsFixture(t)
	fx.saveRun(t, "run-f", dag.StepStatusCompleted)
	code, body := fx.do(t, http.MethodPost, "/runs/run-f/steps/agent/inputs",
		`{"request_id":"late","mode":"follow_up","content":1}`)
	if code != http.StatusConflict {
		t.Fatalf("POST to a finished step = %d %v, want 409", code, body)
	}
	// The race: the step finishes after the pre-check. Play it by
	// submitting through the store, then reading through the API.
	fx.saveRun(t, "run-g", dag.StepStatusRunning)
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	store, err := inputs.Open(ctx, fx.js)
	if err != nil {
		t.Fatalf("inputs.Open: %v", err)
	}
	if _, _, err := store.Submit(ctx, inputs.Submission{
		RunID: "run-g", StepID: "agent", RequestID: "raced", Mode: inputs.ModeSteer,
		Content: json.RawMessage(`1`),
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	run, err := fx.store.Load(ctx, "run-g")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	state := run.Steps["agent"]
	state.Status = dag.StepStatusFailed
	run.Steps["agent"] = state
	if err := fx.store.Save(ctx, run); err != nil {
		t.Fatalf("Save: %v", err)
	}
	code, body = fx.do(t, http.MethodGet, "/runs/run-g/steps/agent/inputs/raced", "")
	if code != http.StatusOK || body["status"] != "unanswered" {
		t.Fatalf("GET after the step failed = %d %v, want unanswered", code, body)
	}
}

func TestRESTInputs_GetLongPollsAndRepairsFromJournal(t *testing.T) {
	fx := newInputsFixture(t)
	fx.saveRun(t, "run-p", dag.StepStatusRunning)
	base := "/runs/run-p/steps/agent/inputs"
	if code, _ := fx.do(t, http.MethodPost, base,
		`{"request_id":"p1","mode":"steer","content":1}`); code != http.StatusCreated {
		t.Fatalf("POST = %d, want 201", code)
	}
	start := time.Now()
	code, body := fx.do(t, http.MethodGet, base+"/p1?wait=300ms", "")
	if code != http.StatusOK || body["status"] != "queued" || time.Since(start) < 250*time.Millisecond {
		t.Fatalf("GET wait = %d %v after %s, want queued at the deadline", code, body, time.Since(start))
	}
	// The worker placed p1 and died before projecting: the journal
	// holds the record, the bucket still says queued.
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	j, err := journal.Open(ctx, fx.js, natsutil.NewTracingPublisher(fx.nc, fx.js), journal.Task{
		RunID: "run-p", StepID: "agent", Attempt: 1, DispatchNonce: "n", Delivered: 1,
	})
	if err != nil {
		t.Fatalf("journal.Open: %v", err)
	}
	if _, err := j.PlaceInput(ctx, journal.InputPlaced{RequestID: "p1", Mode: "steer"}); err != nil {
		t.Fatalf("PlaceInput: %v", err)
	}
	code, body = fx.do(t, http.MethodGet, base+"/p1", "")
	if code != http.StatusOK || body["status"] != "placed" {
		t.Fatalf("GET = %d %v, want placed, repaired from the journal", code, body)
	}
	if _, err := j.Commit(ctx, journal.Commit{Action: journal.ActionContinue}); err != nil {
		t.Fatalf("Commit: %v", err)
	}
	code, body = fx.do(t, http.MethodGet, base+"/p1?wait=2s&after=placed", "")
	if code != http.StatusOK || body["status"] != "done" {
		t.Fatalf("GET after the commit = %d %v, want done", code, body)
	}
	if code, _ := fx.do(t, http.MethodGet, base+"/nope", ""); code != http.StatusNotFound {
		t.Errorf("GET of an unknown request = %d, want 404", code)
	}
}

func TestRESTInputs_WithdrawOnlyWhileQueued(t *testing.T) {
	fx := newInputsFixture(t)
	fx.saveRun(t, "run-w", dag.StepStatusRunning)
	base := "/runs/run-w/steps/agent/inputs"
	if code, _ := fx.do(t, http.MethodPost, base,
		`{"request_id":"w1","mode":"steer","content":1}`); code != http.StatusCreated {
		t.Fatalf("POST = %d, want 201", code)
	}
	if code, _ := fx.do(t, http.MethodDelete, base+"/w1", ""); code != http.StatusNoContent {
		t.Fatalf("DELETE = %d, want 204", code)
	}
	if code, _ := fx.do(t, http.MethodGet, base+"/w1", ""); code != http.StatusNotFound {
		t.Errorf("GET after withdraw = %d, want 404", code)
	}
	if code, _ := fx.do(t, http.MethodDelete, base+"/w1", ""); code != http.StatusNotFound {
		t.Errorf("second DELETE = %d, want 404", code)
	}
	ctx, cancel := context.WithTimeout(context.Background(), inputsTestTimeout)
	defer cancel()
	store, err := inputs.Open(ctx, fx.js)
	if err != nil {
		t.Fatalf("inputs.Open: %v", err)
	}
	if _, _, err := store.Submit(ctx, inputs.Submission{
		RunID: "run-w", StepID: "agent", RequestID: "w2", Mode: inputs.ModeSteer,
		Content: json.RawMessage(`1`),
	}); err != nil {
		t.Fatalf("Submit: %v", err)
	}
	entry, err := store.Get(ctx, "run-w", "agent", "w2")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if _, err := store.Transition(ctx, entry, inputs.StatusPlaced, 1); err != nil {
		t.Fatalf("Transition: %v", err)
	}
	if code, _ := fx.do(t, http.MethodDelete, base+"/w2", ""); code != http.StatusConflict {
		t.Errorf("DELETE of a placed input = %d, want 409", code)
	}
}

func TestNATSAPIInputs_SubmitGetWithdraw(t *testing.T) {
	fx := newInputsFixture(t)
	fx.saveRun(t, "run-n", dag.StepStatusRunning)
	natsAPI := NewNATSAPI(fx.svc, fx.nc, "1.0.0")
	natsAPI.Start()
	defer natsAPI.Stop()
	ask := func(subject, body string) map[string]any {
		t.Helper()
		reply, err := fx.nc.Request(subject, []byte(body), inputsTestTimeout)
		if err != nil {
			t.Fatalf("Request %s: %v", subject, err)
		}
		var out map[string]any
		if err := json.Unmarshal(reply.Data, &out); err != nil {
			t.Fatalf("decode %s: %v", reply.Data, err)
		}
		return out
	}
	submit := `{"run_id":"run-n","step_id":"agent","request_id":"n1","mode":"steer","content":"hi"}`
	first := ask("api.inputs.submit", submit)
	if first["status"] != "queued" || first["created"] != true {
		t.Fatalf("submit = %v, want queued and created", first)
	}
	again := ask("api.inputs.submit", submit)
	if again["created"] != false {
		t.Errorf("repeat submit = %v, want created false", again)
	}
	got := ask("api.inputs.get", `{"run_id":"run-n","step_id":"agent","request_id":"n1","wait_ms":100}`)
	if got["status"] != "queued" {
		t.Errorf("get = %v, want queued", got)
	}
	gone := ask("api.inputs.withdraw", `{"run_id":"run-n","step_id":"agent","request_id":"n1"}`)
	if gone["status"] != "withdrawn" {
		t.Errorf("withdraw = %v, want withdrawn", gone)
	}
	missing := ask("api.inputs.get", `{"run_id":"run-n","step_id":"agent","request_id":"n1"}`)
	if missing["error"] == nil || missing["kind"] != "not_found" {
		t.Errorf("get after withdraw = %v, want a not_found error", missing)
	}
	bad := ask("api.inputs.submit", `{"run_id":"run-n","step_id":"agent","request_id":"x y","mode":"steer","content":1}`)
	if bad["error"] == nil || bad["kind"] != "invalid" {
		t.Errorf("invalid submit = %v, want an invalid error", bad)
	}
}
