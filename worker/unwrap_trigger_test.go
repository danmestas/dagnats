// worker/unwrap_trigger_test.go
// Tests for the UnwrapTrigger() TypedOption: when set, the typed
// wrapper auto-detects trigger envelopes via structural inspection
// (top-level "trigger" string AND "data" field) and unmarshals the
// handler's typed parameter from the envelope's `data` field. When
// the option is not set, behavior is unchanged. Auto-detect is
// structural — not heuristic — so non-envelope inputs that happen to
// share one of the two keys still pass through.
//
// Methodology: unit tests use a mockTaskContext (shared with
// typed_test.go) for the pure cases; one end-to-end case wires the
// option through HandleTyped with an embedded NATS server and
// publishes a real envelope-shaped task message to verify the unwrap
// works through the full Worker dispatch path.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// httpReqData mirrors the shape of internal/httpenvelope.Envelope —
// the kind-specific payload carried under TriggerEnvelope.Data for
// HTTP triggers. Kept local to the test so this file does not depend
// on the internal package's exported types.
type httpReqData struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    []byte            `json:"body,omitempty"`
}

// envelopeBytes builds a JSON encoding of a TriggerEnvelope wrapping
// payload as `data`. Mirrors the wire shape produced by
// internal/trigger without depending on that package.
func envelopeBytes(t *testing.T, kind string, payload any) []byte {
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	env := struct {
		Trigger    string          `json:"trigger"`
		Source     string          `json:"source"`
		WorkflowID string          `json:"workflow_id"`
		Timestamp  string          `json:"timestamp"`
		Data       json.RawMessage `json:"data"`
	}{
		Trigger:    kind,
		Source:     "test-source",
		WorkflowID: "wf-test",
		Timestamp:  "2026-05-13T18:37:29Z",
		Data:       data,
	}
	out, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal envelope: %v", err)
	}
	return out
}

// TestUnwrapTrigger_EnvelopeInput verifies the happy path: with
// UnwrapTrigger set and a real envelope-shaped input, the typed
// handler receives the unwrapped `data` payload.
func TestUnwrapTrigger_EnvelopeInput(t *testing.T) {
	payload := httpReqData{
		Method: "POST", Path: "/api/echo",
		Body: []byte(`{"name":"alice"}`),
	}
	input := envelopeBytes(t, "http", payload)

	var got httpReqData
	handler := Typed(
		func(_ TaskContext, in httpReqData) (httpReqData, error) {
			got = in
			return in, nil
		},
		UnwrapTrigger(),
	)
	mock := &mockTaskContext{input: input}
	if err := handler(mock); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	// Positive: method + path lifted out of envelope.
	if got.Method != "POST" || got.Path != "/api/echo" {
		t.Fatalf("got %+v, want POST /api/echo", got)
	}
	// Negative: completed bytes must be present (handler did Complete).
	if mock.completed == nil {
		t.Fatal("Complete was not called")
	}
}

// TestUnwrapTrigger_RawInputPassesThrough verifies that when the
// input does NOT look like an envelope (no `trigger` key), the
// wrapper unmarshals directly into the typed parameter. This is the
// "local unit test" / "ad-hoc workflow run" path — workers with the
// option set must still accept plain inputs.
func TestUnwrapTrigger_RawInputPassesThrough(t *testing.T) {
	type plainIn struct {
		Name string `json:"name"`
	}
	var got plainIn
	handler := Typed(
		func(_ TaskContext, in plainIn) (plainIn, error) {
			got = in
			return in, nil
		},
		UnwrapTrigger(),
	)
	mock := &mockTaskContext{input: []byte(`{"name":"alice"}`)}
	if err := handler(mock); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	// Positive: typed parameter populated from raw input.
	if got.Name != "alice" {
		t.Fatalf("got Name=%q, want alice", got.Name)
	}
	// Negative: no panic, no error, no envelope-shaped extra fields.
	if mock.failErr != nil {
		t.Fatalf("unexpected fail: %v", mock.failErr)
	}
}

// TestUnwrapTrigger_EnvelopeWithoutOption verifies backward compat:
// existing handlers without UnwrapTrigger see the envelope as their
// input (i.e., they would need an embedded `Data` field to read the
// payload). This is the pre-change behavior — the new option must
// not alter it when not requested.
func TestUnwrapTrigger_EnvelopeWithoutOption(t *testing.T) {
	type envShape struct {
		Trigger string          `json:"trigger"`
		Data    json.RawMessage `json:"data"`
	}
	var got envShape
	handler := Typed(
		func(_ TaskContext, in envShape) (envShape, error) {
			got = in
			return in, nil
		},
		// No UnwrapTrigger().
	)
	payload := httpReqData{Method: "GET", Path: "/x"}
	input := envelopeBytes(t, "http", payload)
	mock := &mockTaskContext{input: input}
	if err := handler(mock); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	// Positive: trigger metadata visible at top level (envelope kept).
	if got.Trigger != "http" {
		t.Fatalf("got Trigger=%q, want http", got.Trigger)
	}
	// Negative: data is non-empty (envelope.Data survived intact).
	if len(got.Data) == 0 {
		t.Fatal("data field missing on envelope passthrough")
	}
}

// TestUnwrapTrigger_TriggerNotString verifies the structural check:
// if `trigger` is not a string (e.g., a nested object or number),
// the input is NOT treated as an envelope — passes through to the
// raw unmarshal. Negative-space assertion on the auto-detect.
func TestUnwrapTrigger_TriggerNotString(t *testing.T) {
	type wrapper struct {
		Trigger map[string]string `json:"trigger"`
		Data    map[string]string `json:"data"`
	}
	var got wrapper
	handler := Typed(
		func(_ TaskContext, in wrapper) (wrapper, error) {
			got = in
			return in, nil
		},
		UnwrapTrigger(),
	)
	// `trigger` is an object here — must NOT trigger unwrap.
	input := []byte(
		`{"trigger":{"kind":"webhook"},"data":{"k":"v"}}`,
	)
	mock := &mockTaskContext{input: input}
	if err := handler(mock); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	// Positive: typed param sees the wrapper shape (passthrough).
	if got.Trigger["kind"] != "webhook" {
		t.Fatalf("got %+v, want trigger.kind=webhook", got)
	}
	// Negative: data made it through unwrapped, i.e. not lifted.
	if got.Data["k"] != "v" {
		t.Fatalf("got %+v, want data.k=v", got)
	}
}

// TestUnwrapTrigger_DataMissing verifies the second arm of the
// structural check: an input with `trigger` but no `data` field is
// NOT an envelope (real envelopes always carry both). Passes through.
func TestUnwrapTrigger_DataMissing(t *testing.T) {
	type onlyTrig struct {
		Trigger string `json:"trigger"`
		Note    string `json:"note"`
	}
	var got onlyTrig
	handler := Typed(
		func(_ TaskContext, in onlyTrig) (onlyTrig, error) {
			got = in
			return in, nil
		},
		UnwrapTrigger(),
	)
	input := []byte(`{"trigger":"manual","note":"hello"}`)
	mock := &mockTaskContext{input: input}
	if err := handler(mock); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	// Positive: trigger field visible (passthrough).
	if got.Trigger != "manual" {
		t.Fatalf("got Trigger=%q, want manual", got.Trigger)
	}
	// Negative: note also visible (no unwrap happened).
	if got.Note != "hello" {
		t.Fatalf("got Note=%q, want hello", got.Note)
	}
}

// TestUnwrapTrigger_EmptyTrigger verifies that an empty-string
// `trigger` is not treated as an envelope. Defends against subtle
// shapes where a caller accidentally sends `{"trigger":""}` and
// expects passthrough behavior.
func TestUnwrapTrigger_EmptyTrigger(t *testing.T) {
	type shape struct {
		Trigger string `json:"trigger"`
		Data    string `json:"data"`
	}
	var got shape
	handler := Typed(
		func(_ TaskContext, in shape) (shape, error) {
			got = in
			return in, nil
		},
		UnwrapTrigger(),
	)
	input := []byte(`{"trigger":"","data":"hello"}`)
	mock := &mockTaskContext{input: input}
	if err := handler(mock); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	// Positive: empty trigger string seen as passthrough.
	if got.Trigger != "" {
		t.Fatalf("got Trigger=%q, want empty", got.Trigger)
	}
	if got.Data != "hello" {
		t.Fatalf("got Data=%q, want hello", got.Data)
	}
}

// TestUnwrapTrigger_NonObjectInput verifies that a JSON value that
// isn't an object (an array, string, number, null) can't be an
// envelope — passthrough.
func TestUnwrapTrigger_NonObjectInput(t *testing.T) {
	var got []int
	handler := Typed(
		func(_ TaskContext, in []int) ([]int, error) {
			got = in
			return in, nil
		},
		UnwrapTrigger(),
	)
	mock := &mockTaskContext{input: []byte(`[1,2,3]`)}
	if err := handler(mock); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if len(got) != 3 || got[0] != 1 {
		t.Fatalf("got %v, want [1 2 3]", got)
	}
}

// TestUnwrapTrigger_MalformedJSON verifies that bad JSON returns a
// NonRetryableError from the detect step (since redelivery will not
// fix bad bytes). The wrapped error name is "detect envelope" so the
// surface area is recognizable in logs.
func TestUnwrapTrigger_MalformedJSON(t *testing.T) {
	handler := Typed(
		func(
			_ TaskContext, _ map[string]any,
		) (map[string]any, error) {
			t.Fatal("handler should not run on malformed input")
			return nil, nil
		},
		UnwrapTrigger(),
	)
	mock := &mockTaskContext{input: []byte(`{not json`)}
	err := handler(mock)
	if err == nil {
		t.Fatal("expected error on malformed input")
	}
	var nre *NonRetryableError
	if !errors.As(err, &nre) {
		t.Fatalf("expected NonRetryableError, got %T", err)
	}
}

// TestUnwrapTrigger_NilInput verifies the edge case where ctx.Input
// is nil. The wrapper must not panic — the typed handler receives
// the zero value of its input type, same as the no-option path.
func TestUnwrapTrigger_NilInput(t *testing.T) {
	type plainIn struct {
		Name string `json:"name"`
	}
	called := false
	handler := Typed(
		func(_ TaskContext, in plainIn) (plainIn, error) {
			called = true
			if in.Name != "" {
				t.Fatalf("got Name=%q, want empty", in.Name)
			}
			return plainIn{Name: "ok"}, nil
		},
		UnwrapTrigger(),
	)
	mock := &mockTaskContext{input: nil}
	if err := handler(mock); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !called {
		t.Fatal("handler not called on nil input")
	}
}

// TestUnwrapTrigger_OrderInsensitive verifies that the structural
// auto-detect does not care whether `trigger` precedes or follows
// `data` in the JSON object. JSON objects are unordered; the scan
// must accept either order.
func TestUnwrapTrigger_OrderInsensitive(t *testing.T) {
	var got map[string]string
	handler := Typed(
		func(
			_ TaskContext, in map[string]string,
		) (map[string]string, error) {
			got = in
			return in, nil
		},
		UnwrapTrigger(),
	)
	// data first, then trigger — opposite of the canonical shape.
	input := []byte(
		`{"data":{"k":"v"},"trigger":"http","source":"s"}`,
	)
	mock := &mockTaskContext{input: input}
	if err := handler(mock); err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if got["k"] != "v" {
		t.Fatalf("got %+v, want k=v after unwrap", got)
	}
}

// TestUnwrapTrigger_E2E exercises the option through the real Worker
// dispatch path with an embedded NATS server. Publishes a task whose
// input is a real envelope shape; verifies the typed handler runs
// with the unwrapped data and produces a completion event.
func TestUnwrapTrigger_E2E(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	var (
		called atomic.Bool
		gotIn  atomic.Value // httpReqData
	)
	w := NewWorker(nc)
	HandleTyped(w, "echo",
		func(
			_ TaskContext, in httpReqData,
		) (httpReqData, error) {
			called.Store(true)
			gotIn.Store(in)
			return in, nil
		},
		UnwrapTrigger(),
	)
	w.Start()
	defer w.Stop()

	payload := httpReqData{
		Method: "POST", Path: "/e2e",
		Body: []byte(`{"hello":"world"}`),
	}
	envInput := envelopeBytes(t, "http", payload)
	taskMsg := protocol.TaskPayload{
		RunID:  "run-unwrap",
		StepID: "echo",
		Input:  envInput,
	}
	data, err := json.Marshal(taskMsg)
	if err != nil {
		t.Fatalf("marshal task: %v", err)
	}
	if _, err := js.Publish(
		"task.echo.run-unwrap", data,
	); err != nil {
		t.Fatalf("Publish task: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !called.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s")
		case <-time.After(25 * time.Millisecond):
		}
	}
	got, ok := gotIn.Load().(httpReqData)
	if !ok {
		t.Fatal("gotIn not populated")
	}
	// Positive: method came from envelope.data, not envelope itself.
	if got.Method != "POST" || got.Path != "/e2e" {
		t.Fatalf("got %+v, want POST /e2e", got)
	}
	// Negative: completion event lands on history (round-trip works).
	if err := awaitStepCompleted(t, js); err != nil {
		t.Fatalf("await completion: %v", err)
	}
}

// awaitStepCompleted drains history.run-unwrap until a step.completed
// event lands or 5s elapses. Mirrors the pattern in worker_test.go
// without depending on its package-private helpers.
func awaitStepCompleted(
	t *testing.T, js nats.JetStreamContext,
) error {
	t.Helper()
	sub, err := js.SubscribeSync(
		"history.run-unwrap", nats.DeliverAll(),
	)
	if err != nil {
		return err
	}
	defer func() { _ = sub.Unsubscribe() }()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			return err
		}
		if evt.Type == protocol.EventStepCompleted {
			return nil
		}
	}
	return context.DeadlineExceeded
}
