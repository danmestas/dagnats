// trigger/http_test.go
//
// Methodology: integration tests with embedded NATS. Each test
// stands up its own NATS server (no shared state). For every
// scenario we (a) register the HTTPHandler, (b) start a minimal
// engine simulator that subscribes to history.> and publishes the
// expected response, and (c) drive the handler via httptest. This
// keeps the surface area honest about the subscribe-before-publish
// race that ADR-013 calls out.
package trigger

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/httpenvelope"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// httpRespondWire mirrors the engine's respondWirePayload — the
// handler reads exactly this shape off the response subject.
type httpRespondWire struct {
	Status      int               `json:"status"`
	Headers     map[string]string `json:"headers,omitempty"`
	ContentType string            `json:"content_type"`
	Body        []byte            `json:"body,omitempty"`
}

func TestHTTPHandlerHappyPath(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-happy",
		WorkflowID: "wf-happy",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:         "/api/happy",
			Method:       http.MethodPost,
			TimeoutMs:    5_000,
			MaxBodyBytes: 1024,
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	handler := NewHTTPHandler(nc, def)
	stopEngine := startFakeRespondEngine(t, nc, httpRespondWire{
		Status:      201,
		ContentType: "application/json",
		Body:        []byte(`{"ok":true}`),
	})
	defer stopEngine()

	req := httptest.NewRequest(
		http.MethodPost, "/api/happy",
		bytes.NewReader([]byte(`{"hello":"world"}`)),
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != 201 {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	if got := rec.Body.String(); got != `{"ok":true}` {
		t.Fatalf("body = %q, want engine response verbatim", got)
	}
	if rec.Header().Get("X-Dagnats-Run-Id") == "" {
		t.Fatal("X-Dagnats-Run-Id header must be set (ADR-013 Q7)")
	}
}

func TestHTTPHandlerRejectsBodyTooLarge(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-too-big",
		WorkflowID: "wf-too-big",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:         "/api/big",
			Method:       http.MethodPost,
			TimeoutMs:    1_000,
			MaxBodyBytes: 16,
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	handler := NewHTTPHandler(nc, def)

	body := bytes.Repeat([]byte("A"), 1024)
	req := httptest.NewRequest(
		http.MethodPost, "/api/big", bytes.NewReader(body),
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", rec.Code)
	}
}

func TestHTTPHandlerRejectsBadMethod(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-method",
		WorkflowID: "wf-method",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:         "/api/posty",
			Method:       http.MethodPost,
			TimeoutMs:    1_000,
			MaxBodyBytes: 256,
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	handler := NewHTTPHandler(nc, def)

	req := httptest.NewRequest(http.MethodGet, "/api/posty", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}

func TestHTTPHandlerAcceptsValidHMACAndRejectsInvalid(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	const secret = "deadbeefdeadbeef" // 16 chars
	def := TriggerDef{
		ID:         "http-hmac",
		WorkflowID: "wf-hmac",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:         "/api/hmac",
			Method:       http.MethodPost,
			TimeoutMs:    5_000,
			MaxBodyBytes: 1024,
			Secret:       secret,
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	handler := NewHTTPHandler(nc, def)
	stop := startFakeRespondEngine(t, nc, httpRespondWire{
		Status:      200,
		ContentType: "application/json",
		Body:        []byte(`{"ok":true}`),
	})
	defer stop()

	body := []byte(`{"hmac":"yes"}`)

	// Invalid signature: 401
	badReq := httptest.NewRequest(
		http.MethodPost, "/api/hmac", bytes.NewReader(body),
	)
	badReq.Header.Set("X-Signature-256", "sha256=00")
	badRec := httptest.NewRecorder()
	handler.ServeHTTP(badRec, badReq)
	if badRec.Code != http.StatusUnauthorized {
		t.Fatalf("bad-sig status = %d, want 401", badRec.Code)
	}

	// Valid signature: passes through to engine, returns 200
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	sig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	goodReq := httptest.NewRequest(
		http.MethodPost, "/api/hmac", bytes.NewReader(body),
	)
	goodReq.Header.Set("X-Signature-256", sig)
	goodRec := httptest.NewRecorder()
	handler.ServeHTTP(goodRec, goodReq)
	if goodRec.Code != 200 {
		t.Fatalf("good-sig status = %d, want 200", goodRec.Code)
	}
}

func TestHTTPHandlerIdempotencyHeaderDeduplicates(t *testing.T) {
	// PR 2: verifies the JetStream dedup window collapses the second
	// publish when two HTTP requests share an Idempotency-Key header.
	// The second HTTP request currently times out (504) because the
	// handler still subscribes on its own per-run subject; PR 3 wires
	// idempotency replay (lookup prior runID, reuse response). This
	// test pins the wire-level dedup behavior for now.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	def := TriggerDef{
		ID:         "http-idem",
		WorkflowID: "wf-idem",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:              "/api/idem",
			Method:            http.MethodPost,
			TimeoutMs:         500,
			MaxBodyBytes:      1024,
			IdempotencyHeader: "Idempotency-Key",
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	sub, err := js.SubscribeSync(
		"history.>", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync history: %v", err)
	}

	handler := NewHTTPHandler(nc, def)

	// Two requests with the same Idempotency-Key — JetStream's dedup
	// window must collapse the second publish so only ONE
	// workflow.started event lands in the history stream regardless
	// of how the handler responds to the client.
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(
			http.MethodPost, "/api/idem",
			bytes.NewReader([]byte(`{"in":1}`)),
		)
		req.Header.Set("Idempotency-Key", "key-XYZ")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		// Status is irrelevant for this test (PR 3 will make it 200
		// on both); we are inspecting the wire-level dedup.
		_ = rec
	}

	// First message: present.
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("expected first workflow.started: %v", err)
	}
	// Second message: dedup window must drop it.
	if msg, err := sub.NextMsg(500 * time.Millisecond); err == nil {
		t.Fatalf(
			"unexpected second workflow.started "+
				"(dedup leak): subject=%s",
			msg.Subject,
		)
	}
}

func TestHTTPHandlerPublishesTriggerEnvelope(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	def := TriggerDef{
		ID:         "http-envelope",
		WorkflowID: "wf-envelope",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:         "/api/envelope",
			Method:       http.MethodPost,
			TimeoutMs:    5_000,
			MaxBodyBytes: 1024,
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Subscribe to history.> BEFORE issuing the request to catch the
	// workflow.started event the handler publishes.
	sub, err := js.SubscribeSync(
		"history.>", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync history: %v", err)
	}

	handler := NewHTTPHandler(nc, def)
	stop := startFakeRespondEngine(t, nc, httpRespondWire{
		Status:      200,
		ContentType: "application/json",
		Body:        []byte(`{"ok":true}`),
	})
	defer stop()

	body := []byte(`{"in":42}`)
	req := httptest.NewRequest(
		http.MethodPost, "/api/envelope", bytes.NewReader(body),
	)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("no workflow.started published: %v", err)
	}
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if evt.Type != protocol.EventWorkflowStarted {
		t.Fatalf("event = %q, want workflow.started", evt.Type)
	}

	var envelope TriggerEnvelope
	if err := json.Unmarshal(evt.Payload, &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if envelope.WorkflowID != "wf-envelope" {
		t.Fatalf("WorkflowID = %q, want wf-envelope",
			envelope.WorkflowID)
	}
	if envelope.Trigger != "http" {
		t.Fatalf("Trigger = %q, want http", envelope.Trigger)
	}
}

// startFakeRespondEngine subscribes to history.> and, for any
// workflow.started event whose payload is a TriggerEnvelope, publishes
// the configured response on the matching ResponseSubject(runID). The
// returned func stops the simulator.
func startFakeRespondEngine(
	t *testing.T, nc *nats.Conn, response httpRespondWire,
) func() {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := js.SubscribeSync(
		"history.>", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			msg, err := sub.NextMsg(50 * time.Millisecond)
			if err != nil {
				continue
			}
			respondOnce(t, nc, msg, response)
		}
	}()
	return func() {
		close(stop)
		<-done
		_ = sub.Unsubscribe()
	}
}

// respondOnce decodes a history event; if it is a workflow.started
// carrying a TriggerEnvelope, it publishes the configured response on
// ResponseSubject(runID). All other events are ignored.
func respondOnce(
	t *testing.T, nc *nats.Conn, msg *nats.Msg,
	response httpRespondWire,
) {
	t.Helper()
	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		return
	}
	if evt.Type != protocol.EventWorkflowStarted {
		return
	}
	if evt.RunID == "" {
		return
	}
	data, err := json.Marshal(response)
	if err != nil {
		t.Errorf("marshal response: %v", err)
		return
	}
	subj := httpenvelope.ResponseSubject(evt.RunID)
	if err := nc.Publish(subj, data); err != nil {
		t.Errorf("publish response: %v", err)
		return
	}
	// Flush so the synchronous handler subscription sees it.
	if err := nc.Flush(); err != nil {
		t.Errorf("flush: %v", err)
	}
	_ = fmt.Sprintf // silence import in case of build issues
}
