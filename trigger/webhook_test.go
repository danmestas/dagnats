// trigger/webhook_test.go
// Methodology: Unit tests with httptest for HTTP handling, integration with
// embedded NATS for workflow event publishing. Tests verify HMAC validation,
// body limits, and error handling. No shared state between tests.
package trigger

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/protocol"
)

func TestWebhookHandlerPublishesWorkflowStarted(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	def := TriggerDef{
		ID:         "test-webhook",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Webhook: &WebhookConfig{
			Path:   "/webhooks/test",
			Secret: "",
		},
	}

	handler := NewWebhookHandler(nc, def)

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Create test request
	payload := []byte(`{"event": "test", "data": "value"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/test", bytes.NewReader(payload))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	// Should return 200
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Should publish workflow.started
	msg, err := sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("expected workflow.started event")
	}

	var evt protocol.Event
	err = json.Unmarshal(msg.Data, &evt)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	if evt.Type != protocol.EventWorkflowStarted {
		t.Errorf("expected workflow.started, got %s", evt.Type)
	}

	var envelope TriggerEnvelope
	err = json.Unmarshal(evt.Payload, &envelope)
	if err != nil {
		t.Fatalf("unmarshal envelope failed: %v", err)
	}

	if envelope.Trigger != "webhook" {
		t.Errorf("expected trigger=webhook, got %s", envelope.Trigger)
	}
	if envelope.Source != "test-webhook" {
		t.Errorf("expected source=test-webhook, got %s", envelope.Source)
	}

	// Verify original payload embedded (compare as JSON objects)
	var gotData, wantData map[string]interface{}
	err = json.Unmarshal(envelope.Data, &gotData)
	if err != nil {
		t.Fatalf("unmarshal envelope data failed: %v", err)
	}
	err = json.Unmarshal(payload, &wantData)
	if err != nil {
		t.Fatalf("unmarshal original payload failed: %v", err)
	}
	if gotData["event"] != wantData["event"] || gotData["data"] != wantData["data"] {
		t.Errorf("payload mismatch: got %v, want %v", gotData, wantData)
	}
}

func TestWebhookHandlerHMACValidation(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	secret := "my-secret-key"
	def := TriggerDef{
		ID:         "secure-webhook",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Webhook: &WebhookConfig{
			Path:   "/webhooks/secure",
			Secret: secret,
		},
	}

	handler := NewWebhookHandler(nc, def)

	payload := []byte(`{"secure": "data"}`)

	// Valid signature
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(payload)
	validSig := "sha256=" + hex.EncodeToString(mac.Sum(nil))

	req := httptest.NewRequest(http.MethodPost, "/webhooks/secure", bytes.NewReader(payload))
	req.Header.Set("X-Signature-256", validSig)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("valid signature: expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// Invalid signature
	req = httptest.NewRequest(http.MethodPost, "/webhooks/secure", bytes.NewReader(payload))
	req.Header.Set("X-Signature-256", "sha256=invalid")
	rec = httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("invalid signature: expected 401, got %d", rec.Code)
	}

	// Missing signature
	req = httptest.NewRequest(http.MethodPost, "/webhooks/secure", bytes.NewReader(payload))
	rec = httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusUnauthorized {
		t.Errorf("missing signature: expected 401, got %d", rec.Code)
	}
}

func TestWebhookHandlerBodyLimit(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	def := TriggerDef{
		ID:         "limited-webhook",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Webhook: &WebhookConfig{
			Path: "/webhooks/limited",
		},
	}

	handler := NewWebhookHandler(nc, def)

	// 2 MB payload (exceeds 1 MB limit)
	largePayload := bytes.Repeat([]byte("x"), 2*1024*1024)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/limited", bytes.NewReader(largePayload))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("expected 413, got %d", rec.Code)
	}
}

func TestWebhookHandlerDisabled(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	def := TriggerDef{
		ID:         "disabled-webhook",
		WorkflowID: "test-workflow",
		Enabled:    false,
		Webhook: &WebhookConfig{
			Path: "/webhooks/disabled",
		},
	}

	handler := NewWebhookHandler(nc, def)

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	payload := []byte(`{"test": "data"}`)
	req := httptest.NewRequest(http.MethodPost, "/webhooks/disabled", bytes.NewReader(payload))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	// Should still return 200 (accepted but not processed)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	// Should NOT publish workflow event
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("disabled webhook should not publish events")
	}
}

func TestWebhookHandlerEmptyBody(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	def := TriggerDef{
		ID:         "empty-webhook",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Webhook: &WebhookConfig{
			Path: "/webhooks/empty",
		},
	}

	handler := NewWebhookHandler(nc, def)

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/webhooks/empty", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}

	// Should publish event even with empty body
	msg, err := sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("expected event with empty body")
	}

	var evt protocol.Event
	err = json.Unmarshal(msg.Data, &evt)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}

	var envelope TriggerEnvelope
	err = json.Unmarshal(evt.Payload, &envelope)
	if err != nil {
		t.Fatalf("unmarshal envelope failed: %v", err)
	}

	if len(envelope.Data) > 0 {
		t.Errorf("expected empty data, got %s", envelope.Data)
	}
}

func TestWebhookHandlerMethodNotAllowed(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc)
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	def := TriggerDef{
		ID:         "method-webhook",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Webhook: &WebhookConfig{
			Path: "/webhooks/method",
		},
	}

	handler := NewWebhookHandler(nc, def)

	// GET request should fail
	req := httptest.NewRequest(http.MethodGet, "/webhooks/method", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}

	// PUT request should fail
	req = httptest.NewRequest(http.MethodPut, "/webhooks/method", nil)
	rec = httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}
