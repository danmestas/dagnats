package trigger

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// WebhookHandler implements http.Handler for webhook triggers.
// Validates HMAC-SHA256 signatures, enforces 1 MB body limit, and publishes
// workflow.started events to JetStream.
type WebhookHandler struct {
	nc  *nats.Conn
	js  nats.JetStreamContext
	def TriggerDef
}

// NewWebhookHandler creates a WebhookHandler for the given trigger def.
// Panics if nc is nil (programmer error).
func NewWebhookHandler(nc *nats.Conn, def TriggerDef) *WebhookHandler {
	if nc == nil {
		panic("NewWebhookHandler: connection must not be nil")
	}
	if def.Webhook == nil {
		panic("NewWebhookHandler: def.Webhook must not be nil")
	}

	js, err := nc.JetStream()
	if err != nil {
		panic(fmt.Sprintf("NewWebhookHandler: JetStream failed: %v", err))
	}

	return &WebhookHandler{
		nc:  nc,
		js:  js,
		def: def,
	}
}

// ServeHTTP handles incoming webhook requests.
func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 1 MB body limit
	const maxBodySize = 1 * 1024 * 1024
	r.Body = http.MaxBytesReader(w, r.Body, maxBodySize)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		if strings.Contains(err.Error(), "request body too large") {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	// Validate HMAC if secret configured
	if h.def.Webhook.Secret != "" {
		if !h.validateSignature(r, body) {
			http.Error(w, "invalid signature", http.StatusUnauthorized)
			return
		}
	}

	// Publish workflow event if enabled
	if h.def.Enabled {
		if err := h.publishWorkflowEvent(body); err != nil {
			http.Error(w, "failed to publish event", http.StatusInternalServerError)
			return
		}
	}

	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// validateSignature checks X-Signature-256 header against computed HMAC.
func (h *WebhookHandler) validateSignature(r *http.Request, body []byte) bool {
	signature := r.Header.Get("X-Signature-256")
	if signature == "" {
		return false
	}

	if !strings.HasPrefix(signature, "sha256=") {
		return false
	}

	expectedMAC := signature[7:]

	mac := hmac.New(sha256.New, []byte(h.def.Webhook.Secret))
	mac.Write(body)
	actualMAC := hex.EncodeToString(mac.Sum(nil))

	return hmac.Equal([]byte(actualMAC), []byte(expectedMAC))
}

// publishWorkflowEvent builds TriggerEnvelope and publishes workflow.started.
func (h *WebhookHandler) publishWorkflowEvent(body []byte) error {
	now := time.Now().UTC()

	var data json.RawMessage
	if len(body) > 0 {
		data = json.RawMessage(body)
	}

	envelope := TriggerEnvelope{
		Trigger:   "webhook",
		Source:    h.def.ID,
		Timestamp: now,
		Data:      data,
	}

	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	runID := fmt.Sprintf("%s-%d", h.def.WorkflowID, now.UnixNano())
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		runID,
		payloadBytes,
	)

	evtBytes, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	_, err = h.js.Publish(evt.NATSSubject(), evtBytes)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}
