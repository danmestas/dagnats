// trigger/http.go
//
// HTTPHandler implements http.Handler for the synchronous HTTP trigger
// kind per ADR-013. The handler:
//
//   1. Reads and bounds the body via httpenvelope.
//   2. Verifies HMAC when HTTPConfig.Secret is set.
//   3. Subscribes to httpenvelope.ResponseSubject(runID) on the plain
//      NATS connection BEFORE publishing the workflow.started event.
//      Without subscribe-before-publish, a workflow fast enough to
//      respond inside the publish→subscribe window would lose the
//      response (ADR-013 §1 step 3).
//   4. Publishes the TriggerEnvelope on the JetStream history stream
//      so the engine picks it up.
//   5. Awaits the response (or per-request timeout) and writes the
//      engine's status/headers/body to the http.ResponseWriter.
//   6. Always sets X-Dagnats-Run-Id so operators can correlate the
//      response back to `dagnats run inspect` (ADR-013 Q7).
//
// Failure-mode coverage (run.failed / cancelled / no-respond) lands
// in PR 3; PR 2 covers the happy path and per-request timeout.
package trigger

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/danmestas/dagnats/internal/httpenvelope"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// httpRunIDHeader is the response header name carrying the run ID
// (ADR-013 Q7: always present, not configurable).
const httpRunIDHeader = "X-Dagnats-Run-Id"

// httpHMACHeader is the request header HTTPHandler reads when
// HTTPConfig.Secret is set. Matches WebhookHandler's choice so a
// caller signing one kind of request signs the other identically.
const httpHMACHeader = "X-Signature-256"

// httpResponsePayload mirrors engine.respondWirePayload — the
// handler unmarshals what the engine publishes. Field shape drift
// here means a wire break, hence the shared struct definition.
type httpResponsePayload struct {
	Status      int               `json:"status"`
	Headers     map[string]string `json:"headers,omitempty"`
	ContentType string            `json:"content_type"`
	Body        []byte            `json:"body,omitempty"`
}

// HTTPHandler implements http.Handler for one HTTP trigger.
type HTTPHandler struct {
	nc  *nats.Conn
	js  jetstream.JetStream
	def TriggerDef
}

// NewHTTPHandler constructs an HTTPHandler bound to def's config.
// Panics on nil connection or missing HTTP config — both are
// programmer errors caught at trigger registration time.
func NewHTTPHandler(nc *nats.Conn, def TriggerDef) *HTTPHandler {
	if nc == nil {
		panic("NewHTTPHandler: nc must not be nil")
	}
	if def.HTTP == nil {
		panic("NewHTTPHandler: def.HTTP must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		panic(fmt.Sprintf("NewHTTPHandler: jetstream.New: %v", err))
	}
	return &HTTPHandler{nc: nc, js: js, def: def}
}

// ServeHTTP is the http.Handler entry point. Splits the work across
// helpers so each stays within the 70-line budget.
func (h *HTTPHandler) ServeHTTP(
	w http.ResponseWriter, r *http.Request,
) {
	if w == nil {
		panic("ServeHTTP: ResponseWriter must not be nil")
	}
	if r == nil {
		panic("ServeHTTP: Request must not be nil")
	}

	if r.Method != h.def.HTTP.Method {
		http.Error(w, "method not allowed",
			http.StatusMethodNotAllowed)
		return
	}

	body, ok := h.readAndValidate(w, r)
	if !ok {
		return
	}

	runID := fmt.Sprintf(
		"%s-%d", h.def.WorkflowID, time.Now().UTC().UnixNano(),
	)
	w.Header().Set(httpRunIDHeader, runID)

	timeout := time.Duration(h.def.HTTP.TimeoutMs) * time.Millisecond
	ctx, cancel := context.WithTimeout(r.Context(), timeout)
	defer cancel()

	sub, err := h.subscribeResponse(runID)
	if err != nil {
		http.Error(w,
			"failed to subscribe response",
			http.StatusInternalServerError,
		)
		return
	}
	defer func() { _ = sub.Unsubscribe() }()

	envelope, err := buildHTTPEnvelope(r, h.def, body)
	if err != nil {
		http.Error(w,
			"failed to build envelope",
			http.StatusInternalServerError,
		)
		return
	}
	if err := h.publishTrigger(ctx, envelope, runID, r); err != nil {
		http.Error(w,
			"failed to publish trigger",
			http.StatusInternalServerError,
		)
		return
	}

	h.awaitResponse(ctx, w, sub, runID)
}

// readAndValidate reads the body within the configured limit and,
// when configured, validates the HMAC signature. Writes the
// appropriate HTTP error on failure and returns ok=false. The body
// is returned on success so callers can avoid re-reading the
// request stream.
func (h *HTTPHandler) readAndValidate(
	w http.ResponseWriter, r *http.Request,
) ([]byte, bool) {
	if r == nil {
		panic("readAndValidate: r must not be nil")
	}
	if h.def.HTTP == nil {
		panic("readAndValidate: HTTP config must not be nil")
	}

	body, err := httpenvelope.BoundedBody(
		r.Body, h.def.HTTP.MaxBodyBytes,
	)
	if err != nil {
		if errors.Is(err, httpenvelope.ErrBodyTooLarge) {
			http.Error(w, "request body too large",
				http.StatusRequestEntityTooLarge)
			return nil, false
		}
		http.Error(w, "failed to read body",
			http.StatusBadRequest)
		return nil, false
	}

	if h.def.HTTP.Secret != "" {
		if !validHTTPSignature(r, h.def.HTTP.Secret, body) {
			http.Error(w, "invalid signature",
				http.StatusUnauthorized)
			return nil, false
		}
	}
	return body, true
}

// subscribeResponse opens a synchronous subscription on the
// per-run response subject. Subscribing here — before the trigger
// publish — closes the race a fast workflow would otherwise win.
func (h *HTTPHandler) subscribeResponse(
	runID string,
) (*nats.Subscription, error) {
	if runID == "" {
		panic("subscribeResponse: runID must not be empty")
	}
	if h.nc == nil {
		panic("subscribeResponse: nc must not be nil")
	}
	sub, err := h.nc.SubscribeSync(
		httpenvelope.ResponseSubject(runID),
	)
	if err != nil {
		return nil, fmt.Errorf("SubscribeSync: %w", err)
	}
	// Flush so the subscription is registered server-side before
	// the caller publishes the trigger envelope.
	if err := h.nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("flush: %w", err)
	}
	return sub, nil
}

// publishTrigger emits the workflow.started event carrying the
// TriggerEnvelope on the JetStream history stream. When the trigger
// has IdempotencyHeader set, the header value becomes the
// Nats-Msg-Id so JetStream's dedup window handles duplicates.
func (h *HTTPHandler) publishTrigger(
	ctx context.Context, envelope TriggerEnvelope,
	runID string, r *http.Request,
) error {
	if h.js == nil {
		panic("publishTrigger: js must not be nil")
	}
	if runID == "" {
		panic("publishTrigger: runID must not be empty")
	}
	envBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, envBytes,
	)
	evtBytes, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	msgID := evt.NATSMsgID()
	if h.def.HTTP.IdempotencyHeader != "" {
		if hv := r.Header.Get(h.def.HTTP.IdempotencyHeader); hv != "" {
			msgID = "http-idem-" + h.def.ID + "-" + hv
		}
	}
	_, err = h.js.Publish(
		ctx, evt.NATSSubject(), evtBytes,
		jetstream.WithMsgID(msgID),
	)
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}
	return nil
}

// awaitResponse selects on the response subscription and the
// per-request timeout. PR 3 will extend this with run.failed and
// run.cancelled event observation.
func (h *HTTPHandler) awaitResponse(
	ctx context.Context, w http.ResponseWriter,
	sub *nats.Subscription, runID string,
) {
	if sub == nil {
		panic("awaitResponse: sub must not be nil")
	}
	if runID == "" {
		panic("awaitResponse: runID must not be empty")
	}

	deadline, hasDeadline := ctx.Deadline()
	wait := time.Until(deadline)
	if !hasDeadline || wait <= 0 {
		wait = time.Duration(h.def.HTTP.TimeoutMs) * time.Millisecond
	}

	msg, err := sub.NextMsg(wait)
	if err != nil {
		http.Error(w,
			`{"error":"workflow_timeout","run_id":"`+runID+`"}`,
			http.StatusGatewayTimeout,
		)
		return
	}
	writeHTTPResponse(w, msg.Data)
}

// writeHTTPResponse decodes the engine's response payload and
// writes status/headers/body to the http.ResponseWriter. On decode
// failure it returns a 502 with a structured error.
func writeHTTPResponse(w http.ResponseWriter, data []byte) {
	if w == nil {
		panic("writeHTTPResponse: w must not be nil")
	}
	if data == nil {
		panic("writeHTTPResponse: data must not be nil")
	}
	var payload httpResponsePayload
	if err := json.Unmarshal(data, &payload); err != nil {
		http.Error(w,
			`{"error":"malformed_response"}`,
			http.StatusBadGateway,
		)
		return
	}
	for k, v := range payload.Headers {
		w.Header().Set(k, v)
	}
	if payload.ContentType != "" {
		w.Header().Set("Content-Type", payload.ContentType)
	}
	status := payload.Status
	if status == 0 {
		status = http.StatusOK
	}
	w.WriteHeader(status)
	if len(payload.Body) > 0 {
		_, _ = w.Write(payload.Body)
	}
}

// validHTTPSignature returns true when the request's HMAC header
// matches the secret. Mirrors WebhookHandler.validateSignature.
func validHTTPSignature(
	r *http.Request, secret string, body []byte,
) bool {
	if r == nil {
		panic("validHTTPSignature: r must not be nil")
	}
	if secret == "" {
		panic("validHTTPSignature: secret must not be empty")
	}
	sig := r.Header.Get(httpHMACHeader)
	if sig == "" {
		return false
	}
	if !strings.HasPrefix(sig, "sha256=") {
		return false
	}
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expected), []byte(sig[len("sha256="):]))
}

// buildHTTPEnvelope wraps an HTTP request as the trigger envelope
// the engine consumes. Body is the already-bounded slice from
// readAndValidate so the reader is not consumed twice.
func buildHTTPEnvelope(
	r *http.Request, def TriggerDef, body []byte,
) (TriggerEnvelope, error) {
	if r == nil {
		panic("buildHTTPEnvelope: r must not be nil")
	}
	if def.HTTP == nil {
		panic("buildHTTPEnvelope: HTTP config must not be nil")
	}
	env := httpenvelope.BuildEnvelopeFromBody(
		r, body, def.HTTP.MaxBodyBytes,
	)
	envData, err := json.Marshal(env)
	if err != nil {
		return TriggerEnvelope{}, fmt.Errorf(
			"marshal envelope: %w", err,
		)
	}
	return TriggerEnvelope{
		Trigger:    "http",
		Source:     def.ID,
		WorkflowID: def.WorkflowID,
		Timestamp:  time.Now().UTC(),
		Data:       envData,
	}, nil
}
