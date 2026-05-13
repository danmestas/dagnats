// trigger/http.go
//
// HTTPHandler implements http.Handler for the synchronous HTTP trigger
// kind per ADR-013. The handler:
//
//  1. Reads and bounds the body via httpenvelope.
//  2. Verifies HMAC when HTTPConfig.Secret is set.
//  3. Subscribes to httpenvelope.ResponseSubject(runID) on the plain
//     NATS connection BEFORE publishing the workflow.started event.
//     Without subscribe-before-publish, a workflow fast enough to
//     respond inside the publish→subscribe window would lose the
//     response (ADR-013 §1 step 3).
//  4. Publishes the TriggerEnvelope on the JetStream history stream
//     so the engine picks it up.
//  5. Awaits the response (or per-request timeout) and writes the
//     engine's status/headers/body to the http.ResponseWriter.
//  6. Always sets X-Dagnats-Run-Id so operators can correlate the
//     response back to `dagnats run inspect` (ADR-013 Q7).
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
	"github.com/danmestas/dagnats/internal/runid"
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

// idempotencyKVBucket is the JetStream KV bucket that stores
// (triggerID, header-value) → originalRunID for HTTP idempotency
// replay (ADR-013 Q6 / PR 3). Created by natsutil.SetupKVBuckets so
// the handler can assume it exists in any topology that uses HTTP
// triggers. Bucket-level TTL governs entry lifetime — see conn.go.
const idempotencyKVBucket = "http_idempotency"

// HTTPHandler implements http.Handler for one HTTP trigger.
type HTTPHandler struct {
	nc   *nats.Conn
	js   jetstream.JetStream
	idkv jetstream.KeyValue // nil unless IdempotencyHeader is set
	def  TriggerDef
}

// NewHTTPHandler constructs an HTTPHandler bound to def's config.
// Panics on nil connection or missing HTTP config — both are
// programmer errors caught at trigger registration time. Resolves
// the idempotency KV lazily so a handler whose trigger doesn't use
// IdempotencyHeader does not require the bucket to exist.
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
	h := &HTTPHandler{nc: nc, js: js, def: def}
	if def.HTTP.IdempotencyHeader != "" {
		ctx, cancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer cancel()
		kv, err := js.KeyValue(ctx, idempotencyKVBucket)
		if err == nil {
			h.idkv = kv
		}
		// Bucket may not exist in tests/topologies that don't use
		// HTTP triggers; the handler degrades to per-run subjects
		// (the PR 2 behavior). This is benign because the bucket
		// is created by natsutil.SetupAll in normal deployments.
	}
	return h
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
	// runid.New is crypto-random; the previous workflowID + UnixNano
	// shape collided under concurrent load and the per-run JetStream
	// dedup window dropped one of the colliding workflow.started
	// events, delivering the surviving run's response to both
	// waiting HTTP handlers (a cross-client data leak).
	newRunID := runid.New()
	timeout := time.Duration(h.def.HTTP.TimeoutMs) * time.Millisecond
	publishCtx, publishCancel := context.WithTimeout(
		r.Context(), timeout,
	)
	defer publishCancel()

	runID, replay := h.resolveIdempotentRunID(
		publishCtx, r, newRunID,
	)
	w.Header().Set(httpRunIDHeader, runID)

	if replay && h.replayFromStoredResult(w, r, runID, timeout) {
		return
	}
	h.dispatchAndAwait(w, r, body, publishCtx, runID, replay)
}

// replayFromStoredResult is the fast path when the (triggerID,
// header-value) → runID claim already exists and the original
// handler stored its response in KV. Returns true when the stored
// payload was found and the HTTP response was written; false to
// fall through to the subscribe-and-await path (which both the
// original and any concurrent replay handlers also use when no
// stored payload exists yet).
func (h *HTTPHandler) replayFromStoredResult(
	w http.ResponseWriter, r *http.Request,
	runID string, timeout time.Duration,
) bool {
	if w == nil {
		panic("replayFromStoredResult: w must not be nil")
	}
	if runID == "" {
		panic("replayFromStoredResult: runID must not be empty")
	}
	data, ok := h.fetchStoredResult(r.Context(), runID, timeout)
	if !ok {
		return false
	}
	writeHTTPResponse(w, data)
	return true
}

// dispatchAndAwait runs the subscribe-publish-await sequence for a
// non-replay request, OR for a replay request whose stored result
// was not yet present in KV. Subscribing to history.<runID> BEFORE
// publishing the trigger envelope closes the race where a fast-
// failing engine emits workflow.failed before the observer wires up.
func (h *HTTPHandler) dispatchAndAwait(
	w http.ResponseWriter, r *http.Request, body []byte,
	publishCtx context.Context, runID string, replay bool,
) {
	if runID == "" {
		panic("dispatchAndAwait: runID must not be empty")
	}
	if w == nil {
		panic("dispatchAndAwait: w must not be nil")
	}
	sub, err := h.subscribeResponse(runID)
	if err != nil {
		http.Error(w, "failed to subscribe response",
			http.StatusInternalServerError)
		return
	}
	defer func() { _ = sub.Unsubscribe() }()

	failCh := make(chan failureSignal, 1)
	stopFailWatch, ferr := startFailureObserver(h.nc, runID, failCh)
	if ferr != nil {
		http.Error(w,
			`{"error":"failure_observer_failed","run_id":"`+
				runID+`"}`,
			http.StatusInternalServerError,
		)
		return
	}
	defer stopFailWatch()

	if !replay {
		if !h.buildAndPublish(publishCtx, w, r, body, runID) {
			return
		}
	}
	h.awaitResponse(r.Context(), w, sub, runID, failCh)
}

// buildAndPublish marshals the trigger envelope and publishes the
// workflow.started event. Returns false (and writes the HTTP error)
// on either failure; true on success.
func (h *HTTPHandler) buildAndPublish(
	ctx context.Context, w http.ResponseWriter, r *http.Request,
	body []byte, runID string,
) bool {
	if runID == "" {
		panic("buildAndPublish: runID must not be empty")
	}
	if w == nil {
		panic("buildAndPublish: w must not be nil")
	}
	envelope, err := buildHTTPEnvelope(r, h.def, body)
	if err != nil {
		http.Error(w, "failed to build envelope",
			http.StatusInternalServerError)
		return false
	}
	if err := h.publishTrigger(
		ctx, envelope, runID, r,
	); err != nil {
		http.Error(w, "failed to publish trigger",
			http.StatusInternalServerError)
		return false
	}
	return true
}

// resolveIdempotentRunID looks up the idempotency KV for a prior runID
// bound to (triggerID, header-value). On hit, returns (existingRunID,
// true) so the caller subscribes to the original run's subject and
// skips publishing a new workflow.started event. On miss, atomically
// claims newRunID via KV.Create and returns (newRunID, false). Any KV
// error (bucket missing, header empty, ctx cancelled) falls through
// to (newRunID, false) — the worst case is the pre-replay PR 2
// behavior (each request its own per-run subject).
func (h *HTTPHandler) resolveIdempotentRunID(
	ctx context.Context, r *http.Request, newRunID string,
) (string, bool) {
	if r == nil {
		panic("resolveIdempotentRunID: r must not be nil")
	}
	if newRunID == "" {
		panic("resolveIdempotentRunID: newRunID must not be empty")
	}
	if h.idkv == nil {
		return newRunID, false
	}
	hdrName := h.def.HTTP.IdempotencyHeader
	if hdrName == "" {
		return newRunID, false
	}
	hdrValue := r.Header.Get(hdrName)
	if hdrValue == "" {
		return newRunID, false
	}
	key := idempotencyKey(h.def.ID, hdrValue)

	entry, err := h.idkv.Get(ctx, key)
	if err == nil && entry != nil && len(entry.Value()) > 0 {
		return string(entry.Value()), true
	}
	// Miss: race-safe claim via Create (first writer wins).
	if _, err := h.idkv.Create(
		ctx, key, []byte(newRunID),
	); err != nil {
		// Someone else won the race — re-read and use theirs.
		entry, err := h.idkv.Get(ctx, key)
		if err == nil && entry != nil && len(entry.Value()) > 0 {
			return string(entry.Value()), true
		}
		// Genuine KV error: degrade to PR 2 behavior.
		return newRunID, false
	}
	return newRunID, false
}

// idempotencyKey composes the KV key. Trigger ID prefix scopes the
// header value to the route — two distinct triggers can use the same
// Idempotency-Key without colliding. The "claim" prefix distinguishes
// claim entries from result entries (storeResult / fetchStoredResult).
func idempotencyKey(triggerID, headerValue string) string {
	if triggerID == "" {
		panic("idempotencyKey: triggerID must not be empty")
	}
	if headerValue == "" {
		panic("idempotencyKey: headerValue must not be empty")
	}
	return "claim." + triggerID + "." + headerValue
}

// resultKey is the KV key for the stored response payload of a run,
// readable by replay handlers. The runID is unique per run so no
// prefix collision is possible.
func resultKey(runID string) string {
	if runID == "" {
		panic("resultKey: runID must not be empty")
	}
	return "result." + runID
}

// storeResult writes the engine response payload to the idempotency
// KV under result.<runID> so subsequent replay requests (same
// IdempotencyHeader value) can serve it without re-running the
// workflow. Errors are logged at the caller's discretion — best-effort
// only; the live response is already on the wire.
func (h *HTTPHandler) storeResult(
	ctx context.Context, runID string, data []byte,
) {
	if runID == "" {
		panic("storeResult: runID must not be empty")
	}
	if data == nil {
		panic("storeResult: data must not be nil")
	}
	if h.idkv == nil {
		return
	}
	// Use a separate short-deadline ctx so the request's own timer
	// cannot abort the KV write after the response is on the wire.
	writeCtx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()
	_, _ = h.idkv.Put(writeCtx, resultKey(runID), data)
	_ = ctx
}

// fetchStoredResult polls the idempotency KV for the result of a
// prior run, returning the stored payload and ok=true on hit.
// Exponential backoff (25ms → 500ms cap) keeps concurrent duplicates
// from thundering the bucket: a 30s timeout costs ~60 gets per miss,
// not ~1200. Bounded loop per TigerStyle.
func (h *HTTPHandler) fetchStoredResult(
	ctx context.Context, runID string, timeout time.Duration,
) ([]byte, bool) {
	if runID == "" {
		panic("fetchStoredResult: runID must not be empty")
	}
	if timeout <= 0 {
		panic("fetchStoredResult: timeout must be positive")
	}
	if h.idkv == nil {
		return nil, false
	}
	const pollIntervalMin = 25 * time.Millisecond
	const pollIntervalMax = 500 * time.Millisecond
	interval := pollIntervalMin
	deadline := time.Now().Add(timeout)
	for i := 0; i < 10000; i++ {
		if ctx.Err() != nil {
			return nil, false
		}
		readCtx, cancel := context.WithTimeout(
			context.Background(), 500*time.Millisecond,
		)
		entry, err := h.idkv.Get(readCtx, resultKey(runID))
		cancel()
		if err == nil && entry != nil && len(entry.Value()) > 0 {
			return entry.Value(), true
		}
		if time.Now().After(deadline) {
			return nil, false
		}
		time.Sleep(interval)
		interval *= 2
		if interval > pollIntervalMax {
			interval = pollIntervalMax
		}
	}
	return nil, false
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

// awaitResponse selects on the response subscription, the
// pre-armed failure observer (history.<runID>), the request context,
// and the per-request timeout. Maps each terminal signal to the
// documented HTTP outcome per ADR-013 §"Failure handling". The
// caller is responsible for arming the failure observer BEFORE
// publishing the trigger envelope so a fast-failing engine cannot
// publish workflow.failed before the observer is wired.
func (h *HTTPHandler) awaitResponse(
	ctx context.Context, w http.ResponseWriter,
	sub *nats.Subscription, runID string,
	failCh <-chan failureSignal,
) {
	if sub == nil {
		panic("awaitResponse: sub must not be nil")
	}
	if runID == "" {
		panic("awaitResponse: runID must not be empty")
	}
	if failCh == nil {
		panic("awaitResponse: failCh must not be nil")
	}

	respCh := make(chan *nats.Msg, 1)
	startResponseReader(sub, respCh)

	timer := time.NewTimer(awaitTimeout(ctx, h.def.HTTP.TimeoutMs))
	defer timer.Stop()

	select {
	case msg := <-respCh:
		// Store the result in KV first so any concurrent / future
		// replay request for this runID sees the same response. KV
		// write errors are non-fatal — the live client still gets
		// the response on the wire; only replay degrades.
		h.storeResult(ctx, runID, msg.Data)
		writeHTTPResponse(w, msg.Data)
	case sig := <-failCh:
		writeFailureResponse(w, runID, sig)
	case <-ctx.Done():
		// Client closed the request before any signal arrived. 499
		// is nginx's "client closed request" code — observability
		// tooling distinguishes it from 5xx and ADR-013 reserves it
		// for this case.
		http.Error(w,
			`{"error":"client_closed","run_id":"`+runID+`"}`,
			499,
		)
	case <-timer.C:
		http.Error(w,
			`{"error":"workflow_timeout","run_id":"`+runID+`"}`,
			http.StatusGatewayTimeout,
		)
	}
}

// awaitTimeout returns the per-request timeout as a duration. The
// request's own context handles client-close as its own select arm
// in awaitResponse — the timeout here is purely the configured
// HTTPConfig.TimeoutMs cap.
func awaitTimeout(ctx context.Context, timeoutMs int) time.Duration {
	if ctx == nil {
		panic("awaitTimeout: ctx must not be nil")
	}
	if timeoutMs <= 0 {
		panic("awaitTimeout: timeoutMs must be positive")
	}
	return time.Duration(timeoutMs) * time.Millisecond
}

// failureSignal carries the kind of failure event observed on
// history.<runID>. The select case maps it to a specific HTTP status.
type failureSignal struct {
	kind string // "failed" or "cancelled"
}

// startResponseReader spawns a goroutine that does a single blocking
// NextMsg on the response subscription and forwards the message (or
// silently exits on error — the awaitResponse select will fall through
// to ctx.Done / timer).
func startResponseReader(
	sub *nats.Subscription, out chan<- *nats.Msg,
) {
	if sub == nil {
		panic("startResponseReader: sub must not be nil")
	}
	if out == nil {
		panic("startResponseReader: out must not be nil")
	}
	go func() {
		// 24h cap so a hung subscription cannot block forever even
		// if the awaitResponse select somehow misses its other arms.
		msg, err := sub.NextMsg(24 * time.Hour)
		if err != nil {
			return
		}
		select {
		case out <- msg:
		default:
		}
	}()
}

// startFailureObserver subscribes (plain NATS) to history.<runID> and
// forwards the first workflow.failed or workflow.cancelled event to
// failCh. Returns a teardown func that the caller MUST defer. Returns
// an error if the subscription cannot be opened — failing fast is
// safer than silently losing failure signals.
func startFailureObserver(
	nc *nats.Conn, runID string, failCh chan<- failureSignal,
) (func(), error) {
	if nc == nil {
		panic("startFailureObserver: nc must not be nil")
	}
	if runID == "" {
		panic("startFailureObserver: runID must not be empty")
	}
	sub, err := nc.SubscribeSync("history." + runID)
	if err != nil {
		return nil, fmt.Errorf("subscribe history: %w", err)
	}
	if err := nc.Flush(); err != nil {
		_ = sub.Unsubscribe()
		return nil, fmt.Errorf("flush: %w", err)
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go observeFailureEvents(sub, failCh, stop, done)
	return func() {
		close(stop)
		_ = sub.Unsubscribe()
		<-done
	}, nil
}

// observeFailureEvents loops on the per-run history subscription
// looking for the two terminal events the HTTP handler reacts to. It
// exits on stop close or first matching event. Bounded loop (10k
// iterations) per TigerStyle "all loops bounded".
func observeFailureEvents(
	sub *nats.Subscription, out chan<- failureSignal,
	stop <-chan struct{}, done chan<- struct{},
) {
	defer close(done)
	for i := 0; i < 10000; i++ {
		select {
		case <-stop:
			return
		default:
		}
		msg, err := sub.NextMsg(50 * time.Millisecond)
		if err != nil {
			continue
		}
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data, &evt); err != nil {
			continue
		}
		sig, ok := failureSignalFor(evt.Type)
		if !ok {
			continue
		}
		select {
		case out <- sig:
		default:
		}
		return
	}
}

// failureSignalFor maps a protocol event type to the failureSignal
// kind the HTTP handler reacts to. Returns ok=false for events the
// handler ignores (step.*, etc.) — keeps the select arm shallow.
func failureSignalFor(t protocol.EventType) (failureSignal, bool) {
	switch t {
	case protocol.EventWorkflowFailed:
		return failureSignal{kind: "failed"}, true
	case protocol.EventWorkflowCancelled:
		return failureSignal{kind: "cancelled"}, true
	}
	return failureSignal{}, false
}

// writeFailureResponse maps a failureSignal to the documented HTTP
// status + body shape. Keep parallel to ADR-013 §"Failure handling".
func writeFailureResponse(
	w http.ResponseWriter, runID string, sig failureSignal,
) {
	if w == nil {
		panic("writeFailureResponse: w must not be nil")
	}
	if runID == "" {
		panic("writeFailureResponse: runID must not be empty")
	}
	switch sig.kind {
	case "failed":
		http.Error(w,
			`{"error":"workflow_failed","run_id":"`+runID+`"}`,
			http.StatusInternalServerError,
		)
	case "cancelled":
		http.Error(w,
			`{"error":"workflow_cancelled","run_id":"`+runID+`"}`,
			http.StatusServiceUnavailable,
		)
	default:
		// Unknown kind is a programmer error — failureSignalFor only
		// returns the two cases above.
		panic("writeFailureResponse: unknown sig kind " + sig.kind)
	}
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
