// trigger/http_idempotency_unhealth_test.go
//
// Methodology: integration tests with embedded NATS that delete the
// http_idempotency KV bucket between handler construction and the
// first request, so the cached KV handle inside HTTPHandler points at
// a now-missing stream. Both `idkv.Create` and the follow-up `idkv.Get`
// therefore fail. The handler must:
//
//	(1) degrade gracefully to PR-2 behavior (fresh runID per request,
//	    no replay) — already documented and acceptable.
//	(2) emit a structured warning so operators can see the silent
//	    degradation rather than wonder why duplicate requests no
//	    longer dedup.
//
// The (2) requirement is the F4 audit finding this file pins.
package trigger

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// installLogCapture swaps slog.Default with a thread-safe capturing
// handler whose output is collected into a buffer and restored on
// t.Cleanup. Inlined here (rather than imported from dagnatstest)
// because dagnatstest transitively imports internal/trigger, creating
// a test-import cycle.
func installLogCapture(t *testing.T) *logBuf {
	t.Helper()
	prior := slog.Default()
	buf := &logBuf{}
	captured := slog.New(slog.NewTextHandler(buf, nil))
	slog.SetDefault(captured)
	t.Cleanup(func() { slog.SetDefault(prior) })
	return buf
}

// logBuf is a thread-safe io.Writer the slog handler dumps lines into.
type logBuf struct {
	mu    sync.Mutex
	lines []string
}

func (b *logBuf) Write(p []byte) (int, error) {
	if b == nil {
		panic("logBuf.Write: receiver must not be nil")
	}
	if len(p) == 0 {
		panic("logBuf.Write: zero-length write (slog should not do this)")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.lines = append(b.lines, string(p))
	return len(p), nil
}

// Hits counts lines containing substr.
func (b *logBuf) Hits(substr string) int {
	if b == nil {
		panic("logBuf.Hits: receiver must not be nil")
	}
	if substr == "" {
		panic("logBuf.Hits: substr must not be empty")
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	count := 0
	for _, ln := range b.lines {
		if strings.Contains(ln, substr) {
			count++
		}
	}
	return count
}

func TestHTTPHandlerLogsWarningWhenIdempotencyKVUnhealthy(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-idem-unhealthy",
		WorkflowID: "wf-idem-unhealthy",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:              "/api/unhealthy",
			Method:            http.MethodPost,
			TimeoutMs:         2_000,
			MaxBodyBytes:      1024,
			IdempotencyHeader: "Idempotency-Key",
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	// Capture slog output BEFORE constructing the handler so the warn
	// emission has somewhere to land.
	capture := installLogCapture(t)

	handler := NewHTTPHandler(nc, def)

	// Sabotage the KV after the handler has captured the (now stale)
	// handle. Subsequent Create/Get calls will fail with "stream not
	// found" — the exact failure mode F4 guards against.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()
	if err := js.DeleteKeyValue(ctx, idempotencyKVBucket); err != nil {
		t.Fatalf("DeleteKeyValue: %v", err)
	}

	stop := startFakeRespondEngine(t, nc, httpRespondWire{
		Status:      200,
		ContentType: "application/json",
		Body:        []byte(`{"ok":true}`),
	})
	defer stop()

	req := httptest.NewRequest(
		http.MethodPost, "/api/unhealthy",
		bytes.NewReader([]byte(`{"x":1}`)),
	)
	req.Header.Set("Idempotency-Key", "key-unhealthy-1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	// Positive space: the request still succeeds — the audit explicitly
	// chose "degrade to PR-2 behavior" over "503 on KV outage".
	if rec.Code != http.StatusOK {
		t.Fatalf(
			"status = %d, want 200 (handler must degrade gracefully)",
			rec.Code,
		)
	}

	// Negative space (the bug F4 fixes): the operator MUST see a
	// structured warning naming the trigger and key. Without this, a
	// KV outage is operationally invisible.
	hits := capture.Hits("http idempotency KV unhealthy")
	if hits == 0 {
		t.Fatalf("expected slog warning for KV unhealth, got 0 hits")
	}
}

// TestHTTPHandlerLogsWarningWhenStoreResultPutFails pins the F3 audit
// finding: storeResult previously discarded its Put error via `_, _ =`,
// rendering KV-result-store failures operationally invisible. With KV
// deleted, the engine's response still reaches the client (live
// response is on the wire before storeResult runs) but the KV Put MUST
// emit a structured warning so the silent-replay-degradation does not
// stay silent.
func TestHTTPHandlerLogsWarningWhenStoreResultPutFails(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-store-unhealthy",
		WorkflowID: "wf-store-unhealthy",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:              "/api/store-unhealthy",
			Method:            http.MethodPost,
			TimeoutMs:         2_000,
			MaxBodyBytes:      1024,
			IdempotencyHeader: "Idempotency-Key",
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	capture := installLogCapture(t)

	handler := NewHTTPHandler(nc, def)

	// Same sabotage as the F4 test: stale KV handle, deleted bucket.
	// Both Create (resolveIdempotentRunID) and Put (storeResult) will
	// fail. The F4 warning fires from the resolve path; this test pins
	// the F3 warning that fires from the storeResult path.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 2*time.Second,
	)
	defer cancel()
	if err := js.DeleteKeyValue(ctx, idempotencyKVBucket); err != nil {
		t.Fatalf("DeleteKeyValue: %v", err)
	}

	stop := startFakeRespondEngine(t, nc, httpRespondWire{
		Status:      200,
		ContentType: "application/json",
		Body:        []byte(`{"ok":true}`),
	})
	defer stop()

	req := httptest.NewRequest(
		http.MethodPost, "/api/store-unhealthy",
		bytes.NewReader([]byte(`{"x":1}`)),
	)
	req.Header.Set("Idempotency-Key", "key-store-1")
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	if hits := capture.Hits("http idempotency result Put failed"); hits == 0 {
		t.Fatalf("expected slog warning for storeResult Put failure, got 0 hits")
	}
}
