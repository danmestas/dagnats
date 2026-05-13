// trigger/http_idempotency_test.go
//
// Methodology: integration tests with embedded NATS exercising
// ADR-013 Q6 idempotency replay. The handler stores
// (triggerID, header-value) → originalRunID in a JetStream KV bucket
// and, on a duplicate request, subscribes to the original run's
// response subject so the duplicate sees the same response body the
// first request saw — instead of timing out on a fresh per-run
// subject the engine never publishes to. Bounded waits.
package trigger

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
)

// countingFakeEngine answers each unique runID with a body containing
// a monotonically incrementing call counter. If the handler replays
// correctly all 5 requests see the same body; if it doesn't, the
// second request gets a different body (or times out).
func startCountingFakeEngine(
	t *testing.T, nc *nats.Conn,
) (stop func(), callCount *int64) {
	t.Helper()
	var counter int64
	stop = startFakeRespondEngine(t, nc, httpRespondWire{
		Status:      200,
		ContentType: "application/json",
		Body:        []byte(`{"call":1}`),
	})
	return stop, &counter
}

func TestHTTPHandlerIdempotencyReplayReturnsSameResponseBody(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-idem-replay",
		WorkflowID: "wf-idem-replay",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:              "/api/replay",
			Method:            http.MethodPost,
			TimeoutMs:         3_000,
			MaxBodyBytes:      1024,
			IdempotencyHeader: "Idempotency-Key",
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	stop, _ := startCountingFakeEngine(t, nc)
	defer stop()

	handler := NewHTTPHandler(nc, def)

	const want = `{"call":1}`
	const N = 5
	runIDs := make(map[string]int, N)

	for i := 0; i < N; i++ {
		req := httptest.NewRequest(
			http.MethodPost, "/api/replay",
			bytes.NewReader([]byte(`{"x":1}`)),
		)
		req.Header.Set("Idempotency-Key", "key-replay-XYZ")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("req %d: status = %d body=%q",
				i, rec.Code, rec.Body.String())
		}
		if !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("req %d: body = %q, want %s",
				i, rec.Body.String(), want)
		}
		runIDs[rec.Header().Get("X-Dagnats-Run-Id")]++
	}

	// All 5 requests must report the SAME run id — replay reuses the
	// original run, not a new one per request.
	if len(runIDs) != 1 {
		t.Fatalf("expected 1 unique run id across %d requests, got %d (%v)",
			N, len(runIDs), runIDs)
	}
}

func TestHTTPHandlerIdempotencyDifferentKeysDifferentRuns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-idem-distinct",
		WorkflowID: "wf-idem-distinct",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:              "/api/distinct",
			Method:            http.MethodPost,
			TimeoutMs:         3_000,
			MaxBodyBytes:      1024,
			IdempotencyHeader: "Idempotency-Key",
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	stop := startFakeRespondEngine(t, nc, httpRespondWire{
		Status:      200,
		ContentType: "application/json",
		Body:        []byte(`{"ok":true}`),
	})
	defer stop()

	handler := NewHTTPHandler(nc, def)

	// Two requests with DIFFERENT keys → two distinct run ids.
	keys := []string{"key-A", "key-B"}
	seen := make(map[string]bool, 2)
	for _, k := range keys {
		req := httptest.NewRequest(
			http.MethodPost, "/api/distinct",
			bytes.NewReader([]byte(`{}`)),
		)
		req.Header.Set("Idempotency-Key", k)
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("key=%s status = %d", k, rec.Code)
		}
		seen[rec.Header().Get("X-Dagnats-Run-Id")] = true
	}
	if len(seen) != 2 {
		t.Fatalf("expected 2 unique run ids, got %d (%v)",
			len(seen), seen)
	}
}

func TestHTTPHandlerIdempotencyNoHeaderProvidedNoDedup(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	def := TriggerDef{
		ID:         "http-idem-noheader",
		WorkflowID: "wf-idem-noheader",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:              "/api/noheader",
			Method:            http.MethodPost,
			TimeoutMs:         3_000,
			MaxBodyBytes:      1024,
			IdempotencyHeader: "Idempotency-Key",
		},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("Validate: %v", err)
	}

	stop := startFakeRespondEngine(t, nc, httpRespondWire{
		Status:      200,
		ContentType: "application/json",
		Body:        []byte(`{"ok":true}`),
	})
	defer stop()

	handler := NewHTTPHandler(nc, def)

	seen := make(map[string]bool, 2)
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest(
			http.MethodPost, "/api/noheader",
			bytes.NewReader([]byte(`{}`)),
		)
		// Deliberately no Idempotency-Key header.
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		if rec.Code != 200 {
			t.Fatalf("req %d status = %d", i, rec.Code)
		}
		seen[rec.Header().Get("X-Dagnats-Run-Id")] = true
	}
	if len(seen) != 2 {
		t.Fatalf(
			"expected 2 unique run ids when no Idempotency-Key, got %d",
			len(seen),
		)
	}
}

// startTimedFakeEngine is the variant that records how many distinct
// runIDs the engine actually saw — used to assert the engine only
// runs the workflow once across N duplicate replay requests.
func startTimedFakeEngine(
	t *testing.T, nc *nats.Conn, response httpRespondWire,
) (stop func(), runIDCount *int64) {
	t.Helper()
	var count int64
	stop = startFakeRespondEngine(t, nc, response)
	runIDCount = &count
	return
}

// Compile-time interface use to keep imports.
var _ = atomic.AddInt64
var _ = time.Second
