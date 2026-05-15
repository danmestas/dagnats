// handler_test.go covers the HTTP handler that wraps the renderer.
//
// Methodology:
//   - httptest.ResponseRecorder + a stub aggregator; no real network.
//   - Bounded: each request gets its own recorder; no shared state.
//   - Minimum 2 assertions per test (status + body / status + header).
package prom

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/observe/metrics"
)

func TestHandler_GETReturnsTextFormatAndContentType(t *testing.T) {
	agg := metrics.NewAggregator(silentLogger())
	defer agg.Close()
	if err := agg.Ingest(
		metrics.Series{Name: "workers", Kind: metrics.KindGauge,
			Description: "Active workers."},
		metrics.Point{Value: 4, Timestamp: time.Now()},
	); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	h := Handler(agg, silentLogger())
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if got := rec.Header().Get("Content-Type"); got != ContentType {
		t.Fatalf("Content-Type = %q, want %q", got, ContentType)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "workers 4") {
		t.Fatalf("body missing scalar sample:\n%s", body)
	}
}

func TestHandler_RejectsNonGET(t *testing.T) {
	agg := metrics.NewAggregator(silentLogger())
	defer agg.Close()
	h := Handler(agg, silentLogger())
	for _, method := range []string{
		http.MethodPost, http.MethodPut, http.MethodDelete,
		http.MethodPatch, http.MethodOptions,
	} {
		req := httptest.NewRequest(method, "/metrics", nil)
		rec := httptest.NewRecorder()
		h(rec, req)
		if rec.Code != http.StatusMethodNotAllowed {
			t.Fatalf("method %s: status = %d, want 405", method, rec.Code)
		}
		if rec.Header().Get("Allow") == "" {
			t.Fatalf("method %s: Allow header missing", method)
		}
	}
}

func TestHandler_HEADAnswersWithoutBody(t *testing.T) {
	agg := metrics.NewAggregator(silentLogger())
	defer agg.Close()
	h := Handler(agg, silentLogger())
	req := httptest.NewRequest(http.MethodHead, "/metrics", nil)
	rec := httptest.NewRecorder()
	h(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if rec.Body.Len() != 0 {
		t.Fatalf("HEAD body = %q, want empty", rec.Body.String())
	}
}
