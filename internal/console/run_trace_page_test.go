// Methodology: drive the standalone /console/runs/<id>/trace full page
// against the in-memory fakeDataSource (no NATS). Each test asserts the
// positive render shape and a negative-space property: the empty state
// shows no fabricated span, and an ok-only trace never leaks the
// status-failed class. The page reuses the shared `trace-tree` component
// so these mirror the run-trace tab tests' coverage at the page level.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestServePageRunTrace_rendersTree(t *testing.T) {
	fake := newFakeDS()
	fake.runTrace = []TraceRow{
		{Depth: 0, Name: "startRun", DurationMs: 2410,
			Status: "ok", SpanID: "a1"},
		{Depth: 1, Name: "step:fetch", DurationMs: 1100,
			Status: "ok", SpanID: "b2"},
	}
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet,
		"/console/runs/run-tr/trace", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s",
			rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	for _, want := range []string{
		"startRun", "step:fetch", `href="/console/runs/run-tr"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("trace page missing %q; body=%s", want, body)
		}
	}
	// Negative: a populated trace must not paint the empty state.
	if strings.Contains(body, "No spans recorded") {
		t.Errorf("populated trace must not show empty state; body=%s",
			body)
	}
}

func TestServePageRunTrace_emptyState(t *testing.T) {
	fake := newFakeDS()
	fake.runTrace = nil // run produced no telemetry
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet,
		"/console/runs/run-tr/trace", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s",
			rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "No spans recorded for this run") {
		t.Errorf("empty trace missing empty-state copy; body=%s", body)
	}
	// Negative: no fabricated span row.
	if strings.Contains(body, "run-trace-row-") {
		t.Errorf("empty trace must not fabricate a span row; body=%s",
			body)
	}
}

func TestServePageRunTrace_statusFailed(t *testing.T) {
	fake := newFakeDS()
	fake.runTrace = []TraceRow{
		{Depth: 0, Name: "errStep", DurationMs: 10,
			Status: "error", SpanID: "e1"},
	}
	handler := mountWithFake(t, fake)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet,
		"/console/runs/run-tr/trace", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "status-failed") {
		t.Errorf("error span must use status-failed; body=%s",
			rec.Body.String())
	}

	// Negative space: an ok-only trace must NOT carry status-failed.
	okFake := newFakeDS()
	okFake.runTrace = []TraceRow{
		{Depth: 0, Name: "okStep", DurationMs: 10,
			Status: "ok", SpanID: "o1"},
	}
	okHandler := mountWithFake(t, okFake)
	okRec := httptest.NewRecorder()
	okHandler.ServeHTTP(okRec, httptest.NewRequest(http.MethodGet,
		"/console/runs/run-tr/trace", nil))
	if strings.Contains(okRec.Body.String(), "status-failed") {
		t.Errorf("ok trace must not contain status-failed; body=%s",
			okRec.Body.String())
	}
}
