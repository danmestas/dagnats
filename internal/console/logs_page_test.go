// logs_page_test.go covers the /console/logs surface:
//   - TestLogsPage_RendersControls: severity select, search input,
//     trace-ID input, pause/resume button render in the DOM.
//   - TestLogsPage_TraceIDFilter: only rows matching trace_id=... are
//     rendered when the query parameter is set.
//   - TestLogsPage_EmptyStateWhenRingMissing: an unwired Config still
//     loads the page chrome instead of returning 503 — the operator
//     should see the nav entry resolved even before observability is
//     plumbed.
//
// Methodology:
//   - Each test mounts its own console.Mount; no shared state.
//   - The LogTailSource is satisfied by a tiny fakeLogRing seeded
//     per-test so trace-id matching can be asserted with concrete
//     records.
//   - Min 2 assertions per test (positive + negative space).
package console

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeLogRing satisfies LogTailSource without pulling in the logring
// package. Tests seed it with the records they want to assert against.
type fakeLogRing struct {
	records []slog.Record
	cleared int
}

func (f *fakeLogRing) Snapshot() []slog.Record {
	out := make([]slog.Record, len(f.records))
	copy(out, f.records)
	return out
}

func (f *fakeLogRing) Subscribe(_ context.Context) (<-chan slog.Record, func()) {
	ch := make(chan slog.Record)
	close(ch)
	return ch, func() {}
}

// Clear resets the seeded records so the test assertions match the
// behaviour the production ring promises (Snapshot() empty after
// Clear). cleared is bumped so tests can assert the call ran.
func (f *fakeLogRing) Clear() {
	f.records = nil
	f.cleared++
}

// mountWithLogRing builds a console handler with a LogTailSource
// attached. fake is allowed to be nil for the empty-state path.
func mountWithLogRing(t *testing.T, fake LogTailSource) http.Handler {
	t.Helper()
	return Mount(Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "test",
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Data:     newFakeDS(),
		LogRing:  fake,
	})
}

// mountWithLogRingAuth boots the console under AuthForwarded so the
// CSRF middleware fires — needed for the Clear endpoint tests that
// must verify the token is enforced.
func mountWithLogRingAuth(
	t *testing.T, fake LogTailSource, mode AuthMode,
) http.Handler {
	t.Helper()
	if _, _, err := LoadCSRFSecret(
		"test-secret-fixed-32-bytes-fixed-",
	); err != nil {
		t.Fatalf("LoadCSRFSecret: %v", err)
	}
	password := ""
	if mode == AuthBasic {
		password = "pw"
	}
	return Mount(Config{
		HTTPAddr: "10.0.0.1:9999",
		AuthMode: mode,
		Password: password,
		Build:    "test",
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
		Data:     newFakeDS(),
		LogRing:  fake,
	})
}

// seedRecord builds one slog.Record at the given offset from base
// with the supplied attrs flattened into key/value pairs.
func seedRecord(
	base time.Time, offset time.Duration, lvl slog.Level,
	msg string, attrs ...any,
) slog.Record {
	rec := slog.NewRecord(base.Add(offset), lvl, msg, 0)
	rec.Add(attrs...)
	return rec
}

func TestLogsPage_RendersControls(t *testing.T) {
	t.Parallel()
	base := time.Now()
	fake := &fakeLogRing{
		records: []slog.Record{
			seedRecord(base, -2*time.Second, slog.LevelInfo, "engine: startup"),
			seedRecord(base, -1*time.Second, slog.LevelWarn, "trigger: skew",
				"trace_id", "abc123"),
		},
	}
	srv := httptest.NewServer(mountWithLogRing(t, fake))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/console/logs")
	if err != nil {
		t.Fatalf("GET /console/logs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	got := string(body)
	mustContain := []string{
		`id="logs-severity"`,
		`id="logs-search"`,
		`id="logs-trace-id"`,
		`id="logs-pause-resume"`,
		`id="logs-tbody"`,
		`engine: startup`,
		`trigger: skew`,
	}
	for _, want := range mustContain {
		if !strings.Contains(got, want) {
			t.Fatalf("body missing %q\n--- body ---\n%s", want, got)
		}
	}
	// Negative space: the page must NOT render the "log tail not wired"
	// banner when LogRing is non-nil.
	if strings.Contains(got, `data-empty="logs-not-wired"`) {
		t.Fatalf("logs-not-wired banner rendered despite LogRing wired")
	}
}

func TestLogsPage_TraceIDFilter(t *testing.T) {
	t.Parallel()
	base := time.Now()
	fake := &fakeLogRing{
		records: []slog.Record{
			seedRecord(base, -3*time.Second, slog.LevelInfo,
				"unrelated entry"),
			seedRecord(base, -2*time.Second, slog.LevelInfo,
				"first matching", "trace_id", "deadbeef"),
			seedRecord(base, -1*time.Second, slog.LevelWarn,
				"second matching", "trace_id", "deadbeef"),
			seedRecord(base, -500*time.Millisecond, slog.LevelInfo,
				"another trace", "trace_id", "feedface"),
		},
	}
	srv := httptest.NewServer(mountWithLogRing(t, fake))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/console/logs?trace_id=deadbeef")
	if err != nil {
		t.Fatalf("GET /console/logs: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	got := string(body)
	// Positive: matching messages present.
	if !strings.Contains(got, "first matching") {
		t.Fatalf("first matching trace_id row missing")
	}
	if !strings.Contains(got, "second matching") {
		t.Fatalf("second matching trace_id row missing")
	}
	// Negative space: non-matching messages must be filtered out.
	if strings.Contains(got, "unrelated entry") {
		t.Fatalf("unrelated entry leaked through trace_id filter")
	}
	if strings.Contains(got, "another trace") {
		t.Fatalf("different trace_id leaked through filter")
	}
}

func TestLogsPage_EmptyStateWhenRingMissing(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(mountWithLogRing(t, nil))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/console/logs")
	if err != nil {
		t.Fatalf("GET /console/logs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200 (empty state, not 503)",
			resp.StatusCode)
	}
	body, _ := io.ReadAll(resp.Body)
	got := string(body)
	if !strings.Contains(got, `data-empty="logs-not-wired"`) {
		t.Fatalf("not-wired empty state missing\n%s", got)
	}
	// Negative space: chrome still renders so the operator sees the
	// nav entry resolved. The Logs anchor in the layout must appear.
	if !strings.Contains(got, `href="/console/logs"`) {
		t.Fatalf("Logs nav anchor missing on empty state")
	}
}

func TestLogsPage_SeverityFilter(t *testing.T) {
	t.Parallel()
	base := time.Now()
	fake := &fakeLogRing{
		records: []slog.Record{
			seedRecord(base, -3*time.Second, slog.LevelInfo, "info msg"),
			seedRecord(base, -2*time.Second, slog.LevelWarn, "warn msg"),
			seedRecord(base, -1*time.Second, slog.LevelError, "error msg"),
		},
	}
	srv := httptest.NewServer(mountWithLogRing(t, fake))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/console/logs?severity=warn")
	if err != nil {
		t.Fatalf("GET /console/logs: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	got := string(body)
	if !strings.Contains(got, "warn msg") {
		t.Fatalf("warn row missing from severity-filtered view")
	}
	if strings.Contains(got, "info msg") {
		t.Fatalf("info row leaked through severity=warn")
	}
}

// TestLogsPage_ExportEndpoint validates GET /console/logs/export
// returns a non-empty plain-text body with one line per snapshot
// entry. Methodology: seed 3 records, GET, count newline-terminated
// lines, assert line count matches Snapshot() length and the first
// line carries the level + source projection the brief specifies.
func TestLogsPage_ExportEndpoint(t *testing.T) {
	t.Parallel()
	base := time.Now()
	fake := &fakeLogRing{
		records: []slog.Record{
			seedRecord(base, -3*time.Second, slog.LevelInfo, "first"),
			seedRecord(base, -2*time.Second, slog.LevelWarn, "second"),
			seedRecord(base, -1*time.Second, slog.LevelError, "third"),
		},
	}
	srv := httptest.NewServer(mountWithLogRing(t, fake))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/console/logs/export")
	if err != nil {
		t.Fatalf("GET /console/logs/export: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); !strings.HasPrefix(ct, "text/plain") {
		t.Fatalf("Content-Type = %q, want text/plain", ct)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	if len(body) == 0 {
		t.Fatalf("export body is empty")
	}
	// Positive: line count matches snapshot length.
	lines := strings.Split(strings.TrimRight(string(body), "\n"), "\n")
	if got, want := len(lines), len(fake.records); got != want {
		t.Fatalf("line count = %d, want %d\nbody=%s", got, want, body)
	}
	// Positive: first line carries the level + message projection.
	if !strings.Contains(lines[0], "[INFO]") || !strings.Contains(lines[0], "first") {
		t.Fatalf("first line missing level/message: %q", lines[0])
	}
	// Negative space: format=json switches content type.
	respJSON, err := http.Get(srv.URL + "/console/logs/export?format=json")
	if err != nil {
		t.Fatalf("GET export?format=json: %v", err)
	}
	defer respJSON.Body.Close()
	if ct := respJSON.Header.Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("json Content-Type = %q, want application/json", ct)
	}
}

// TestLogsPage_ClearEndpoint validates POST /console/logs/clear with a
// loopback Mount (no CSRF) drains the ring and returns 204.
// Methodology: seed 3 records, POST, verify Snapshot() is empty and
// cleared counter bumped.
func TestLogsPage_ClearEndpoint(t *testing.T) {
	t.Parallel()
	base := time.Now()
	fake := &fakeLogRing{
		records: []slog.Record{
			seedRecord(base, -3*time.Second, slog.LevelInfo, "a"),
			seedRecord(base, -2*time.Second, slog.LevelInfo, "b"),
			seedRecord(base, -1*time.Second, slog.LevelInfo, "c"),
		},
	}
	srv := httptest.NewServer(mountWithLogRing(t, fake))
	defer srv.Close()
	resp, err := http.Post(srv.URL+"/console/logs/clear",
		"application/x-www-form-urlencoded", nil)
	if err != nil {
		t.Fatalf("POST clear: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("status = %d, want 204", resp.StatusCode)
	}
	// Positive: ring drained.
	if got := len(fake.Snapshot()); got != 0 {
		t.Fatalf("post-clear snapshot len = %d, want 0", got)
	}
	// Negative space: Clear() was called exactly once.
	if fake.cleared != 1 {
		t.Fatalf("cleared = %d, want 1", fake.cleared)
	}
}

// TestLogsPage_ClearRequiresCSRF validates a non-loopback Mount
// rejects POSTs without an X-CSRF-Token header. Methodology: boot
// AuthForwarded, POST without token, assert 403 and Clear() never
// ran.
func TestLogsPage_ClearRequiresCSRF(t *testing.T) {
	fake := &fakeLogRing{
		records: []slog.Record{
			seedRecord(time.Now(), 0, slog.LevelInfo, "only"),
		},
	}
	h := mountWithLogRingAuth(t, fake, AuthForwarded)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/console/logs/clear", nil)
	req.Header.Set("X-Forwarded-User", "alice")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want 403", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "csrf_invalid") {
		t.Fatalf("expected csrf_invalid; body=%s", rr.Body.String())
	}
	// Negative space: Clear() must NOT have run.
	if fake.cleared != 0 {
		t.Fatalf("Clear() ran despite missing CSRF: cleared=%d", fake.cleared)
	}
	// Positive: with a valid token, the call succeeds.
	good := CSRFTokenForActor(Actor{User: "alice", Source: AuthForwarded})
	if good == "" {
		t.Fatalf("expected non-empty CSRF token")
	}
	rr2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/console/logs/clear", nil)
	req2.Header.Set("X-Forwarded-User", "alice")
	req2.Header.Set("X-CSRF-Token", good)
	h.ServeHTTP(rr2, req2)
	if rr2.Code != http.StatusNoContent {
		t.Fatalf("status with valid token = %d, want 204; body=%s",
			rr2.Code, rr2.Body.String())
	}
	if fake.cleared != 1 {
		t.Fatalf("cleared after valid POST = %d, want 1", fake.cleared)
	}
}
