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
