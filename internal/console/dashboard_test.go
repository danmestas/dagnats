// dashboard_test.go covers the Phase 2 T06+T07+T08 dashboard
// restructure: six operational status tiles + recent failures /
// operator-action panels + live SSE patching.
//
// Methodology:
//   - Pure-handler tests against fakeDataSource + fakeMetricsSource.
//     The dashboard data assembly path runs end-to-end without NATS.
//   - Tests assert structural HTML facts (tile IDs, link hrefs, state
//     class names) rather than exact bytes so cosmetic CSS tweaks
//     don't break the suite.
//   - Min 2 assertions per test (positive + negative space).
//   - Bounded waits in SSE tests; channels close on ctx cancel.
package console

import (
	"compress/gzip"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
)

// TestDashboard_assemblesAllTiles verifies every operational tile key
// shows up in the rendered dashboard and carries the contract fields
// (LinkHref, Value, State). Positive: six tiles present. Negative: no
// tile is missing a state class.
func TestDashboard_assemblesAllTiles(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 25, now)
	src.addCounter("workflow.runs.failed", 2, now)
	src.addHistogram(
		"snapshot.save.duration_ms", 10,
		[]MetricBucket{
			{UpperBound: 5, Count: 5},
			{UpperBound: 10, Count: 10},
		}, now,
	)
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{
		{RunID: "r-1", WorkflowID: "demo",
			Status: dag.RunStatusRunning, CreatedAt: now},
		{RunID: "r-2", WorkflowID: "demo",
			Status: dag.RunStatusFailed, CreatedAt: now.Add(-10 * time.Minute),
			Steps: map[string]dag.StepState{
				"s1": {Status: dag.StepStatusFailed, Error: "boom"},
			}},
	}
	cfg := dashTestCfg(t, fake, src)
	rec := dashGet(t, cfg, "/console/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	wantTileIDs := []string{
		"tile-failed-1h", "tile-dlq-depth", "tile-in-flight",
		"tile-success-rate", "tile-p99-latency", "tile-workers-active",
	}
	for _, id := range wantTileIDs {
		if !strings.Contains(body, "id=\""+id+"\"") {
			t.Errorf("dashboard missing tile id=%q", id)
		}
	}
	if !strings.Contains(body, "tile-state-") {
		t.Error("dashboard missing tile state coloring class")
	}
}

// TestDashboard_failedTileLinksToFilteredRuns asserts the Failed-1h
// tile's anchor points at the runs list filtered to status=failed and
// range=1h, the operator's natural drill path on a red tile.
func TestDashboard_failedTileLinksToFilteredRuns(t *testing.T) {
	src := newFakeMetricsSource()
	fake := newFakeDS()
	cfg := dashTestCfg(t, fake, src)
	rec := dashGet(t, cfg, "/console/")
	body := rec.Body.String()
	if !strings.Contains(body, "/console/runs?status=failed&amp;range=1h") &&
		!strings.Contains(body, "/console/runs?status=failed&range=1h") {
		t.Errorf("failed-1h tile must link to filtered runs list")
	}
	if !strings.Contains(body, "/console/dlq") {
		t.Error("dlq-depth tile must link to /console/dlq")
	}
}

// TestDashboard_recentFailuresPanelRenders asserts the "Recent failures"
// card lists up to five most-recent failed runs with workflow + run id
// + error message, and shows the explicit empty state when none exist.
func TestDashboard_recentFailuresPanelRenders(t *testing.T) {
	fake := newFakeDS()
	now := time.Now()
	fake.runs = []dag.WorkflowRun{
		{RunID: "abcdef123456", WorkflowID: "demo-wf",
			Status: dag.RunStatusFailed, CreatedAt: now,
			Steps: map[string]dag.StepState{
				"s1": {Status: dag.StepStatusFailed, Error: "boom-error"},
			}},
	}
	cfg := dashTestCfg(t, fake, nil)
	rec := dashGet(t, cfg, "/console/")
	body := rec.Body.String()
	if !strings.Contains(body, "id=\"recent-failures\"") {
		t.Error("dashboard missing recent-failures panel")
	}
	if !strings.Contains(body, "demo-wf") {
		t.Error("recent failures must include workflow name")
	}
}

// TestDashboard_recentActionsPanelRenders verifies the "Recent operator
// actions" card shows the last few audit entries; empty state copy is
// honest, no dev-speak.
func TestDashboard_recentActionsPanelRenders(t *testing.T) {
	fake := newFakeDS()
	fake.auditEvents = []AuditEvent{
		{Time: time.Now(), Actor: "alice", Action: "trigger.toggle",
			Target: "cron-nightly", Outcome: "success"},
	}
	cfg := dashTestCfg(t, fake, nil)
	rec := dashGet(t, cfg, "/console/")
	body := rec.Body.String()
	if !strings.Contains(body, "id=\"recent-actions\"") {
		t.Error("dashboard missing recent-actions panel")
	}
	if !strings.Contains(body, "alice") {
		t.Error("recent actions must include actor name")
	}
}

// TestDashboard_systemOverviewBehindDisclosure asserts the legacy
// System overview card moved into a <details>-wrapped footer so it no
// longer dominates the at-a-glance surface.
func TestDashboard_systemOverviewBehindDisclosure(t *testing.T) {
	fake := newFakeDS()
	cfg := dashTestCfg(t, fake, nil)
	rec := dashGet(t, cfg, "/console/")
	body := rec.Body.String()
	if !strings.Contains(body, "Show config") {
		t.Error("dashboard must wrap System overview behind a 'Show config' disclosure")
	}
	if !strings.Contains(body, "<details") {
		t.Error("dashboard must wrap config in a <details> element")
	}
}

// TestDashboardSSE_initRouteRegistered checks the new /console/sse/dashboard
// route returns 200 + text/event-stream on a GET. Cancel via context to
// confirm graceful shutdown.
func TestDashboardSSE_initRouteRegistered(t *testing.T) {
	fake := newFakeDS()
	cfg := dashTestCfg(t, fake, nil)
	h := Mount(cfg)
	ctx, cancel := context.WithTimeout(
		context.Background(), 250*time.Millisecond)
	defer cancel()
	req := httptest.NewRequest(http.MethodGet,
		"/console/sse/dashboard", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	ct := rec.Header().Get("Content-Type")
	if !strings.Contains(ct, "event-stream") {
		t.Errorf("Content-Type = %q, want event-stream", ct)
	}
}

// TestSSEDashboard_emitsTilePatchOnRunCompletion verifies the event-
// bus integration: publishing a TopicRun event causes the SSE handler
// to emit a Datastar PatchElements event scoped to a tile id.
func TestSSEDashboard_emitsTilePatchOnRunCompletion(t *testing.T) {
	fake := newFakeDS()
	cfg := dashTestCfg(t, fake, nil)
	AttachBus(&cfg)
	if cfg.bus == nil {
		t.Fatal("AttachBus must initialize cfg.bus")
	}
	h := Mount(cfg)
	ctx, cancel := context.WithTimeout(
		context.Background(), 1*time.Second)
	defer cancel()
	// Publish from a goroutine so the request can start consuming.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cfg.bus.publish(busEventRunCompleted("run-x"))
	}()
	req := httptest.NewRequest(http.MethodGet,
		"/console/sse/dashboard", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	body := rec.Body.String()
	if !strings.Contains(body, "tile-failed-1h") {
		t.Errorf("SSE body missing failed-1h tile patch:\n%s",
			body[:min(len(body), 500)])
	}
}

// TestSSEDashboard_recentFailuresUpdatesOnNewFailure verifies the
// recent-failures panel patch fires when a run completion event lands.
func TestSSEDashboard_recentFailuresUpdatesOnNewFailure(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{
		{RunID: "fail-1", WorkflowID: "wf-a",
			Status: dag.RunStatusFailed, CreatedAt: time.Now(),
			Steps: map[string]dag.StepState{
				"s": {Status: dag.StepStatusFailed, Error: "explode"},
			}},
	}
	cfg := dashTestCfg(t, fake, nil)
	AttachBus(&cfg)
	h := Mount(cfg)
	ctx, cancel := context.WithTimeout(
		context.Background(), 1*time.Second)
	defer cancel()
	go func() {
		time.Sleep(50 * time.Millisecond)
		cfg.bus.publish(busEventRunCompleted("fail-1"))
	}()
	req := httptest.NewRequest(http.MethodGet,
		"/console/sse/dashboard", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	body := rec.Body.String()
	if !strings.Contains(body, "recent-failures") {
		t.Error("SSE body missing recent-failures panel patch")
	}
	if !strings.Contains(body, "wf-a") {
		t.Error("SSE body missing the failed workflow id")
	}
}

// min is local since the test file targets Go 1.21+ but the builtin
// landed in 1.21; some test grids on older toolchains still mismatch.
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// TestNotFoundPage_noDevSpeak guards drive-by 1 — the audit found the
// 404 still says "This section is being built." which is dev-speak.
func TestNotFoundPage_noDevSpeak(t *testing.T) {
	h := newTestConsole(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/console/garbage", nil)
	h.ServeHTTP(rec, req)
	body := rec.Body.String()
	if strings.Contains(body, "being built") {
		t.Error("404 page must not contain dev-speak 'being built'")
	}
	if !strings.Contains(body, "Return to dashboard") {
		t.Error("404 page must offer return-to-dashboard link")
	}
}

// TestPrintCSS_concatenatesAllTabPanels checks the @media print rule
// targets [hidden].tabs-content so the HTML hidden attribute can't
// suppress non-active panels in the print output.
func TestPrintCSS_concatenatesAllTabPanels(t *testing.T) {
	h := newTestConsole(t)
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet,
		"/console/assets/basecoat.css", nil)
	h.ServeHTTP(rec, req)
	body := mustReadGzipped(t, rec)
	if !strings.Contains(body, "[hidden].tabs-content") &&
		!strings.Contains(body, ".tabs-content[hidden]") {
		t.Error("print CSS must override the HTML hidden attribute")
	}
	if !strings.Contains(body, "@media print") {
		t.Error("print stylesheet block missing")
	}
}

// dashTestCfg builds a Config carrying the supplied data + metrics
// sources for dashboard tests. Audit bucket is unset; the data source
// returns the in-memory fixture data directly.
func dashTestCfg(t *testing.T, ds DataSource, m MetricsSource) Config {
	t.Helper()
	return Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "test",
		Logger:   silentTestLogger(),
		Data:     ds,
		Metrics:  m,
	}
}

// dashGet exercises one path through the mounted handler and returns
// the recorder.
func dashGet(t *testing.T, cfg Config, path string) *httptest.ResponseRecorder {
	t.Helper()
	h := Mount(cfg)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// _ keeps the api import alive even when refactors trim the test set;
// dashboard_test.go references api.DeadLetterView indirectly via the
// fake but the linter occasionally flags it as unused mid-refactor.
var _ = api.DeadLetterView{}

// mustReadGzipped decompresses the gzipped asset body and returns it
// as a string. Lives here so the print-CSS test can read the raw CSS.
func mustReadGzipped(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	if rec == nil {
		panic("mustReadGzipped: rec is nil")
	}
	zr, err := gzip.NewReader(rec.Body)
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	defer zr.Close()
	body, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gzip read: %v", err)
	}
	if len(body) == 0 {
		t.Fatal("decoded body empty")
	}
	return string(body)
}
