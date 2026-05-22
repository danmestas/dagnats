// Methodology: integration tests pinning the production console
// wiring. Issue #290 closed-out the "MetricsSource is nil in
// production" gap surfaced by the #281 Ousterhout audit. These tests
// guard the wire: buildConsoleConfig must hand the engine's metrics
// aggregator to the console, and the dashboard's on-demand p99 +
// success-rate tiles must read live values once the aggregator has
// seen data. Both tests boot a full Server (real embedded NATS) so the
// guard catches regressions where the production path silently
// drops the MetricsSource — exactly the failure mode #290 fixes.
package server

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/observe/metrics"
)

// TestProductionMountReceivesNonNilMetricsSource is the audit's
// minimum-viable guard: it asserts the production wiring constructs a
// console.Config whose Metrics field is non-nil when the engine's
// aggregator is wired. Boots a real Server so all upstream deps
// (api.Service, audit KV) are realised exactly as the production path
// does; then calls buildConsoleConfig directly with srv.metricsAgg so
// the assertion is on the canonical handle, not a fake.
//
// Two assertions:
//  1. cfg.Metrics != nil (the wire exists).
//  2. cfg.Metrics observes a metric we Ingest into the same agg
//     (the wire is live, not a stale pointer to a dead instance).
func TestProductionMountReceivesNonNilMetricsSource(t *testing.T) {
	cfg := testConfig(t)
	srv := New(cfg)
	if srv == nil {
		panic("New() returned nil")
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()
	defer func() {
		srv.Stop()
		select {
		case <-errCh:
		case <-time.After(20 * time.Second):
			t.Fatal("Run() did not return within 20s")
		}
	}()
	waitForReady(t, cfg.HTTPAddr)

	if srv.metricsAgg == nil {
		t.Fatal("srv.metricsAgg is nil after Run(); aggregator must " +
			"be wired for the console MetricsSource regression test")
	}

	consoleCfg := buildConsoleConfig(
		srv.cfg.HTTPAddr, srv.svc, srv.nc,
		srv.metricsAgg, srv.metricsErrorReason,
	)
	if consoleCfg.Metrics == nil {
		t.Fatal("buildConsoleConfig returned nil Metrics — the " +
			"production Mount() would receive a placeholder source " +
			"(issue #290 regression)")
	}

	// Liveness: ingest a metric into the canonical aggregator and
	// confirm the MetricsSource sees it. Catches "wired but stale"
	// regressions where the adapter wraps a different aggregator
	// instance than the one the engine pumps into.
	meta := metrics.Series{
		Name: "test.console.wiring",
		Kind: metrics.KindCounter,
	}
	if err := srv.metricsAgg.Ingest(meta, metrics.Point{
		Timestamp: time.Now(), Value: 1,
	}); err != nil {
		t.Fatalf("Ingest failed: %v", err)
	}
	names := consoleCfg.Metrics.MetricNames()
	found := false
	for _, n := range names {
		if n == "test.console.wiring" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("MetricsSource did not see test ingest — wired to "+
			"the wrong aggregator. Names: %v", names)
	}
}

// TestServer_DashboardSeesLiveMetrics is the end-to-end-ish proof: it
// ingests the two counters the dashboard's success-rate tile reads
// (workflow.runs.completed + workflow.runs.failed), then GETs the
// /console/ landing page and asserts the success-rate tile rendered
// with a live value rather than the "telemetry pending" empty state.
// Catches regressions where Mount() receives a non-nil but disconnected
// MetricsSource (e.g., a fresh aggregator the pump never touches).
func TestServer_DashboardSeesLiveMetrics(t *testing.T) {
	cfg := testConfig(t)
	srv := New(cfg)
	if srv == nil {
		panic("New() returned nil")
	}
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()
	defer func() {
		srv.Stop()
		select {
		case <-errCh:
		case <-time.After(20 * time.Second):
			t.Fatal("Run() did not return within 20s")
		}
	}()
	waitForReady(t, cfg.HTTPAddr)
	if srv.metricsAgg == nil {
		t.Fatal("srv.metricsAgg is nil; aggregator must be wired")
	}

	now := time.Now()
	ingestCounter(t, srv.metricsAgg, "workflow.runs.completed", 9, now)
	ingestCounter(t, srv.metricsAgg, "workflow.runs.failed", 1, now)

	body := getDashboardBody(t, cfg.HTTPAddr)

	// Positive: the success-rate tile slot exists.
	if !strings.Contains(body, `id="tile-success-rate"`) {
		t.Fatalf("/console/ missing tile-success-rate slot. body:\n%s",
			truncate(body, 2000))
	}
	// Negative: the success-rate tile is NOT empty. The empty state
	// stamps the `is-empty` class on the tile's anchor; a live tile
	// drops it. Search for the tile's marker followed by absence of
	// is-empty in the same `<a>` element.
	tile := extractTile(body, "tile-success-rate")
	if tile == "" {
		t.Fatalf("could not extract tile-success-rate from body:\n%s",
			truncate(body, 2000))
	}
	if strings.Contains(tile, "is-empty") {
		t.Fatalf("tile-success-rate rendered with is-empty after "+
			"ingesting both counters — MetricsSource is wired but "+
			"disconnected from the pump. tile:\n%s", tile)
	}
	if strings.Contains(tile, "telemetry pending") {
		t.Fatalf("tile-success-rate hint is 'telemetry pending' "+
			"after ingest — wire is broken. tile:\n%s", tile)
	}
}

// waitForReady blocks until /ready returns 200 or fails the test.
// Pattern lifted from the surrounding server_test.go fixtures; kept
// local here so the wiring test stays self-contained.
func waitForReady(t *testing.T, addr string) {
	t.Helper()
	url := fmt.Sprintf("http://%s/ready", addr)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := http.Get(url)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("/ready did not return 200 within 10s")
}

// ingestCounter is a small test helper that pushes one counter point
// into the live aggregator. Bounded by the time.Now caller; panics on
// nil agg so test failures locate quickly.
func ingestCounter(
	t *testing.T, agg *metrics.Aggregator,
	name string, value float64, ts time.Time,
) {
	t.Helper()
	if agg == nil {
		t.Fatal("ingestCounter: agg is nil")
	}
	meta := metrics.Series{Name: name, Kind: metrics.KindCounter}
	if err := agg.Ingest(meta, metrics.Point{
		Timestamp: ts, Value: value,
	}); err != nil {
		t.Fatalf("Ingest %q: %v", name, err)
	}
}

// getDashboardBody fetches /console/ and returns the response body.
// Asserts 200; fails the test on any HTTP error.
func getDashboardBody(t *testing.T, addr string) string {
	t.Helper()
	url := fmt.Sprintf("http://%s/console/", addr)
	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/console/ status = %d, want 200", resp.StatusCode)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(body)
}

// extractTile slices the rendered <a class="dashboard-tile..."
// id="tile-<key>" …> element out of the response body. Returns the
// substring from the opening `<a` to the closing `</a>`. Returns ""
// when the marker is absent. Bounded scan — no regex, no recursion.
func extractTile(body, id string) string {
	marker := fmt.Sprintf(`id="%s"`, id)
	idx := strings.Index(body, marker)
	if idx < 0 {
		return ""
	}
	// Walk backwards to the nearest "<a " — bounded by 4096 chars so a
	// malformed page can't wedge the scan.
	const lookback = 4096
	start := idx
	limit := idx - lookback
	if limit < 0 {
		limit = 0
	}
	for start > limit {
		if start+2 <= len(body) && body[start:start+2] == "<a" {
			break
		}
		start--
	}
	end := strings.Index(body[idx:], "</a>")
	if end < 0 {
		return body[start:]
	}
	return body[start : idx+end+4]
}

// truncate keeps test-failure output bounded. Reads up to n bytes;
// appends an ellipsis when truncated.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…(truncated)"
}
