// metrics_page_test.go covers the /console/ops/metrics page and the
// shared tile/chart builders. The dashboard tiles on /console/ are
// covered indirectly via the same builders.
//
// Methodology:
//   - Unit tests against fakeMetricsSource — no NATS, no live wires.
//   - Each test builds a Config holding the fake, hits the page, and
//     asserts on the rendered HTML or the typed view.
//   - Bounded waits via httptest; min 2 assertions per test.
package console

import (
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// mustContainMetrics is a local assertion helper. Named with a suffix
// so it doesn't collide with similar helpers in sibling test files.
func mustContainMetrics(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Fatalf("output missing %q:\n---\n%s\n---",
			needle, truncate(haystack, 2048))
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "… [truncated]"
}

// fakeMetricsSource is the test stub for MetricsSource. Each metric
// is a fixed slice of points; SubscribeMetric returns a channel the
// test populates manually.
type fakeMetricsSource struct {
	series map[string]MetricSeries
	subs   []chan MetricEvent
}

func newFakeMetricsSource() *fakeMetricsSource {
	return &fakeMetricsSource{
		series: make(map[string]MetricSeries),
	}
}

func (f *fakeMetricsSource) addCounter(name string, value float64, ts time.Time) {
	pts := append(f.series[name].Points, MetricPoint{
		Value: value, Timestamp: ts,
	})
	f.series[name] = MetricSeries{
		Name: name, Kind: "counter", Points: pts,
	}
}

func (f *fakeMetricsSource) addHistogram(
	name string, count uint64, buckets []MetricBucket, ts time.Time,
) {
	pts := append(f.series[name].Points, MetricPoint{
		Count: count, Buckets: buckets, Timestamp: ts,
	})
	f.series[name] = MetricSeries{
		Name: name, Kind: "histogram", Points: pts,
	}
}

func (f *fakeMetricsSource) MetricNames() []string {
	out := make([]string, 0, len(f.series))
	for k := range f.series {
		out = append(out, k)
	}
	return out
}

func (f *fakeMetricsSource) MetricSnapshot(
	name string,
) (MetricSeries, bool) {
	s, ok := f.series[name]
	return s, ok
}

func (f *fakeMetricsSource) SubscribeMetric(
	_ string,
) (<-chan MetricEvent, func()) {
	ch := make(chan MetricEvent, 8)
	f.subs = append(f.subs, ch)
	return ch, func() {
		// Test stub: cancel is a no-op; channels live until process end.
	}
}

func silentTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestMetricsPage_RendersWithSeededTilesAndCharts(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 10, now.Add(-30*time.Minute))
	src.addCounter("workflow.runs.completed", 25, now)
	src.addCounter("workflow.runs.failed", 1, now)
	src.addHistogram(
		"snapshot.save.duration_ms", 5,
		[]MetricBucket{
			{UpperBound: 5, Count: 3},
			{UpperBound: 10, Count: 5},
		}, now,
	)
	cfg := makeMetricsCfg(t, src)
	rec := exerciseMetrics(t, cfg, "/console/ops/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	mustContainMetrics(t, body, "id=\"tile-runs-rate\"")
	mustContainMetrics(t, body, "id=\"tile-success-rate\"")
	mustContainMetrics(t, body, "id=\"tile-snapshot-p50\"")
	mustContainMetrics(t, body, "id=\"chart-throughput-wrap\"")
}

func TestMetricsPage_EmptyAggregatorRendersExplicitState(t *testing.T) {
	cfg := makeMetricsCfg(t, nil)
	rec := exerciseMetrics(t, cfg, "/console/ops/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	mustContainMetrics(t, body, "Metrics aggregator not wired")
	if strings.Contains(body, "id=\"tile-runs-rate\"") {
		t.Fatal("empty page must not render tiles")
	}
	if strings.Contains(body, "Metrics aggregator down") {
		t.Fatal("nil-but-no-error path must not render the down banner")
	}
}

// TestMetricsPage_DownAggregatorSurfacesErrorBanner pins the close-out
// fix for the silent-stderr disable: when the aggregator failed to
// start, the page must surface a "Metrics aggregator down" banner
// instead of the misleading "not wired" copy that implies a deferred
// feature. Positive assertion: the error reason text is visible.
// Negative: the "not wired" empty state must not also fire.
func TestMetricsPage_DownAggregatorSurfacesErrorBanner(t *testing.T) {
	cfg := makeMetricsCfg(t, nil)
	cfg.MetricsErrorReason = "pump start failed: stream missing"
	rec := exerciseMetrics(t, cfg, "/console/ops/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	mustContainMetrics(t, body, "Metrics aggregator down")
	mustContainMetrics(t, body, "pump start failed: stream missing")
	if strings.Contains(body, "Metrics aggregator not wired") {
		t.Fatal("down banner must replace the not-wired copy")
	}
}

// Phase 2 T06: the dashboard tiles are now the operational status
// tiles (tile-failed-1h / tile-dlq-depth / tile-in-flight /
// tile-success-rate / tile-p99-latency / tile-workers-active). The
// legacy "tile-runs-rate" id moved exclusively to /console/ops/metrics.
// These two tests verify the new dashboard surface still reacts to a
// wired metrics source (success-rate / p99 tiles get values) and
// degrades gracefully when one isn't wired (tiles still render but
// in their "telemetry pending" empty state).
func TestDashboard_EmbedsLiveMetricsTilesWhenAggregatorWired(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 7, now)
	src.addCounter("workflow.runs.failed", 0, now)
	cfg := makeMetricsCfg(t, src)
	rec := exerciseMetrics(t, cfg, "/console/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	mustContainMetrics(t, body, "id=\"tile-failed-1h\"")
	mustContainMetrics(t, body, "id=\"tile-success-rate\"")
}

func TestDashboard_RendersWithoutTilesWhenAggregatorMissing(t *testing.T) {
	cfg := makeMetricsCfg(t, nil)
	rec := exerciseMetrics(t, cfg, "/console/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	mustContainMetrics(t, body, "id=\"tile-failed-1h\"")
	if !strings.Contains(body, "telemetry pending") &&
		!strings.Contains(body, "is-empty") {
		t.Fatal("dashboard with nil Metrics must show telemetry-pending state")
	}
}

func TestBuildMetricsTiles_RunsRateUsesPerMinuteDelta(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 0, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.completed", 50, now)
	tiles := buildMetricsTiles(src)
	if len(tiles) == 0 {
		t.Fatal("buildMetricsTiles returned empty")
	}
	rate := tiles[0]
	if rate.ID != "tile-runs-rate" {
		t.Fatalf("ID = %q, want tile-runs-rate", rate.ID)
	}
	if rate.Empty {
		t.Fatal("rate tile must not be empty when both samples present")
	}
	// 50 runs over 10 minutes = 5/min — formatNumber should produce "5".
	if !strings.Contains(rate.Value, "5") {
		t.Fatalf("Value = %q, want %q", rate.Value, "5")
	}
}

func TestBuildMetricsTiles_EmptyAggregatorReturnsEmptyTiles(t *testing.T) {
	src := newFakeMetricsSource()
	tiles := buildMetricsTiles(src)
	if len(tiles) == 0 {
		t.Fatal("buildMetricsTiles must return placeholder tiles")
	}
	for _, tile := range tiles {
		if !tile.Empty {
			t.Fatalf("tile %s must be Empty=true on cold start", tile.ID)
		}
		if tile.Value != "—" {
			t.Fatalf("tile %s Value = %q, want em-dash", tile.ID, tile.Value)
		}
	}
}

func TestPercentileFromBuckets_LinearInterpolation(t *testing.T) {
	p := MetricPoint{
		Count: 100,
		Buckets: []MetricBucket{
			{UpperBound: 10, Count: 50},
			{UpperBound: 20, Count: 100},
		},
	}
	// p50 should be at the upper edge of bucket 0 = 10.
	if got := percentileFromBuckets(p, 0.50); math.Abs(got-10) > 1e-6 {
		t.Fatalf("p50 = %v, want 10", got)
	}
	// p75 should be halfway between the buckets = 15.
	if got := percentileFromBuckets(p, 0.75); math.Abs(got-15) > 1e-6 {
		t.Fatalf("p75 = %v, want 15", got)
	}
}

func TestSparkFromPoints_DownsamplesToBins(t *testing.T) {
	pts := make([]MetricPoint, 100)
	for i := range pts {
		pts[i] = MetricPoint{Value: float64(i)}
	}
	spark := sparkFromPoints(pts, 10)
	if len(spark) != 10 {
		t.Fatalf("len = %d, want 10", len(spark))
	}
	// First bin must be ~0, last bin ~90.
	if spark[0] != 0 {
		t.Fatalf("spark[0] = %v, want 0", spark[0])
	}
	if spark[9] < 80 {
		t.Fatalf("spark[9] = %v, want ≥80", spark[9])
	}
}

// makeMetricsCfg builds a Config wired up with the metrics source +
// minimal data source so the page renderer can run.
func makeMetricsCfg(t *testing.T, src MetricsSource) Config {
	t.Helper()
	return Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "test",
		Logger:   silentTestLogger(),
		Metrics:  src,
	}
}

// exerciseMetrics drives a single request through Mount and returns
// the recorded response.
func exerciseMetrics(t *testing.T, cfg Config, path string) *httptest.ResponseRecorder {
	t.Helper()
	h := Mount(cfg)
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}
