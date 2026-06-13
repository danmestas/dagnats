// metrics_page_test.go covers the /console/metrics page and the
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
	"context"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/api"
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

func (f *fakeMetricsSource) addCounterLabeled(
	name string, value float64, ts time.Time, labels map[string]string,
) {
	pts := append(f.series[name].Points, MetricPoint{
		Value: value, Timestamp: ts, Labels: labels,
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
	rec := exerciseMetrics(t, cfg, "/console/metrics")
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
	rec := exerciseMetrics(t, cfg, "/console/metrics")
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
	rec := exerciseMetrics(t, cfg, "/console/metrics")
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
// legacy "tile-runs-rate" id moved exclusively to /console/metrics.
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
	// Always-on tile is present even with no aggregator.
	mustContainMetrics(t, body, "id=\"tile-failed-1h\"")
	// Issue #284: placeholders dropped entirely when metrics absent —
	// no "telemetry pending" copy, no is-empty class.
	if strings.Contains(body, "telemetry pending") {
		t.Error("dashboard must not render 'telemetry pending' placeholder copy")
	}
	if strings.Contains(body, "is-empty") {
		t.Error("dashboard must not render is-empty placeholder class")
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

// TestClampChartWindow_DropsEpochJunkAndFutureTicks pins the helper
// that keeps the rendered x-axis inside the real recent window. The
// adversarial inputs mix epoch junk (0), a year-1 unset-timestamp
// (time.Time{}.Unix()), and two sane recent points. Positive: the
// clamped domain hugs the recent points. Negative: lo must not drop
// to the year-1 sentinel and hi must not exceed now.
func TestClampChartWindow_DropsEpochJunkAndFutureTicks(t *testing.T) {
	now := time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
	year1 := float64(time.Time{}.Unix())
	xs := []float64{
		0,
		year1,
		float64(now.Add(-30 * time.Minute).Unix()),
		float64(now.Unix()),
	}
	lo, hi := clampChartWindow(xs, now)
	if hi > float64(now.Unix()) {
		t.Fatalf("hi = %v, must not exceed now %v", hi, now.Unix())
	}
	if lo <= 0 || lo <= year1 {
		t.Fatalf("lo = %v, must drop epoch/year-1 junk", lo)
	}
	if hi-lo > 3600 {
		t.Fatalf("span = %v, want <= 3600s", hi-lo)
	}
	if lo > hi {
		t.Fatalf("lo %v must not exceed hi %v", lo, hi)
	}
}

// TestBuildThroughputChart_SparseDataStaysInRecentWindow is the
// load-bearing regression for the time-axis bug: sparse counter points
// plus one adversarial unset-timestamp point must not poison the
// rendered domain. Positive: XMin/XMax hug the real recent window.
// Negative: no XAxis entry is <= 0 (year-1 contamination dropped) and
// XMax never reaches into the future.
func TestBuildThroughputChart_SparseDataStaysInRecentWindow(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now().UTC()
	src.addCounter("workflow.runs.completed", 3,
		now.Add(-2*time.Minute))
	src.addCounter("workflow.runs.completed", 5, now)
	// Adversarial: a point with an unset Timestamp (year 1).
	src.addCounter("workflow.runs.completed", 4, time.Time{})
	chart := buildThroughputChart(src)
	for i, x := range chart.XAxis {
		if x <= 0 {
			t.Fatalf("XAxis[%d] = %v, must drop x <= 0", i, x)
		}
	}
	if chart.XMax > float64(now.Unix())+1 {
		t.Fatalf("XMax = %v, must not exceed now %v",
			chart.XMax, now.Unix())
	}
	if chart.XMin < chart.XMax-3600 {
		t.Fatalf("XMin = %v, want >= XMax-3600 (%v)",
			chart.XMin, chart.XMax-3600)
	}
	if chart.XMax-chart.XMin > 3600 {
		t.Fatalf("span = %v, want <= 3600s", chart.XMax-chart.XMin)
	}
}

// TestBuildMetricsView_DLQDepthTileFromDataSource verifies the DLQ
// depth tile reads its value from cfg.Data via ListDeadLetters (the
// proven dashboard read), not from a metrics counter. Positive: the
// tile value equals the dead-letter count. Negative: with nil cfg.Data
// the tile is omitted entirely (honest: no source wired).
func TestBuildMetricsView_DLQDepthTileFromDataSource(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now().UTC()
	src.addCounter("workflow.runs.completed", 5, now)
	src.addCounter("workflow.runs.failed", 2, now)
	ds := newFakeDS()
	ds.deadLetters = []api.DeadLetterView{
		{DeadLetter: api.DeadLetter{Sequence: 1}},
		{DeadLetter: api.DeadLetter{Sequence: 2}},
		{DeadLetter: api.DeadLetter{Sequence: 3}},
	}
	cfg := makeMetricsCfg(t, src)
	cfg.Data = ds
	view := buildMetricsView(context.Background(), cfg, "")
	tile, ok := findTile(view.Tiles, "tile-dlq-depth")
	if !ok {
		t.Fatal("dlq-depth tile missing when cfg.Data wired")
	}
	if tile.Value != "3" {
		t.Fatalf("dlq tile Value = %q, want 3", tile.Value)
	}
	cfg.Data = nil
	view = buildMetricsView(context.Background(), cfg, "")
	if _, ok := findTile(view.Tiles, "tile-dlq-depth"); ok {
		t.Fatal("dlq-depth tile must be omitted with nil cfg.Data")
	}
}

// TestBuildMetricsView_RunsActiveTileConditional verifies the
// runs.active SeriesCard renders only when the aggregator actually
// holds the series. Positive: present + labelled with the real OTel
// name when seeded. Negative: omitted (not an empty tile) when absent.
func TestBuildMetricsView_RunsActiveTileConditional(t *testing.T) {
	now := time.Now().UTC()
	withActive := newFakeMetricsSource()
	withActive.addCounter("workflow.runs.completed", 5, now)
	withActive.addCounter("workflow.runs.active", 4, now)
	cfg := makeMetricsCfg(t, withActive)
	view := buildMetricsView(context.Background(), cfg, "")
	tile, ok := findTile(view.Tiles, "tile-runs-active")
	if !ok {
		t.Fatal("runs-active tile missing when series seeded")
	}
	if tile.MetricID != "workflow.runs.active" {
		t.Fatalf("MetricID = %q, want workflow.runs.active",
			tile.MetricID)
	}
	noActive := newFakeMetricsSource()
	noActive.addCounter("workflow.runs.completed", 5, now)
	cfg = makeMetricsCfg(t, noActive)
	view = buildMetricsView(context.Background(), cfg, "")
	if _, ok := findTile(view.Tiles, "tile-runs-active"); ok {
		t.Fatal("runs-active tile must be omitted when series absent")
	}
}

// TestMetricsPage_OmitsUnbackedAffordances guards the honesty
// decisions: no anomaly callout banner (no live anomaly datum), no
// Prometheus button (endpoint is loopback-gated on the engine
// listener, cross-origin from the console).
func TestMetricsPage_OmitsUnbackedAffordances(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now().UTC()
	src.addCounter("workflow.runs.completed", 5, now)
	cfg := makeMetricsCfg(t, src)
	rec := exerciseMetrics(t, cfg, "/console/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if strings.Contains(body, "Anomaly · snapshot.save.duration_ms") {
		t.Error("must not render the fixture anomaly callout banner")
	}
	if strings.Contains(body, "GET /metrics") {
		t.Error("must not render Prometheus button (cross-origin gate)")
	}
}

// findTile returns the tile with the given ID, mirroring the
// conditional-append shape the view builder uses.
func findTile(tiles []MetricsTile, id string) (MetricsTile, bool) {
	for _, tile := range tiles {
		if tile.ID == id {
			return tile, true
		}
	}
	return MetricsTile{}, false
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
