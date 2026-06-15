// dashboard_spark_gate_test.go pins the mockup-fidelity rule that the
// four STATUS tiles (failed-1h / dlq-depth / in-flight / success-rate)
// render as plain number+label cells with NO sparkline, while only the
// telemetry cards (throughput / error-rate / p50) carry a stroked spark
// gated on >=2 real points.
//
// Methodology:
//   - httptest through the mounted handler with newFakeDS()/fakeMetricsSource,
//     plus direct builder calls on the pure tile constructors.
//   - Each test makes >=2 assertions covering positive presence and the
//     negative space (no console-spark on a status tile / nil Spark on a
//     1-point telemetry series). Bounded loops over the tile slice.
package console

import (
	"strings"
	"testing"
	"time"
)

// TestStatusTilesCarryNoSparkline pins that the status tiles never set
// Sparkline true, so the {{if and .Sparkline .Spark}} guard suppresses
// any spark even if a Spark series leaks in. Positive: telemetry tiles
// (throughput) do set Sparkline. Negative: every status tile is false.
func TestStatusTilesCarryNoSparkline(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now().UTC()
	// Seed the failed counter so the legacy code path would have set a
	// spark on the failed-1h / dlq-depth status tiles.
	src.addCounter("workflow.runs.failed", 1, now.Add(-30*time.Minute))
	src.addCounter("workflow.runs.failed", 4, now)
	src.addCounter("workflow.runs.completed", 10, now.Add(-30*time.Minute))
	src.addCounter("workflow.runs.completed", 130, now)

	counters := dashboardCounters{
		InFlightCount: 2, DLQDepth: 3, FailedLastHr: 4,
	}
	tiles := append(
		assembleStatusTiles(src, counters),
		assembleTelemetryTiles(src)...,
	)

	status := map[string]bool{
		"failed-1h": true, "dlq-depth": true,
		"in-flight": true, "success-rate": true,
	}
	sawThroughput := false
	for _, tile := range tiles {
		if status[tile.Key] && tile.Sparkline {
			t.Errorf("status tile %q must not set Sparkline", tile.Key)
		}
		if status[tile.Key] && tile.Spark != nil {
			t.Errorf("status tile %q must not carry a Spark series", tile.Key)
		}
		if tile.Key == "throughput" {
			sawThroughput = true
			if !tile.Sparkline {
				t.Error("throughput tile must set Sparkline")
			}
		}
	}
	if !sawThroughput {
		t.Fatal("throughput telemetry tile missing from seeded dashboard")
	}
}

// TestStatusTilesRenderNoSparkSpan pins the rendered HTML: a seeded
// failed counter must NOT produce a dashboard-tile-spark span on the
// failed-1h status tile. Negative space guards the visible "swath" bug.
func TestStatusTilesRenderNoSparkSpan(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now().UTC()
	src.addCounter("workflow.runs.failed", 1, now.Add(-30*time.Minute))
	src.addCounter("workflow.runs.failed", 4, now)
	cfg := dashTestCfg(t, newFakeDS(), src)
	body := dashGet(t, cfg, "/console/").Body.String()

	if !strings.Contains(body, `id="tile-failed-1h"`) {
		t.Fatal("failed-1h status tile must always render")
	}
	failedTile := sliceTileHTML(t, body, "tile-failed-1h")
	if strings.Contains(failedTile, "console-spark") {
		t.Errorf("status tile must not render a console-spark span:\n%s",
			failedTile)
	}
}

// TestTelemetrySparkGatedOnTwoPoints pins the honest-omit floor: a
// telemetry tile with a single real point omits the Spark entirely so
// it can never degenerate into a flat block, while the tile itself
// still renders.
func TestTelemetrySparkGatedOnTwoPoints(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now().UTC()
	// error-rate needs the completed+failed pair to render the tile;
	// failed has only one point so its spark must be nil.
	src.addCounter("workflow.runs.completed", 50, now)
	src.addCounter("workflow.runs.failed", 3, now)

	tile, ok := tileErrorRate(src)
	if !ok {
		t.Fatal("error-rate tile must render with a completed+failed pair")
	}
	if tile.Spark != nil {
		t.Errorf("error-rate spark must be nil with <2 failed points, got %v",
			tile.Spark)
	}

	// Two failed points -> the spark is present and gated true.
	src2 := newFakeMetricsSource()
	src2.addCounter("workflow.runs.completed", 50, now)
	src2.addCounter("workflow.runs.failed", 1, now.Add(-20*time.Minute))
	src2.addCounter("workflow.runs.failed", 3, now)
	tile2, ok2 := tileErrorRate(src2)
	if !ok2 {
		t.Fatal("error-rate tile must render with two failed points")
	}
	if tile2.Spark == nil {
		t.Error("error-rate spark must be present with >=2 failed points")
	}
	if !tile2.Sparkline {
		t.Error("error-rate is a telemetry tile and must set Sparkline")
	}
}

// sliceTileHTML returns the <a> tile block whose id matches domID so a
// test can assert on a single tile rather than the whole page. Bounded:
// it slices from the id to the next "</a>".
func sliceTileHTML(t *testing.T, body, domID string) string {
	t.Helper()
	marker := `id="` + domID + `"`
	start := strings.Index(body, marker)
	if start < 0 {
		t.Fatalf("tile %q not found in body", domID)
	}
	// Walk back to the opening <a for this tile.
	open := strings.LastIndex(body[:start], "<a ")
	if open < 0 {
		open = start
	}
	end := strings.Index(body[start:], "</a>")
	if end < 0 {
		t.Fatalf("tile %q has no closing </a>", domID)
	}
	return body[open : start+end+len("</a>")]
}

// TestBuildThroughputChart_AlignsCounterTimestamps pins the time-axis
// merge: completed at [t0,t1,t2] and failed at [t1,t2] must share one
// sorted union axis with failed values landing at the t1/t2 indices,
// NOT front-padded onto t0. Positive: lengths line up on the union.
// Negative: failed[0] is the carry-forward 0 at t0, not the t1 value.
func TestBuildThroughputChart_AlignsCounterTimestamps(t *testing.T) {
	src := newFakeMetricsSource()
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	t0 := base
	t1 := base.Add(2 * time.Minute)
	t2 := base.Add(4 * time.Minute)
	src.addCounter("workflow.runs.completed", 5, t0)
	src.addCounter("workflow.runs.completed", 9, t1)
	src.addCounter("workflow.runs.completed", 14, t2)
	src.addCounter("workflow.runs.failed", 2, t1)
	src.addCounter("workflow.runs.failed", 3, t2)

	chart := buildThroughputChart(src)
	if chart.Empty {
		t.Fatal("chart must not be empty with real points")
	}
	if len(chart.XAxis) != 3 {
		t.Fatalf("XAxis len = %d, want 3 (union of timestamps)", len(chart.XAxis))
	}
	completed, failed := chart.Series[0].Values, chart.Series[1].Values
	if len(completed) != len(chart.XAxis) || len(failed) != len(chart.XAxis) {
		t.Fatalf("series lengths %d/%d must equal XAxis %d",
			len(completed), len(failed), len(chart.XAxis))
	}
	wantX := []float64{
		float64(t0.Unix()), float64(t1.Unix()), float64(t2.Unix()),
	}
	for i, x := range wantX {
		if chart.XAxis[i] != x {
			t.Fatalf("XAxis[%d] = %v, want %v (sorted union)", i, chart.XAxis[i], x)
		}
	}
	// Failed had no t0 sample: carry-forward floor is 0 there, NOT the
	// t1 value front-padded. This is the core regression.
	if failed[0] != 0 {
		t.Errorf("failed[0] = %v, want 0 (no t0 sample, not front-padded)",
			failed[0])
	}
	if failed[1] != 2 || failed[2] != 3 {
		t.Errorf("failed t1/t2 = %v/%v, want 2/3", failed[1], failed[2])
	}
	if completed[0] != 5 || completed[2] != 14 {
		t.Errorf("completed t0/t2 = %v/%v, want 5/14", completed[0], completed[2])
	}
}

// TestBuildThroughputChart_InterleavedTimestamps is the genuine
// regression guard for the merge: completed samples at {t0,t2} and a
// failed sample ONLY at the interior instant t1. The union axis must be
// [t0,t1,t2] with the failed value landing at the t1 index (carry-forward
// thereafter), i.e. failed == [0, v, v]. This distinguishes the merge
// from the old length-only padFront: padFront would have produced a
// 2-long axis (completed's length) with the single failed value
// front-padded to [0, v], placing v at t2 instead of t1 and dropping the
// interior timestamp entirely. That case goes RED on padFront and GREEN
// on mergeCounterAxis.
func TestBuildThroughputChart_InterleavedTimestamps(t *testing.T) {
	src := newFakeMetricsSource()
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	t0 := base
	t1 := base.Add(2 * time.Minute)
	t2 := base.Add(4 * time.Minute)
	src.addCounter("workflow.runs.completed", 5, t0)
	src.addCounter("workflow.runs.completed", 14, t2)
	src.addCounter("workflow.runs.failed", 7, t1)

	chart := buildThroughputChart(src)
	if chart.Empty {
		t.Fatal("chart must not be empty with real points")
	}
	if len(chart.XAxis) != 3 {
		t.Fatalf("XAxis len = %d, want 3 (union {t0,t1,t2})", len(chart.XAxis))
	}
	wantX := []float64{
		float64(t0.Unix()), float64(t1.Unix()), float64(t2.Unix()),
	}
	for i, x := range wantX {
		if chart.XAxis[i] != x {
			t.Fatalf("XAxis[%d] = %v, want %v (sorted union)", i, chart.XAxis[i], x)
		}
	}
	completed, failed := chart.Series[0].Values, chart.Series[1].Values
	if len(completed) != 3 || len(failed) != 3 {
		t.Fatalf("series lengths %d/%d must equal union XAxis 3",
			len(completed), len(failed))
	}
	// The failed value must land at the t1 index, not be front-padded
	// onto t2. Carry-forward holds it at t2 as well.
	wantFailed := []float64{0, 7, 7}
	for i, want := range wantFailed {
		if failed[i] != want {
			t.Errorf("failed[%d] = %v, want %v (interleaved t1 sample)",
				i, failed[i], want)
		}
	}
	// Completed carries forward across the interior t1 it never sampled.
	wantCompleted := []float64{5, 5, 14}
	for i, want := range wantCompleted {
		if completed[i] != want {
			t.Errorf("completed[%d] = %v, want %v (carry-forward over t1)",
				i, completed[i], want)
		}
	}
}

// TestBuildThroughputChart_BothEmpty pins the honest empty-state: with
// neither counter present the chart is flagged Empty so the template
// renders "no data yet" instead of a garbled overlay.
func TestBuildThroughputChart_BothEmpty(t *testing.T) {
	src := newFakeMetricsSource()
	chart := buildThroughputChart(src)
	if !chart.Empty {
		t.Error("chart must be Empty when no counters are present")
	}
	if len(chart.XAxis) != 0 {
		t.Errorf("empty chart must have no XAxis, got %v", chart.XAxis)
	}
}

// TestMetricsJSDisablesUplotDOMLegend is a source-level regression that
// metrics.js asks uPlot NOT to build its own DOM legend (the stacked
// "Time / Completed / Failed" overlay bug); the template renders an
// accessible static legend instead.
func TestMetricsJSDisablesUplotDOMLegend(t *testing.T) {
	js := readEmbeddedAsset(t, "assets/sources/metrics.js")
	if !strings.Contains(js, "legend: { show: false }") {
		t.Error("metrics.js must set legend: { show: false } to suppress uPlot DOM legend")
	}
	if strings.Contains(js, "legend: { live: true }") {
		t.Error("metrics.js must not keep the uPlot live DOM legend")
	}
}

// TestAppCSSProvidesUplotLayout guards the structural uPlot layout CSS
// (uPlot ships none here) so the canvas/axes position correctly.
func TestAppCSSProvidesUplotLayout(t *testing.T) {
	css := readEmbeddedAsset(t, "assets/app.css")
	if !strings.Contains(css, ".u-over") {
		t.Error("app.css must provide uPlot .u-over layout rule")
	}
	if !strings.Contains(css, ".u-wrap") {
		t.Error("app.css must provide uPlot .u-wrap layout rule")
	}
}

// readEmbeddedAsset reads a file out of the console embed.FS and returns
// its bytes as a string. Errors out the test on a missing asset.
func readEmbeddedAsset(t *testing.T, path string) string {
	t.Helper()
	b, err := assetsFS.ReadFile(path)
	if err != nil {
		t.Fatalf("read embedded asset %q: %v", path, err)
	}
	return string(b)
}
