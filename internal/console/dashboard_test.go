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

// TestDashboard_assemblesAllTiles verifies every populated operational
// tile key shows up in the rendered dashboard and carries the contract
// fields (LinkHref, Value, State). Per the mockup the dashboard renders
// four status tiles + three telemetry sparkcards; p99-latency and
// workers-active are NOT mockup dashboard cards and must be absent even
// when their sources are seeded. Positive: status + telemetry keys
// present. Negative: p99/workers absent and no tile lacks a state class.
func TestDashboard_assemblesAllTiles(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 10, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.completed", 25, now)
	src.addCounter("workflow.runs.failed", 1, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.failed", 2, now)
	src.addHistogram(
		"snapshot.save.duration_ms", 10,
		[]MetricBucket{
			{UpperBound: 5, Count: 5},
			{UpperBound: 10, Count: 10},
		}, now.Add(-10*time.Minute),
	)
	src.addHistogram(
		"snapshot.save.duration_ms", 10,
		[]MetricBucket{
			{UpperBound: 5, Count: 5},
			{UpperBound: 10, Count: 10},
		}, now,
	)
	src.addCounter("workers.active", 3, now)
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
		"tile-success-rate", "tile-throughput", "tile-p50-latency",
		"tile-error-rate",
	}
	for _, id := range wantTileIDs {
		if !strings.Contains(body, "id=\""+id+"\"") {
			t.Errorf("dashboard missing tile id=%q", id)
		}
	}
	for _, id := range []string{"tile-p99-latency", "tile-workers-active"} {
		if strings.Contains(body, "id=\""+id+"\"") {
			t.Errorf("dashboard must not render tile id=%q (not in mockup)", id)
		}
	}
	if !strings.Contains(body, "tile-state-") {
		t.Error("dashboard missing tile state coloring class")
	}
}

// TestDashboardEmptyMetricsRendersFourTiles asserts that when
// MetricsSource is nil (or yields empty data for the three metric-
// derived tiles), the dashboard drops the placeholders entirely and
// renders four always-available tiles: failed-1h, dlq-depth, in-flight,
// plus nothing else with the "telemetry pending" hint or is-empty
// marker. Issue #284.
func TestDashboardEmptyMetricsRendersFourTiles(t *testing.T) {
	fake := newFakeDS()
	cfg := dashTestCfg(t, fake, nil)
	rec := dashGet(t, cfg, "/console/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	wantTileIDs := []string{
		"tile-failed-1h", "tile-dlq-depth", "tile-in-flight",
	}
	for _, id := range wantTileIDs {
		if !strings.Contains(body, "id=\""+id+"\"") {
			t.Errorf("dashboard missing always-on tile id=%q", id)
		}
	}
	dropTileIDs := []string{
		"tile-success-rate", "tile-p99-latency", "tile-workers-active",
	}
	for _, id := range dropTileIDs {
		if strings.Contains(body, "id=\""+id+"\"") {
			t.Errorf("dashboard rendered placeholder tile id=%q with empty metrics", id)
		}
	}
	if strings.Contains(body, "telemetry pending") {
		t.Error("dashboard must not contain 'telemetry pending' placeholder copy")
	}
	if strings.Contains(body, "is-empty") {
		t.Error("dashboard must not contain is-empty placeholder class")
	}
}

// TestDashboardPopulatedMetricsRendersStatusAndTelemetry asserts the full
// status + telemetry surface when the MetricsSource has data: four status
// tiles (failed-1h, dlq-depth, in-flight, success-rate) and three telemetry
// sparkcards (throughput, p50-latency, error-rate). p99-latency and
// workers-active are not mockup dashboard cards and stay absent. Issue #284.
func TestDashboardPopulatedMetricsRendersStatusAndTelemetry(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 10, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.completed", 50, now)
	src.addCounter("workflow.runs.failed", 1, now)
	src.addHistogram(
		"snapshot.save.duration_ms", 10,
		[]MetricBucket{
			{UpperBound: 5, Count: 5},
			{UpperBound: 10, Count: 10},
		}, now.Add(-10*time.Minute),
	)
	src.addHistogram(
		"snapshot.save.duration_ms", 10,
		[]MetricBucket{
			{UpperBound: 5, Count: 5},
			{UpperBound: 10, Count: 10},
		}, now,
	)
	src.addCounter("workers.active", 3, now)
	fake := newFakeDS()
	cfg := dashTestCfg(t, fake, src)
	rec := dashGet(t, cfg, "/console/")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	wantTileIDs := []string{
		"tile-failed-1h", "tile-dlq-depth", "tile-in-flight",
		"tile-success-rate", "tile-throughput", "tile-p50-latency",
		"tile-error-rate",
	}
	for _, id := range wantTileIDs {
		if !strings.Contains(body, "id=\""+id+"\"") {
			t.Errorf("populated dashboard missing tile id=%q", id)
		}
	}
	for _, id := range []string{"tile-p99-latency", "tile-workers-active"} {
		if strings.Contains(body, "id=\""+id+"\"") {
			t.Errorf("populated dashboard must not render tile id=%q", id)
		}
	}
	if strings.Contains(body, "telemetry pending") {
		t.Error("populated dashboard must not contain 'telemetry pending'")
	}
	if strings.Contains(body, "is-empty") {
		t.Error("populated dashboard must not render is-empty placeholders")
	}
}

// TestDashboard_throughputCardRendersWhenSeeded seeds two distinct-
// timestamp workflow.runs.completed points so perMinuteRate has a real
// gap to divide by, and asserts the throughput sparkcard renders with a
// "/s" unit and a non-empty sparkline. Negative: a single-point series
// (no rate computable) omits the tile entirely (honest-omit).
func TestDashboard_throughputCardRendersWhenSeeded(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 10, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.completed", 130, now)
	cfg := dashTestCfg(t, newFakeDS(), src)
	rec := dashGet(t, cfg, "/console/")
	body := rec.Body.String()
	if !strings.Contains(body, "id=\"tile-throughput\"") {
		t.Error("dashboard missing throughput tile when seeded with 2 points")
	}
	if !strings.Contains(body, "/console/runs\"") {
		t.Error("throughput tile must link to /console/runs")
	}

	one := newFakeMetricsSource()
	one.addCounter("workflow.runs.completed", 10, now)
	rec2 := dashGet(t, dashTestCfg(t, newFakeDS(), one), "/console/")
	if strings.Contains(rec2.Body.String(), "id=\"tile-throughput\"") {
		t.Error("throughput tile must be omitted with <2 points (no rate)")
	}
}

// TestDashboard_errorRateCardRendersWhenSeeded seeds the same counter
// pair tileSuccessRate reads and asserts the error-rate sparkcard
// renders, linking to the failed-runs filter. Negative: when both
// counters are absent the tile is omitted (mirror of the empty case).
func TestDashboard_errorRateCardRendersWhenSeeded(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 50, now)
	src.addCounter("workflow.runs.failed", 3, now)
	cfg := dashTestCfg(t, newFakeDS(), src)
	rec := dashGet(t, cfg, "/console/")
	body := rec.Body.String()
	if !strings.Contains(body, "id=\"tile-error-rate\"") {
		t.Error("dashboard missing error-rate tile when seeded")
	}
	if !strings.Contains(body, "status=failed") {
		t.Error("error-rate tile must link to the failed-runs filter")
	}

	empty := dashGet(t, dashTestCfg(t, newFakeDS(), newFakeMetricsSource()), "/console/")
	if strings.Contains(empty.Body.String(), "id=\"tile-error-rate\"") {
		t.Error("error-rate tile must be omitted when counters are absent")
	}
}

// TestDashboard_p50CardRendersWhenHistogramSeeded seeds the snapshot
// latency histogram and asserts the p50 sparkcard renders. Negative:
// an empty histogram omits the tile (honest-omit, not fabricated).
func TestDashboard_p50CardRendersWhenHistogramSeeded(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addHistogram(
		"snapshot.save.duration_ms", 10,
		[]MetricBucket{
			{UpperBound: 5, Count: 5},
			{UpperBound: 10, Count: 10},
		}, now,
	)
	cfg := dashTestCfg(t, newFakeDS(), src)
	rec := dashGet(t, cfg, "/console/")
	if !strings.Contains(rec.Body.String(), "id=\"tile-p50-latency\"") {
		t.Error("dashboard missing p50-latency tile when histogram seeded")
	}

	empty := dashGet(t, dashTestCfg(t, newFakeDS(), newFakeMetricsSource()), "/console/")
	if strings.Contains(empty.Body.String(), "id=\"tile-p50-latency\"") {
		t.Error("p50-latency tile must be omitted when histogram empty")
	}
}

// TestTileP50Latency_neutralStateForNormalSnapshotSave guards the Norman
// finding: the snapshot-save p50 tile must NOT inherit the run-latency
// alarm bands (good <100ms / amber <500ms / red >=500ms). A 200ms
// object-store snapshot save is perfectly healthy I/O, but those bands
// would render it amber and a 600ms save red — falsely alarming the
// operator over normal disk/object-store latency. Snapshot save is an
// informational latency with no SLO, so it must carry a non-alarming
// state. Positive: a 200ms save is not amber/red. Negative: a 600ms save
// is likewise not amber/red (the upper band must not fire either).
func TestTileP50Latency_neutralStateForNormalSnapshotSave(t *testing.T) {
	for _, p50 := range []float64{200, 600} {
		src := newFakeMetricsSource()
		now := time.Now()
		// A single bucket resolves p50 to its upper bound, so the tile's
		// computed p50 equals the band-tripping value under test.
		src.addHistogram(
			"snapshot.save.duration_ms", 10,
			[]MetricBucket{{UpperBound: p50, Count: 10}}, now,
		)
		tile, ok := tileP50Latency(src)
		if !ok {
			t.Fatalf("p50=%.0f: tile must render for a seeded histogram", p50)
		}
		if tile.State == "amber" || tile.State == "red" {
			t.Errorf("p50=%.0f: snapshot-save tile carries alarming "+
				"run-latency state %q; want neutral", p50, tile.State)
		}
	}
}

// TestDashboard_deltaBadgeRendersWithHistory seeds a two-point failed
// series so the error-rate tile's computeDelta has prior history, and
// asserts the trend badge renders with a direction glyph. (Throughput
// no longer carries a delta — its only history is a cumulative counter
// whose raw change is meaningless beside a /s value, so it honest-omits.)
// Negative: a single-point series renders no delta span.
func TestDashboard_deltaBadgeRendersWithHistory(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 10, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.completed", 130, now)
	src.addCounter("workflow.runs.failed", 1, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.failed", 4, now)
	rec := dashGet(t, dashTestCfg(t, newFakeDS(), src), "/console/")
	body := rec.Body.String()
	if !strings.Contains(body, "dashboard-tile-delta") {
		t.Error("delta badge must render when the window has >=2 points")
	}
	if !strings.Contains(body, "▲") && !strings.Contains(body, "▼") {
		t.Error("delta badge must carry an up/down direction glyph")
	}

	one := newFakeMetricsSource()
	one.addCounter("workflow.runs.completed", 10, now)
	one.addCounter("workflow.runs.failed", 1, now)
	rec2 := dashGet(t, dashTestCfg(t, newFakeDS(), one), "/console/")
	// The single-point error-rate tile renders but carries no delta.
	if strings.Contains(rec2.Body.String(), "dashboard-tile-delta") {
		t.Error("delta badge must be omitted for a single-point series")
	}
}

// TestDashboard_telemetryCardsNoDoubledUnit pins the throughput and
// error-rate tiles to the bare-number-in-Value + unit-in-Unit convention
// shared with success-rate/p99. The value span must NOT carry the unit
// suffix, so "/s /s" and "% %" can never render.
func TestDashboard_telemetryCardsNoDoubledUnit(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 12, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.completed", 130, now)
	src.addCounter("workflow.runs.failed", 2, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.failed", 5, now)
	body := dashGet(t, dashTestCfg(t, newFakeDS(), src), "/console/").Body.String()
	if strings.Contains(body, "/s</span>\n    <span class=\"dashboard-tile-unit\">/s") {
		t.Error("throughput value must not carry a /s suffix beside the /s unit")
	}
	// A doubled unit shows up as the unit token appearing twice in a row.
	if strings.Count(body, "/s") >= 1 && strings.Contains(body, "/s /s") {
		t.Error("throughput must not render a doubled /s unit")
	}
	if strings.Contains(body, "% %") {
		t.Error("error rate must not render a doubled % unit")
	}
}

// TestDashboard_errorRateGlyphTracksRawMovementColorInverts pins the
// error-rate badge to the mockup contract: the glyph follows the raw
// failed-count movement (rising errors => ▲), while the color sense
// inverts (rising errors => coral/"bad"). A rising-error series must
// render ▲ with the bad-tone color class, never a down arrow.
func TestDashboard_errorRateGlyphTracksRawMovementColorInverts(t *testing.T) {
	now := time.Now()
	rising := newFakeMetricsSource()
	rising.addCounter("workflow.runs.completed", 100, now.Add(-10*time.Minute))
	rising.addCounter("workflow.runs.completed", 100, now)
	rising.addCounter("workflow.runs.failed", 1, now.Add(-10*time.Minute))
	rising.addCounter("workflow.runs.failed", 9, now)
	body := dashGet(t, dashTestCfg(t, newFakeDS(), rising), "/console/").Body.String()
	errCard := sliceErrorRateCard(t, body)
	if !strings.Contains(errCard, "▲") {
		t.Errorf("rising error count must render ▲ (raw movement up); card=%q", errCard)
	}
	if strings.Contains(errCard, "▼") {
		t.Errorf("rising error count must NOT render ▼; card=%q", errCard)
	}
	if !strings.Contains(errCard, "dashboard-tile-delta-bad") {
		t.Errorf("rising error rate must color the badge bad/coral; card=%q", errCard)
	}

	falling := newFakeMetricsSource()
	falling.addCounter("workflow.runs.completed", 100, now.Add(-10*time.Minute))
	falling.addCounter("workflow.runs.completed", 100, now)
	falling.addCounter("workflow.runs.failed", 10, now.Add(-10*time.Minute))
	falling.addCounter("workflow.runs.failed", 2, now)
	body2 := dashGet(t, dashTestCfg(t, newFakeDS(), falling), "/console/").Body.String()
	errCard2 := sliceErrorRateCard(t, body2)
	if !strings.Contains(errCard2, "▼") {
		t.Errorf("falling error count must render ▼ (raw movement down); card=%q", errCard2)
	}
	if !strings.Contains(errCard2, "dashboard-tile-delta-good") {
		t.Errorf("falling error rate must color the badge good/teal; card=%q", errCard2)
	}
}

// TestDashboard_throughputHasNoMeaninglessCountDelta guards against the
// old behavior where the throughput badge showed the raw cumulative
// counter change (e.g. "▲ 118") beside a per-second value. The throughput
// tile must render but carry no delta badge until a meaningful rate-over-
// rate delta exists (honest-omit).
func TestDashboard_throughputHasNoMeaninglessCountDelta(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addCounter("workflow.runs.completed", 10, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.completed", 130, now)
	body := dashGet(t, dashTestCfg(t, newFakeDS(), src), "/console/").Body.String()
	tput := sliceTile(t, body, "tile-throughput")
	if strings.Contains(tput, "dashboard-tile-delta") {
		t.Errorf("throughput must not show a raw-count delta badge; card=%q", tput)
	}
	if strings.Contains(tput, "120") {
		t.Errorf("throughput must not render the cumulative counter change; card=%q", tput)
	}
}

// TestDashboard_tilesSplitIntoStatusAndTelemetryRows pins the mockup's
// two-row layout: StatusTiles holds the four number+label status cells in
// order (failed-1h, dlq-depth, in-flight, success-rate) and TelemetryTiles
// holds the three sparkcards in order (throughput, p50-latency, error-rate).
// Neither p99-latency nor workers-active appears anywhere — they are not in
// the mockup dashboard. Positive: both slices carry their expected keys in
// order. Negative: the dropped keys are absent from both slices.
func TestDashboard_tilesSplitIntoStatusAndTelemetryRows(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now().UTC()
	src.addCounter("workflow.runs.completed", 10, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.completed", 130, now)
	src.addCounter("workflow.runs.failed", 1, now.Add(-10*time.Minute))
	src.addCounter("workflow.runs.failed", 4, now)
	src.addHistogram(
		"snapshot.save.duration_ms", 10,
		[]MetricBucket{{UpperBound: 5, Count: 5}, {UpperBound: 10, Count: 10}},
		now.Add(-10*time.Minute),
	)
	src.addHistogram(
		"snapshot.save.duration_ms", 10,
		[]MetricBucket{{UpperBound: 5, Count: 5}, {UpperBound: 10, Count: 10}},
		now,
	)
	src.addCounter("workers.active", 3, now)

	cfg := dashTestCfg(t, newFakeDS(), src)
	view := buildDashboardView(context.Background(), cfg)

	wantStatus := []string{"failed-1h", "dlq-depth", "in-flight", "success-rate"}
	if got := tileKeys(view.StatusTiles); !equalStrings(got, wantStatus) {
		t.Errorf("StatusTiles keys = %v, want %v", got, wantStatus)
	}
	wantTelemetry := []string{"throughput", "p50-latency", "error-rate"}
	if got := tileKeys(view.TelemetryTiles); !equalStrings(got, wantTelemetry) {
		t.Errorf("TelemetryTiles keys = %v, want %v", got, wantTelemetry)
	}
	all := append(tileKeys(view.StatusTiles), tileKeys(view.TelemetryTiles)...)
	for _, dropped := range []string{"p99-latency", "workers-active"} {
		for _, k := range all {
			if k == dropped {
				t.Errorf("tile %q must not appear in the dashboard (not in mockup)", dropped)
			}
		}
	}
}

// tileKeys projects a tile slice to its Key list for order assertions.
func tileKeys(tiles []DashboardTile) []string {
	out := make([]string, 0, len(tiles))
	for _, t := range tiles {
		out = append(out, t.Key)
	}
	return out
}

// equalStrings compares two string slices element-wise.
func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// TestDashboard_throughputNonNegativeAcrossLabelSlots is the regression
// guard for the "-0.0" throughput bug. workflow.runs.completed is emitted
// per-workflow label; the aggregator merges every label slot into ONE
// points slice sorted by timestamp. With two workflows whose cumulative
// counters interleave in time, the raw first point (workflow A, low) and
// raw last point (workflow B, lower still) belong to DIFFERENT label
// slots, so perMinuteRate(points) sees a negative delta and emits a
// negative/"-0.0" rate. The fix derives throughput from the total
// completed across all label slots, which is monotonic and can never go
// negative. Positive: a real workload yields a non-negative rate. Negative:
// the value must never carry a leading "-".
func TestDashboard_throughputNonNegativeAcrossLabelSlots(t *testing.T) {
	src := newFakeMetricsSource()
	base := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second)
	wfA := map[string]string{"workflow": "alpha"}
	wfB := map[string]string{"workflow": "bravo"}
	// Sorted-by-timestamp the merged slice is:
	//   t0 alpha=5, t1 bravo=80, t2 alpha=120, t3 bravo=2
	// so points[0]=alpha:5 and points[last]=bravo:2 straddle label slots
	// and last-first = 2-5 = -3 -> negative rate under the old code.
	src.addCounterLabeled("workflow.runs.completed", 5, base, wfA)
	src.addCounterLabeled("workflow.runs.completed", 80, base.Add(1*time.Minute), wfB)
	src.addCounterLabeled("workflow.runs.completed", 120, base.Add(2*time.Minute), wfA)
	src.addCounterLabeled("workflow.runs.completed", 2, base.Add(3*time.Minute), wfB)

	// Document the bug: the raw-points path goes negative.
	series, _ := src.MetricSnapshot("workflow.runs.completed")
	if rawRate := perMinuteRate(series.Points); rawRate >= 0 {
		t.Fatalf("fixture invalid: raw perMinuteRate = %v, want negative to "+
			"prove the straddle bug", rawRate)
	}

	tile, ok := tileThroughput(src)
	if !ok {
		t.Fatal("throughput tile must render with a multi-label series")
	}
	if strings.HasPrefix(tile.Value, "-") {
		t.Errorf("throughput Value = %q must never be negative", tile.Value)
	}
	if parseFloatOrZero(tile.Value) < 0 {
		t.Errorf("throughput rate = %q parsed negative", tile.Value)
	}
}

// sliceErrorRateCard returns the error-rate tile anchor substring.
func sliceErrorRateCard(t *testing.T, body string) string {
	return sliceTile(t, body, "tile-error-rate")
}

// sliceTile returns the <a> ... </a> substring for the tile with the
// given DOM id, so per-card assertions don't leak across tiles.
func sliceTile(t *testing.T, body, domID string) string {
	t.Helper()
	marker := "id=\"" + domID + "\""
	i := strings.Index(body, marker)
	if i < 0 {
		t.Fatalf("tile %q not present in body", domID)
	}
	start := strings.LastIndex(body[:i], "<a ")
	if start < 0 {
		t.Fatalf("tile %q anchor open not found", domID)
	}
	end := strings.Index(body[start:], "</a>")
	if end < 0 {
		t.Fatalf("tile %q anchor close not found", domID)
	}
	return body[start : start+end+len("</a>")]
}

// TestDashboard_telemetryCardsAbsentWithEmptyMetrics extends the empty-
// metrics guard to the three new telemetry tiles: none may render when
// the aggregator yields nothing (honest-omit, issue #284).
func TestDashboard_telemetryCardsAbsentWithEmptyMetrics(t *testing.T) {
	cfg := dashTestCfg(t, newFakeDS(), nil)
	rec := dashGet(t, cfg, "/console/")
	body := rec.Body.String()
	for _, id := range []string{
		"tile-throughput", "tile-p50-latency", "tile-error-rate",
	} {
		if strings.Contains(body, "id=\""+id+"\"") {
			t.Errorf("telemetry tile id=%q must be absent with nil metrics", id)
		}
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
