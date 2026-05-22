package console

import (
	"context"
	"strconv"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// dashboard.go owns the Phase 2 dashboard view assembly. The legacy
// "System overview" + "Heartbeat" tiles were a config skeleton — they
// didn't answer the operator's #1 landing-page question, "is anything
// on fire?". This file replaces them with up-to-six live operational
// tiles (Failed-1h, DLQ depth, In-flight, plus Success rate / p99
// latency / Workers active when MetricsSource has data for them) plus
// two recent panels (failures, operator actions). Tiles link to
// filtered drill-down pages; SSE patching keeps them current.
//
// Data assembly is best-effort: the always-on counters (failed-1h,
// DLQ depth, in-flight) come from the data source and always render;
// metric-derived tiles only render when MetricsSource yields data for
// them. Issue #284 dropped the previous "telemetry pending" placeholder
// path — empty tiles are now omitted entirely so the grid never shows
// a row of broken-looking muted-dot cards next to working ones.

// DashboardView is the binding the rebuilt dashboard.html template
// consumes. Tiles contains the three always-on counters plus any of
// the three metric-derived tiles whose source has data; ordering is
// deterministic so the CSS grid layout stays stable across re-renders.
// RecentFailures and RecentActions hold the last few entries for the
// side-by-side panels below the tile grid.
type DashboardView struct {
	Tiles            []DashboardTile
	RecentFailures   []RecentFailureRow
	RecentActions    []RecentActionRow
	Overview         overviewData
	Actor            Actor
	MetricsAvailable bool
}

// DashboardTile is one operational tile on the at-a-glance grid. Key
// is the stable identifier (used both for the DOM id and the SSE
// selector); State drives the threshold-based coloring class
// ("good" / "amber" / "red"). Spark is a 24-hour activity series; Hint
// carries the small explanation text below the value.
type DashboardTile struct {
	Key       string
	Title     string
	Value     string
	Unit      string
	State     string
	Spark     []float64
	LinkHref  string
	Hint      string
	Delta     string
	UpdatedAt time.Time
}

// DOMID returns the canonical "tile-<key>" id used by SSE patches.
// Kept as a method so the template never has to string-concat.
func (t DashboardTile) DOMID() string {
	return "tile-" + t.Key
}

// RecentFailureRow is one row in the "Recent failures" panel. ErrorMsg
// is the last failed step's error string — empty when no step error
// survived the run snapshot (the operator still gets the workflow +
// run id pair).
type RecentFailureRow struct {
	When       string
	WorkflowID string
	RunIDShort string
	RunID      string
	ErrorMsg   string
}

// RecentActionRow is one row in the "Recent operator actions" panel.
// Time renders as RFC3339; the template formats it relative for the
// reader. Actor + Action + Target match the AuditEvent fields.
type RecentActionRow struct {
	When   string
	Actor  string
	Action string
	Target string
}

// buildDashboardView assembles the full DashboardView from the
// available data + metrics sources. Errors are logged + swallowed —
// the dashboard always renders, never 500s. Bounded loops everywhere.
func buildDashboardView(
	ctx context.Context, cfg Config,
) DashboardView {
	if ctx == nil {
		panic("buildDashboardView: ctx is nil")
	}
	view := DashboardView{
		MetricsAvailable: cfg.Metrics != nil,
		Overview: overviewData{
			Listener: cfg.HTTPAddr,
			AuthMode: cfg.AuthMode.String(),
			Build:    cfg.Build,
		},
	}
	counters := readDashboardCounters(ctx, cfg)
	view.Tiles = assembleDashboardTiles(cfg.Metrics, counters)
	view.RecentFailures = readRecentFailures(ctx, cfg.Data, recentFailuresMax)
	view.RecentActions = readRecentActions(ctx, cfg.Data, recentActionsMax)
	return view
}

// dashboardCounters carries the non-metric state the data source can
// report directly (in-flight run count, DLQ depth). Both are bounded
// integers so the rendered tile is deterministic on cold start.
type dashboardCounters struct {
	InFlightCount int
	DLQDepth      int
	FailedLastHr  int
}

// recentFailuresMax caps the number of rows in the recent-failures
// panel. Five is enough to fit the side-by-side card without scrolling
// on a 1080p screen; more than that and the panel competes with the
// tile grid for attention.
const recentFailuresMax = 5

// recentActionsMax caps the operator-actions panel. Three rows fits
// the matching card height. Audit traffic is sparse; recent-three
// captures any operator session.
const recentActionsMax = 3

// readDashboardCounters pulls the non-metric tile inputs from the
// data source. Each read is short-bounded so a slow source doesn't
// stall the dashboard render. Errors collapse to zero — the tile then
// shows "0" with the "good" state, which is the right zero-state.
func readDashboardCounters(
	ctx context.Context, cfg Config,
) dashboardCounters {
	if ctx == nil {
		panic("readDashboardCounters: ctx is nil")
	}
	var c dashboardCounters
	if cfg.Data == nil {
		return c
	}
	rctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	runs, err := cfg.Data.ListRuns(rctx, "")
	if err == nil {
		c.InFlightCount, c.FailedLastHr = countRunsForTiles(runs, time.Now())
	}
	dlq, err := cfg.Data.ListDeadLetters(rctx, 200)
	if err == nil {
		c.DLQDepth = len(dlq)
	}
	return c
}

// countRunsForTiles walks the run list once and returns (in-flight,
// failed-in-last-hour). Bounded by len(runs); the api.Service caps
// returned runs at a fixed page size, so the loop is finite.
func countRunsForTiles(
	runs []dag.WorkflowRun, now time.Time,
) (int, int) {
	const cutoffDur = time.Hour
	cutoff := now.Add(-cutoffDur)
	inFlight, failed := 0, 0
	for _, r := range runs {
		if r.Status == dag.RunStatusRunning {
			inFlight++
		}
		if r.Status == dag.RunStatusFailed && r.CreatedAt.After(cutoff) {
			failed++
		}
	}
	return inFlight, failed
}

// readRecentFailures pulls up to limit recent failed runs from the
// data source and projects them into the panel-row shape. Bounded
// loop on limit. The list is already CreatedAt-descending by the
// underlying api.Service.
func readRecentFailures(
	ctx context.Context, ds DataSource, limit int,
) []RecentFailureRow {
	if limit <= 0 {
		panic("readRecentFailures: limit must be positive")
	}
	if ds == nil {
		return nil
	}
	rctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	runs, err := ds.ListRuns(rctx, "")
	if err != nil {
		return nil
	}
	out := make([]RecentFailureRow, 0, limit)
	for _, r := range runs {
		if r.Status != dag.RunStatusFailed {
			continue
		}
		out = append(out, projectFailureRow(r))
		if len(out) >= limit {
			break
		}
	}
	return out
}

// projectFailureRow extracts the per-row fields from a failed run.
// Last-error pick mirrors runOutputAndError in pages.go — most recent
// step error wins.
func projectFailureRow(r dag.WorkflowRun) RecentFailureRow {
	row := RecentFailureRow{
		When:       r.CreatedAt.UTC().Format(time.RFC3339),
		WorkflowID: r.WorkflowID,
		RunID:      r.RunID,
		RunIDShort: shortRunID(r.RunID),
	}
	for _, s := range r.Steps {
		if s.Status == dag.StepStatusFailed && s.Error != "" {
			row.ErrorMsg = s.Error
		}
	}
	return row
}

// readRecentActions pulls up to limit recent audit events and projects
// them into the panel-row shape. Honest empty state on a nil-source.
func readRecentActions(
	ctx context.Context, ds DataSource, limit int,
) []RecentActionRow {
	if limit <= 0 {
		panic("readRecentActions: limit must be positive")
	}
	if ds == nil {
		return nil
	}
	rctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	events, err := ds.ListAuditEvents(rctx, limit)
	if err != nil {
		return nil
	}
	out := make([]RecentActionRow, 0, len(events))
	for _, evt := range events {
		out = append(out, RecentActionRow{
			When:   evt.Time.UTC().Format(time.RFC3339),
			Actor:  evt.Actor,
			Action: evt.Action,
			Target: evt.Target,
		})
	}
	return out
}

// assembleDashboardTiles produces the tile list in display order. The
// first three tiles always render (they're driven by the data source,
// not the metrics aggregator). The remaining three render only when
// their metric has data — empty placeholders are dropped entirely
// rather than shown as muted-dot "telemetry pending" cards next to
// working tiles (issue #284).
func assembleDashboardTiles(
	src MetricsSource, c dashboardCounters,
) []DashboardTile {
	const maxTileCount = 6
	tiles := make([]DashboardTile, 0, maxTileCount)
	tiles = append(tiles,
		tileFailedLastHour(src, c.FailedLastHr),
		tileDLQDepth(c.DLQDepth, dlqSparkFromSource(src)),
		tileInFlight(c.InFlightCount),
	)
	if t, ok := tileSuccessRate(src); ok {
		tiles = append(tiles, t)
	}
	if t, ok := tileP99Latency(src); ok {
		tiles = append(tiles, t)
	}
	if t, ok := tileWorkersActive(src); ok {
		tiles = append(tiles, t)
	}
	return tiles
}

// dlqSparkFromSource synthesises a DLQ sparkline from the metrics
// aggregator's runs.failed counter — failed runs are the dominant
// driver of DLQ growth, so we reuse that history when the dedicated
// dlq.depth metric isn't yet emitted by the engine.
func dlqSparkFromSource(src MetricsSource) []float64 {
	if src == nil {
		return nil
	}
	series, ok := src.MetricSnapshot("workflow.runs.failed")
	if !ok || len(series.Points) == 0 {
		return nil
	}
	return sparkFromPoints(series.Points, 24)
}

// tileFailedLastHour builds the Failed-1h tile. State coloring
// follows the audit's thresholds: 0 = good, 1-4 = amber, 5+ = red.
func tileFailedLastHour(src MetricsSource, count int) DashboardTile {
	t := DashboardTile{
		Key: "failed-1h", Title: "Failed runs (1h)", Unit: "runs",
		LinkHref: "/console/runs?status=failed&range=1h",
		Value:    strconv.Itoa(count),
		State:    failedTileState(count),
		Hint:     "click to drill",
	}
	if src != nil {
		if series, ok := src.MetricSnapshot("workflow.runs.failed"); ok {
			t.Spark = sparkFromPoints(series.Points, 24)
		}
	}
	return t
}

// failedTileState classifies a failed-count into a state color band.
func failedTileState(count int) string {
	if count == 0 {
		return "good"
	}
	if count < 5 {
		return "amber"
	}
	return "red"
}

// tileDLQDepth builds the DLQ depth tile. Coloring follows: 0 = good,
// 1-9 = amber, 10+ = red. Even one DLQ entry warrants operator
// attention so we don't ship green-on-1.
func tileDLQDepth(depth int, spark []float64) DashboardTile {
	return DashboardTile{
		Key: "dlq-depth", Title: "DLQ depth", Unit: "entries",
		LinkHref: "/console/dlq",
		Value:    strconv.Itoa(depth),
		State:    dlqTileState(depth),
		Spark:    spark,
		Hint:     "click to drill",
	}
}

func dlqTileState(depth int) string {
	if depth == 0 {
		return "good"
	}
	if depth < 10 {
		return "amber"
	}
	return "red"
}

// tileInFlight builds the In-flight runs tile. Always green — running
// is the happy path. Sparkline omitted; the value alone is meaningful.
func tileInFlight(count int) DashboardTile {
	return DashboardTile{
		Key: "in-flight", Title: "In-flight runs", Unit: "runs",
		LinkHref: "/console/runs?status=running",
		Value:    strconv.Itoa(count),
		State:    "good",
		Hint:     "click to drill",
	}
}

// tileSuccessRate reuses the existing tileFromSuccessRate builder but
// rekeys it for the dashboard surface. Second return is false when the
// metrics aggregator hasn't seen any runs yet — caller drops the tile
// rather than rendering a placeholder (issue #284).
func tileSuccessRate(src MetricsSource) (DashboardTile, bool) {
	if src == nil {
		return DashboardTile{}, false
	}
	inner := tileFromSuccessRate(src)
	if inner.Empty {
		return DashboardTile{}, false
	}
	pct := parseFloatOrZero(inner.Value)
	t := DashboardTile{
		Key: "success-rate", Title: "Success rate (1h)", Unit: "%",
		LinkHref: "/console/ops/metrics",
		Value:    inner.Value,
		Spark:    inner.Spark,
		State:    successRateState(pct),
	}
	return t, true
}

// successRateState classifies the percentage value into a state band.
// ≥99 = good (engine is healthy), 95–98 = amber, <95 = red.
func successRateState(pct float64) string {
	if pct >= 99 {
		return "good"
	}
	if pct >= 95 {
		return "amber"
	}
	return "red"
}

// tileP99Latency reads the engine snapshot histogram and reports the
// p99 latency. Second return is false when the histogram isn't yet
// populated — caller drops the tile rather than rendering a "telemetry
// pending" placeholder (issue #284).
func tileP99Latency(src MetricsSource) (DashboardTile, bool) {
	if src == nil {
		return DashboardTile{}, false
	}
	series, ok := src.MetricSnapshot("snapshot.save.duration_ms")
	if !ok || len(series.Points) == 0 {
		return DashboardTile{}, false
	}
	latest := series.Latest()
	if latest.Count == 0 || len(latest.Buckets) == 0 {
		return DashboardTile{}, false
	}
	p99 := percentileFromBuckets(latest, 0.99)
	t := DashboardTile{
		Key: "p99-latency", Title: "p99 snapshot latency", Unit: "ms",
		LinkHref: "/console/ops/metrics",
		Value:    formatNumber(p99),
		Spark:    sparkFromHistogramP50(series.Points, 24),
		State:    latencyTileState(p99),
	}
	return t, true
}

// latencyTileState classifies the p99 latency into a band. The
// thresholds are coarse on purpose: anything under 100ms is healthy,
// up to 500ms is amber, beyond is red. Operators tune this later when
// the engine emits per-step labels and we know the real distribution.
func latencyTileState(p99 float64) string {
	if p99 < 100 {
		return "good"
	}
	if p99 < 500 {
		return "amber"
	}
	return "red"
}

// tileWorkersActive reads the workers.active gauge. Second return is
// false when the engine hasn't yet emitted the metric — caller drops
// the tile rather than rendering a "telemetry pending" placeholder
// (issue #284). Showing 0 here would be a false alarm ("no workers!")
// when the real cause is just a missing telemetry source.
func tileWorkersActive(src MetricsSource) (DashboardTile, bool) {
	if src == nil {
		return DashboardTile{}, false
	}
	series, ok := src.MetricSnapshot("workers.active")
	if !ok || len(series.Points) == 0 {
		return DashboardTile{}, false
	}
	latest := series.Latest()
	t := DashboardTile{
		Key: "workers-active", Title: "Workers active", Unit: "",
		LinkHref: "/console/ops/workers",
		Value:    formatNumber(latest.Value),
		State:    "good",
	}
	return t, true
}

// parseFloatOrZero is a tiny helper for percentage tile state coloring.
// formatNumber emits "92" or "92.5"; both parse cleanly to float64.
func parseFloatOrZero(s string) float64 {
	if s == "" || s == "—" {
		return 0
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return v
}
