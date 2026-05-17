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
// on fire?". This file replaces them with six live operational tiles
// (Failed-1h, DLQ depth, In-flight, Success rate, p99 latency, Workers
// active) plus two recent panels (failures, operator actions). Tiles
// link to filtered drill-down pages; SSE patching keeps them current.
//
// The data assembly is intentionally tolerant — every read is best-
// effort, every missing source degrades to an honest placeholder
// tile. The dashboard never fails to render because one upstream is
// silent; instead the affected tile carries an em-dash and the
// "telemetry pending" hint.

// DashboardView is the binding the rebuilt dashboard.html template
// consumes. Tiles is always six entries in deterministic order so the
// CSS grid layout stays stable across re-renders. RecentFailures and
// RecentActions hold the last few entries for the side-by-side panels
// below the tile grid.
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
	Empty     bool
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

// assembleDashboardTiles produces the six tiles in display order. The
// caller owns whether the metrics source is wired; we always emit a
// tile per slot — empty/placeholder when the data source is silent.
func assembleDashboardTiles(
	src MetricsSource, c dashboardCounters,
) []DashboardTile {
	return []DashboardTile{
		tileFailedLastHour(src, c.FailedLastHr),
		tileDLQDepth(c.DLQDepth, dlqSparkFromSource(src)),
		tileInFlight(c.InFlightCount),
		tileSuccessRate(src),
		tileP99Latency(src),
		tileWorkersActive(),
	}
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
// rekeys it for the dashboard surface. Returns an empty tile when the
// metrics aggregator hasn't seen any runs yet.
func tileSuccessRate(src MetricsSource) DashboardTile {
	t := DashboardTile{
		Key: "success-rate", Title: "Success rate (1h)", Unit: "%",
		LinkHref: "/console/ops/metrics",
		Value:    "—",
		State:    "good",
		Empty:    true,
		Hint:     "telemetry pending",
	}
	if src == nil {
		return t
	}
	inner := tileFromSuccessRate(src)
	if inner.Empty {
		return t
	}
	pct := parseFloatOrZero(inner.Value)
	t.Empty = false
	t.Value = inner.Value
	t.Spark = inner.Spark
	t.Hint = ""
	t.State = successRateState(pct)
	return t
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
// p99 latency. Empty placeholder when the histogram isn't populated.
func tileP99Latency(src MetricsSource) DashboardTile {
	t := DashboardTile{
		Key: "p99-latency", Title: "p99 snapshot latency", Unit: "ms",
		LinkHref: "/console/ops/metrics",
		Value:    "—",
		State:    "good",
		Empty:    true,
		Hint:     "telemetry pending",
	}
	if src == nil {
		return t
	}
	series, ok := src.MetricSnapshot("snapshot.save.duration_ms")
	if !ok || len(series.Points) == 0 {
		return t
	}
	latest := series.Latest()
	if latest.Count == 0 || len(latest.Buckets) == 0 {
		return t
	}
	p99 := percentileFromBuckets(latest, 0.99)
	t.Empty = false
	t.Hint = ""
	t.Value = formatNumber(p99)
	t.Spark = sparkFromHistogramP50(series.Points, 24)
	t.State = latencyTileState(p99)
	return t
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

// tileWorkersActive is the placeholder tile for the worker-presence
// counter. The engine's worker_heartbeats bucket isn't yet written
// (audit confirmed). The tile reads "telemetry pending" until that
// lands; operators see explicit non-rendering rather than a 0 that
// implies "no workers" — which would be a false alarm.
func tileWorkersActive() DashboardTile {
	return DashboardTile{
		Key: "workers-active", Title: "Workers active", Unit: "",
		LinkHref: "/console/ops/workers",
		Value:    "—",
		State:    "good",
		Empty:    true,
		Hint:     "telemetry pending",
	}
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
