package console

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strings"
	"time"
)

// metrics_page.go owns the /console/ops/metrics page: the in-console
// metrics dashboard with system-health tiles, throughput / latency
// charts, and a per-workflow breakdown. Live updates run via
// /console/sse/metrics; this file holds only the initial render path.
//
// The page reads from cfg.Metrics (a MetricsSource). Nil source
// renders an explicit empty state — operators see "metrics
// aggregator not wired" rather than a blank canvas.

// MetricsView is the binding the metrics_dashboard.html template
// consumes. Tiles + charts + table + drilldown each get their own
// struct so renderers can be unit-tested in isolation.
type MetricsView struct {
	Tiles     []MetricsTile
	Charts    []MetricsChart
	Workflows []WorkflowMetricsRow
	StepRows  []StepMetricsRow
	Workflow  string // currently drilled-down workflow, empty == no drill
	Available bool   // false when cfg.Metrics is nil
	// ErrorReason, when non-empty, names the failure that prevented
	// the aggregator from starting. The template renders an alert
	// banner that distinguishes "metrics aggregator down" (operator
	// must investigate) from "no aggregator wired" (intended
	// configuration).
	ErrorReason string
	Generated   time.Time
}

// MetricsTile is one tile on the system-health row. Spark is the JSON
// data payload the template injects into a µPlot init; Delta is a
// human-readable change-vs-1h-ago hint (e.g. "+12 %" or "-3"). Trend
// is "up"/"down"/"flat" for the arrow direction.
type MetricsTile struct {
	ID       string
	Title    string
	Value    string
	Unit     string
	Hint     string
	Delta    string
	Trend    string
	Spark    []float64
	Empty    bool
	Href     string
	MetricID string // canonical metric this tile reflects
}

// MetricsChart is one large chart on the page. Series is the slice of
// per-line data; XAxis is the shared timestamp axis as seconds-since
// epoch (µPlot's native shape). Anomalies are detected outliers the
// client renders as a points-only overlay series.
type MetricsChart struct {
	ID          string
	Title       string
	Description string
	Unit        string
	XAxis       []float64
	Series      []ChartSeries
	Empty       bool
	WindowLabel string
	// Anomalies carries the muted-rust point overlays the µPlot
	// renderer draws on top of the regular series. Empty for charts
	// that don't have a latency-shape definition. The tooltip text
	// lives on each marker so the client doesn't have to redo math.
	Anomalies []AnomalyMarker
	// AnomalyThresholdRatio is the p99/p50 ratio at which a marker
	// fires. Surfaced into the template so the glossary text stays
	// in sync with AnomalyP99OverP50Ratio — see metrics_anomaly.go.
	AnomalyThresholdRatio float64
	// WorkflowFilter narrows the anomaly-click navigation to a
	// specific workflow when the chart is rendered inside a
	// per-workflow drilldown. Empty for global charts (the default).
	WorkflowFilter string
}

// ChartSeries is one line/area in a MetricsChart.
type ChartSeries struct {
	Label  string
	Values []float64
	Stroke string // CSS color name; template resolves to var(--...).
}

// WorkflowMetricsRow is one row in the per-workflow breakdown table.
type WorkflowMetricsRow struct {
	Workflow        string
	Completed1h     uint64
	Failed1h        uint64
	P50LatencyMs    float64
	P95LatencyMs    float64
	ThroughputSpark []float64
	HasData         bool
}

// StepMetricsRow is one row in the per-step drilldown.
type StepMetricsRow struct {
	Step         string
	Completed1h  uint64
	Failed1h     uint64
	P50LatencyMs float64
	P95LatencyMs float64
}

// servePageMetrics renders /console/ops/metrics. Implements the same
// shape as servePageOpsIndex: build view, render, exit. Live updates
// arrive on /console/sse/metrics (registered separately).
func servePageMetrics(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("servePageMetrics: w is nil")
	}
	if r == nil {
		panic("servePageMetrics: r is nil")
	}
	q := r.URL.Query()
	workflow := strings.TrimSpace(q.Get("workflow"))
	view := buildMetricsView(r.Context(), cfg, workflow)
	pd := pageData{
		Title:   "Metrics",
		Section: "ops",
		Page:    view,
	}
	renderPage(w, r, ts, cfg, "metrics_dashboard", pd)
}

// buildMetricsView assembles a MetricsView from cfg.Metrics. Returns
// an Available=false view when cfg.Metrics is nil so the template
// renders an explicit "wire dagnats serve --metrics" hint.
func buildMetricsView(
	ctx context.Context, cfg Config, workflow string,
) MetricsView {
	if ctx == nil {
		panic("buildMetricsView: ctx is nil")
	}
	now := time.Now().UTC()
	if cfg.Metrics == nil {
		return MetricsView{
			Available:   false,
			ErrorReason: cfg.MetricsErrorReason,
			Generated:   now,
		}
	}
	tiles := buildMetricsTiles(cfg.Metrics)
	charts := buildMetricsCharts(cfg.Metrics)
	wfs := buildWorkflowRows(cfg.Metrics)
	steps := buildStepRows(cfg.Metrics, workflow)
	return MetricsView{
		Tiles:     tiles,
		Charts:    charts,
		Workflows: wfs,
		StepRows:  steps,
		Workflow:  workflow,
		Available: true,
		Generated: now,
	}
}

// buildMetricsTiles produces the four system-health tiles. Each tile
// has a deterministic order; an empty metric still renders a tile but
// marked Empty=true so the operator sees the gap.
func buildMetricsTiles(src MetricsSource) []MetricsTile {
	if src == nil {
		return nil
	}
	tiles := []MetricsTile{
		tileFromCounterRate(src,
			"workflow.runs.completed", "tile-runs-rate",
			"Runs / min", "runs", "/console/runs"),
		tileFromSuccessRate(src),
		tileFromHistogramP50(src,
			"snapshot.save.duration_ms", "tile-snapshot-p50",
			"Snapshot p50", "ms", "/console/ops/metrics"),
		tileFromCounter(src,
			"workflow.runs.failed", "tile-failed",
			"Runs failed", "runs", "/console/dlq"),
	}
	return tiles
}

// tileFromCounter builds a tile showing the latest value of a counter
// plus a 60min sparkline of its values over time.
func tileFromCounter(
	src MetricsSource, name, id, title, unit, href string,
) MetricsTile {
	series, ok := src.MetricSnapshot(name)
	tile := MetricsTile{
		ID: id, Title: title, Unit: unit, Href: href,
		MetricID: name,
	}
	if !ok || len(series.Points) == 0 {
		tile.Empty = true
		tile.Value = "—"
		return tile
	}
	latest := series.Latest()
	tile.Value = formatNumber(latest.Value)
	tile.Spark = sparkFromPoints(series.Points, 60)
	tile.Delta = "" // counters: delta calc shown in charts, not tile
	tile.Trend = "flat"
	return tile
}

// tileFromCounterRate normalises a counter into a per-minute rate
// over the last 60 minutes. Shows units of "runs/min" in the tile.
func tileFromCounterRate(
	src MetricsSource, name, id, title, unit, href string,
) MetricsTile {
	series, ok := src.MetricSnapshot(name)
	tile := MetricsTile{
		ID: id, Title: title, Unit: unit, Href: href,
		MetricID: name,
	}
	if !ok || len(series.Points) < 2 {
		tile.Empty = true
		tile.Value = "—"
		return tile
	}
	rate := perMinuteRate(series.Points)
	tile.Value = formatNumber(rate)
	tile.Spark = sparkFromPoints(series.Points, 60)
	tile.Trend = trendDirection(tile.Spark)
	return tile
}

// tileFromSuccessRate computes completed / (completed + failed) over
// the visible window. Returns the empty tile when either counter is
// missing so we don't show a misleading 0% or 100%.
func tileFromSuccessRate(src MetricsSource) MetricsTile {
	tile := MetricsTile{
		ID: "tile-success-rate", Title: "Success rate",
		Unit: "%", Href: "/console/runs",
		MetricID: "workflow.runs.completed",
	}
	comp, ok1 := src.MetricSnapshot("workflow.runs.completed")
	fail, ok2 := src.MetricSnapshot("workflow.runs.failed")
	if !ok1 || !ok2 || len(comp.Points) == 0 || len(fail.Points) == 0 {
		tile.Empty = true
		tile.Value = "—"
		return tile
	}
	c := comp.Latest().Value
	f := fail.Latest().Value
	if c+f <= 0 {
		tile.Empty = true
		tile.Value = "—"
		return tile
	}
	pct := 100 * c / (c + f)
	tile.Value = formatNumber(pct)
	tile.Spark = sparkFromPoints(comp.Points, 60)
	if pct >= 99 {
		tile.Trend = "up"
	} else if pct >= 95 {
		tile.Trend = "flat"
	} else {
		tile.Trend = "down"
	}
	return tile
}

// tileFromHistogramP50 renders a tile from a histogram's median (p50).
// Median uses linear interpolation across the cumulative bucket
// counts — same trick Prometheus's histogram_quantile uses.
func tileFromHistogramP50(
	src MetricsSource, name, id, title, unit, href string,
) MetricsTile {
	series, ok := src.MetricSnapshot(name)
	tile := MetricsTile{
		ID: id, Title: title, Unit: unit, Href: href,
		MetricID: name,
	}
	if !ok || len(series.Points) == 0 {
		tile.Empty = true
		tile.Value = "—"
		return tile
	}
	latest := series.Latest()
	if latest.Count == 0 || len(latest.Buckets) == 0 {
		tile.Empty = true
		tile.Value = "—"
		return tile
	}
	median := percentileFromBuckets(latest, 0.50)
	tile.Value = formatNumber(median)
	tile.Spark = sparkFromHistogramP50(series.Points, 60)
	tile.Trend = "flat"
	return tile
}

// formatNumber renders v in a tile-friendly form. Whole numbers stay
// whole; small fractions get one decimal; large numbers truncate to
// integer. Reused by sparkline tooltips so all on-screen numbers
// follow the same convention.
func formatNumber(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "—"
	}
	if v == math.Trunc(v) && math.Abs(v) < 1e6 {
		return fmt.Sprintf("%.0f", v)
	}
	if math.Abs(v) >= 100 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

// perMinuteRate computes a rate per minute from a counter series.
// Uses last point minus first point divided by elapsed minutes; for
// short windows we fall back to last point as a "live tick" value.
func perMinuteRate(points []MetricPoint) float64 {
	if len(points) < 2 {
		return 0
	}
	first := points[0]
	last := points[len(points)-1]
	dt := last.Timestamp.Sub(first.Timestamp).Minutes()
	if dt <= 0 {
		return last.Value
	}
	return (last.Value - first.Value) / dt
}

// sparkFromPoints downsamples a Point series into `bins` evenly-sized
// buckets. The sparkline doesn't need point-level fidelity — it shows
// shape, not detail.
func sparkFromPoints(points []MetricPoint, bins int) []float64 {
	if bins <= 0 {
		panic("sparkFromPoints: bins must be positive")
	}
	if len(points) == 0 {
		return nil
	}
	if len(points) <= bins {
		out := make([]float64, len(points))
		for i, p := range points {
			out[i] = p.Value
		}
		return out
	}
	out := make([]float64, bins)
	step := float64(len(points)) / float64(bins)
	for i := 0; i < bins; i++ {
		idx := int(step * float64(i))
		if idx >= len(points) {
			idx = len(points) - 1
		}
		out[i] = points[idx].Value
	}
	return out
}

// sparkFromHistogramP50 is the p50 variant of sparkFromPoints. Each
// downsampled bucket holds the p50 of the underlying histogram point.
func sparkFromHistogramP50(points []MetricPoint, bins int) []float64 {
	if bins <= 0 {
		panic("sparkFromHistogramP50: bins must be positive")
	}
	if len(points) == 0 {
		return nil
	}
	asValues := make([]MetricPoint, 0, len(points))
	for _, p := range points {
		if p.Count == 0 || len(p.Buckets) == 0 {
			continue
		}
		median := percentileFromBuckets(p, 0.50)
		asValues = append(asValues, MetricPoint{
			Timestamp: p.Timestamp, Value: median,
		})
	}
	return sparkFromPoints(asValues, bins)
}

// percentileFromBuckets returns the q-th percentile (q in [0,1]) of
// the histogram. Linear interpolation between cumulative bucket
// boundaries. Matches Prometheus's histogram_quantile semantics.
func percentileFromBuckets(p MetricPoint, q float64) float64 {
	if q < 0 || q > 1 {
		panic("percentileFromBuckets: q out of [0,1]")
	}
	if p.Count == 0 || len(p.Buckets) == 0 {
		return 0
	}
	rank := q * float64(p.Count)
	prev := 0.0
	prevCount := uint64(0)
	for i, b := range p.Buckets {
		if float64(b.Count) >= rank {
			if i == 0 {
				return b.UpperBound
			}
			bound := b.UpperBound
			if math.IsInf(bound, +1) {
				return prev
			}
			delta := float64(b.Count - prevCount)
			if delta <= 0 {
				return bound
			}
			frac := (rank - float64(prevCount)) / delta
			return prev + frac*(bound-prev)
		}
		prev = b.UpperBound
		prevCount = b.Count
	}
	return prev
}

// trendDirection classifies a sparkline into up / flat / down by
// comparing the first quarter average to the last quarter average.
func trendDirection(spark []float64) string {
	if len(spark) < 4 {
		return "flat"
	}
	quarter := len(spark) / 4
	first := avgSlice(spark[:quarter])
	last := avgSlice(spark[len(spark)-quarter:])
	delta := last - first
	if math.Abs(delta) < 0.5 {
		return "flat"
	}
	if delta > 0 {
		return "up"
	}
	return "down"
}

func avgSlice(xs []float64) float64 {
	if len(xs) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range xs {
		sum += v
	}
	return sum / float64(len(xs))
}

// buildMetricsCharts assembles the throughput + latency charts. Each
// chart's points are the raw aggregator points — the µPlot template
// handles downsampling for display.
func buildMetricsCharts(src MetricsSource) []MetricsChart {
	if src == nil {
		return nil
	}
	return []MetricsChart{
		buildThroughputChart(src),
		buildLatencyChart(src),
	}
}

// buildThroughputChart renders the runs-by-outcome stacked-line.
func buildThroughputChart(src MetricsSource) MetricsChart {
	out := MetricsChart{
		ID: "chart-throughput", Title: "Run throughput",
		Description: "Completed vs failed runs over the last hour.",
		Unit:        "runs", WindowLabel: "60m",
	}
	comp, hasComp := src.MetricSnapshot("workflow.runs.completed")
	fail, hasFail := src.MetricSnapshot("workflow.runs.failed")
	if !hasComp && !hasFail {
		out.Empty = true
		return out
	}
	xs, completedY := xyFromPoints(comp.Points)
	_, failedY := xyFromPoints(fail.Points)
	// Align lengths when one series is shorter.
	if len(failedY) < len(xs) {
		failedY = padFront(failedY, len(xs)-len(failedY))
	}
	if len(completedY) < len(xs) {
		completedY = padFront(completedY, len(xs)-len(completedY))
	}
	out.XAxis = xs
	out.Series = []ChartSeries{
		{Label: "Completed", Values: completedY, Stroke: "paper-indigo"},
		{Label: "Failed", Values: failedY, Stroke: "muted-rust"},
	}
	if len(xs) == 0 {
		out.Empty = true
	}
	return out
}

// buildLatencyChart renders snapshot save duration p50/p95/p99 lines.
func buildLatencyChart(src MetricsSource) MetricsChart {
	out := MetricsChart{
		ID: "chart-latency", Title: "Snapshot save latency",
		Description: "Engine snapshot persistence percentiles.",
		Unit:        "ms", WindowLabel: "60m",
		AnomalyThresholdRatio: AnomalyP99OverP50Ratio,
	}
	series, ok := src.MetricSnapshot("snapshot.save.duration_ms")
	if !ok || len(series.Points) == 0 {
		out.Empty = true
		return out
	}
	xs := make([]float64, 0, len(series.Points))
	p50 := make([]float64, 0, len(series.Points))
	p95 := make([]float64, 0, len(series.Points))
	p99 := make([]float64, 0, len(series.Points))
	for _, p := range series.Points {
		if p.Count == 0 {
			continue
		}
		xs = append(xs, float64(p.Timestamp.Unix()))
		p50 = append(p50, percentileFromBuckets(p, 0.50))
		p95 = append(p95, percentileFromBuckets(p, 0.95))
		p99 = append(p99, percentileFromBuckets(p, 0.99))
	}
	if len(xs) == 0 {
		out.Empty = true
		return out
	}
	out.XAxis = xs
	out.Series = []ChartSeries{
		{Label: "p50", Values: p50, Stroke: "paper-indigo"},
		{Label: "p95", Values: p95, Stroke: "warm-clay"},
		{Label: "p99", Values: p99, Stroke: "muted-rust"},
	}
	out.Anomalies = DetectAnomalies(series.Points)
	return out
}

// xyFromPoints renders a MetricPoint slice into parallel (x, y)
// arrays for µPlot consumption.
func xyFromPoints(points []MetricPoint) ([]float64, []float64) {
	xs := make([]float64, 0, len(points))
	ys := make([]float64, 0, len(points))
	for _, p := range points {
		xs = append(xs, float64(p.Timestamp.Unix()))
		ys = append(ys, p.Value)
	}
	return xs, ys
}

// padFront prepends n zero values to xs so two parallel arrays end
// the same length. Used by stacked-line charts where one outcome
// might have arrived later than another.
func padFront(xs []float64, n int) []float64 {
	if n <= 0 {
		return xs
	}
	const padMax = 4096
	if n > padMax {
		n = padMax
	}
	out := make([]float64, 0, len(xs)+n)
	for i := 0; i < n; i++ {
		out = append(out, 0)
	}
	out = append(out, xs...)
	return out
}

// buildWorkflowRows aggregates per-workflow stats from the workflow
// label on the counters. v1 keeps it simple: each row uses the
// label-less counters since we don't yet emit per-workflow labels.
// Reserved for future per-workflow label emission.
func buildWorkflowRows(src MetricsSource) []WorkflowMetricsRow {
	if src == nil {
		return nil
	}
	// v1: extract any workflow labels we can find on completed-runs.
	series, ok := src.MetricSnapshot("workflow.runs.completed")
	if !ok || len(series.Points) == 0 {
		return nil
	}
	byWorkflow := make(map[string]uint64)
	for _, p := range series.Points {
		wf := p.Labels["workflow"]
		if wf == "" {
			continue
		}
		byWorkflow[wf] += uint64(p.Value)
	}
	if len(byWorkflow) == 0 {
		return nil
	}
	names := make([]string, 0, len(byWorkflow))
	for k := range byWorkflow {
		names = append(names, k)
	}
	sort.Strings(names)
	out := make([]WorkflowMetricsRow, 0, len(names))
	for _, n := range names {
		out = append(out, WorkflowMetricsRow{
			Workflow: n, Completed1h: byWorkflow[n], HasData: true,
		})
	}
	return out
}

// buildStepRows is the per-step drilldown when a workflow is selected.
// v1 returns an empty list; populated when the engine starts emitting
// per-step labels (out of scope for this PR's emission work).
func buildStepRows(
	src MetricsSource, workflow string,
) []StepMetricsRow {
	if src == nil || workflow == "" {
		return nil
	}
	return nil
}
