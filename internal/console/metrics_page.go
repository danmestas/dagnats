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

// metrics_page.go owns the /console/metrics page: the in-console
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
	// XMin/XMax pin the explicit x-domain (seconds-since-epoch) the
	// client hands µPlot's scales.x.range. Clamping the domain at the
	// source stops sparse data + unset-timestamp points from letting
	// the auto-ranger extrapolate future ticks (e.g. "Dec 2027").
	XMin float64
	XMax float64
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

// servePageMetrics renders /console/metrics. Builds the view, renders,
// exits. Live updates arrive on /console/sse/metrics (registered
// separately).
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
		Section: "metrics",
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
	tiles = appendSeriesTiles(tiles, cfg.Metrics)
	tiles = appendActiveRunsTile(ctx, tiles, cfg)
	tiles = appendDLQDepthTile(ctx, tiles, cfg)
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
			"Snapshot p50", "ms", "/console/metrics"),
		tileFromCounter(src,
			"workflow.runs.failed", "tile-failed",
			"Runs failed", "runs", "/console/dlq"),
	}
	return tiles
}

// appendSeriesTiles appends the mockup's SeriesCards for series the
// aggregator actually holds. Each tile carries the real OTel metric
// name (data-metric) so the card maps back to its instrument. The
// existence guard mirrors the dashboard's drop-empty-tiles stance:
// render only when the snapshot truly has points, never a placeholder.
func appendSeriesTiles(
	tiles []MetricsTile, src MetricsSource,
) []MetricsTile {
	if src == nil {
		return tiles
	}
	// "Active runs" is intentionally NOT sourced from the
	// workflow.runs.active UpDownCounter: that gauge's emit sites are
	// asymmetric (increments unlabeled, most decrements workflow-
	// labeled) and the aggregator merges all label slots into one
	// name-keyed series, so its reduced value drifts negative — an
	// in-flight count can never be negative. appendActiveRunsTile reads
	// the authoritative running-run count from cfg.Data instead.
	if hasMetricPoints(src, "task.concurrency.acquired") {
		tiles = append(tiles, tileFromCounter(src,
			"task.concurrency.acquired", "tile-concurrency-acquired",
			"Concurrency acquired", "tasks", "/console/concurrency"))
	}
	if hasMetricPoints(src, "step.enqueue.count") {
		tiles = append(tiles, tileFromCounter(src,
			"step.enqueue.count", "tile-step-enqueue",
			"Step enqueue", "steps", "/console/runs"))
	}
	return tiles
}

// hasMetricPoints reports whether the aggregator holds at least one
// point for name. Used to keep conditional SeriesCards honest: a
// series the telemetry pump doesn't deliver yields no tile at all.
func hasMetricPoints(src MetricsSource, name string) bool {
	if src == nil {
		return false
	}
	series, ok := src.MetricSnapshot(name)
	return ok && len(series.Points) > 0
}

// appendActiveRunsTile appends the "Active runs" tile sourced from the
// authoritative run state (count of runs in RUNNING status) via
// cfg.Data — the same path the dashboard's in-flight tile uses
// (countRunsForTiles). This deliberately replaces the
// workflow.runs.active UpDownCounter, whose asymmetric emit sites make
// its aggregated value drift negative; an in-flight count is a
// non-negative quantity, so we read it from real state. Omitted
// entirely when no data source is wired or the read fails.
func appendActiveRunsTile(
	ctx context.Context, tiles []MetricsTile, cfg Config,
) []MetricsTile {
	if ctx == nil {
		panic("appendActiveRunsTile: ctx is nil")
	}
	if cfg.Data == nil {
		return tiles
	}
	rctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	runs, err := cfg.Data.ListRuns(rctx, "")
	if err != nil {
		return tiles
	}
	inFlight, _ := countRunsForTiles(runs, time.Now())
	tile := MetricsTile{
		ID: "tile-runs-active", Title: "Active runs", Unit: "runs",
		Href: "/console/runs?status=running", MetricID: "runs in RUNNING state",
		Value: fmt.Sprintf("%d", inFlight),
		Trend: "flat",
	}
	return append(tiles, tile)
}

// appendDLQDepthTile appends the DLQ depth tile read from cfg.Data via
// ListDeadLetters — the same proven path readDashboardCounters uses.
// Depth comes from the DEAD_LETTERS stream, not a metrics counter, so
// the tile is omitted entirely when no data source is wired.
func appendDLQDepthTile(
	ctx context.Context, tiles []MetricsTile, cfg Config,
) []MetricsTile {
	if ctx == nil {
		panic("appendDLQDepthTile: ctx is nil")
	}
	if cfg.Data == nil {
		return tiles
	}
	rctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	entries, err := cfg.Data.ListDeadLetters(rctx, 200)
	if err != nil {
		return tiles
	}
	tile := MetricsTile{
		ID: "tile-dlq-depth", Title: "DLQ depth", Unit: "entries",
		Href: "/console/dlq", MetricID: "DEAD_LETTERS stream",
		Value: fmt.Sprintf("%d", len(entries)),
		Spark: dlqSparkFromMetrics(cfg.Metrics),
		Trend: "flat",
	}
	return append(tiles, tile)
}

// dlqSparkFromMetrics synthesises a DLQ sparkline for the metrics-page
// tile from the runs.failed counter — failed runs drive DLQ growth, so
// we reuse that history until a dedicated dlq.depth metric exists. Gated
// on >=2 points so a single sample can't render a flat block.
func dlqSparkFromMetrics(src MetricsSource) []float64 {
	if src == nil {
		return nil
	}
	series, ok := src.MetricSnapshot("workflow.runs.failed")
	if !ok || len(series.Points) < 2 {
		return nil
	}
	return sparkFromPoints(series.Points, 24)
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

// buildThroughputChart renders the runs-by-outcome lines on a single
// unified, sorted timestamp axis. The two cumulative counters
// (completed, failed) rarely share identical sample times, so we merge
// them onto the union of their timestamps and carry the last seen value
// forward for the side that has no sample at a given instant — the
// correct semantic for a monotonic counter. The previous length-only
// padFront misaligned the shorter series onto the wrong timestamps.
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
	xs, completedY, failedY := mergeCounterAxis(comp.Points, fail.Points)
	if len(xs) == 0 {
		out.Empty = true
		return out
	}
	out.XAxis = xs
	out.XMin, out.XMax = clampChartWindow(xs, time.Now().UTC())
	out.Series = []ChartSeries{
		{Label: "Completed", Values: completedY, Stroke: "paper-indigo"},
		{Label: "Failed", Values: failedY, Stroke: "muted-rust"},
	}
	return out
}

// mergeCounterAxis merges two counter point-slices onto the sorted
// union of their (epoch-junk-filtered) timestamps, returning the shared
// x-axis plus the two value series aligned to it. A side with no sample
// at a timestamp carries its last seen value forward (cumulative-counter
// semantic); before any sample it reads 0. Bounded by the input lengths.
func mergeCounterAxis(
	a, b []MetricPoint,
) ([]float64, []float64, []float64) {
	const maxPoints = 8192
	byTime := make(map[int64]struct{ a, b float64 })
	haveA := make(map[int64]bool)
	haveB := make(map[int64]bool)
	collectCounterPoints(a, byTime, haveA, true)
	collectCounterPoints(b, byTime, haveB, false)
	stamps := make([]int64, 0, len(byTime))
	for ts := range byTime {
		stamps = append(stamps, ts)
		if len(stamps) >= maxPoints {
			break
		}
	}
	sort.Slice(stamps, func(i, j int) bool { return stamps[i] < stamps[j] })
	xs := make([]float64, 0, len(stamps))
	aY := make([]float64, 0, len(stamps))
	bY := make([]float64, 0, len(stamps))
	lastA, lastB := 0.0, 0.0
	for _, ts := range stamps {
		v := byTime[ts]
		if haveA[ts] {
			lastA = v.a
		}
		if haveB[ts] {
			lastB = v.b
		}
		xs = append(xs, float64(ts))
		aY = append(aY, lastA)
		bY = append(bY, lastB)
	}
	return xs, aY, bY
}

// collectCounterPoints folds one point-slice into the shared maps.
// isA selects which slot of the merged value pair to write. Epoch-junk
// timestamps (x <= 0, e.g. an unset time.Time year-1 sentinel) are
// dropped so they cannot poison the chart's domain.
func collectCounterPoints(
	points []MetricPoint,
	byTime map[int64]struct{ a, b float64 },
	have map[int64]bool,
	isA bool,
) {
	if byTime == nil || have == nil {
		panic("collectCounterPoints: nil map")
	}
	for _, p := range points {
		ts := p.Timestamp.Unix()
		if ts <= 0 {
			continue
		}
		cur := byTime[ts]
		if isA {
			cur.a = p.Value
		} else {
			cur.b = p.Value
		}
		byTime[ts] = cur
		have[ts] = true
	}
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
		x := float64(p.Timestamp.Unix())
		if p.Count == 0 || x <= 0 {
			continue
		}
		xs = append(xs, x)
		p50 = append(p50, percentileFromBuckets(p, 0.50))
		p95 = append(p95, percentileFromBuckets(p, 0.95))
		p99 = append(p99, percentileFromBuckets(p, 0.99))
	}
	if len(xs) == 0 {
		out.Empty = true
		return out
	}
	out.XAxis = xs
	out.XMin, out.XMax = clampChartWindow(xs, time.Now().UTC())
	out.Series = []ChartSeries{
		{Label: "p50", Values: p50, Stroke: "paper-indigo"},
		{Label: "p95", Values: p95, Stroke: "warm-clay"},
		{Label: "p99", Values: p99, Stroke: "muted-rust"},
	}
	out.Anomalies = DetectAnomalies(series.Points)
	return out
}

// chartWindowSecs is the visible span of the "60m" window charts.
const chartWindowSecs = 3600

// clampChartWindow derives the explicit [lo, hi] x-domain the client
// pins µPlot to. It drops epoch-junk and pre-window stale points
// before computing the range so a single unset-timestamp sample can't
// stretch the axis back to year 1 or forward past now. With no valid
// points it returns a recent window ending at now so the axis still
// renders a sane minute rather than a degenerate point.
func clampChartWindow(xs []float64, now time.Time) (float64, float64) {
	if now.IsZero() {
		panic("clampChartWindow: now is zero")
	}
	nowSecs := float64(now.Unix())
	floor := nowSecs - (chartWindowSecs + 60)
	minValid, maxValid := 0.0, 0.0
	found := false
	for _, x := range xs {
		if x <= 0 || x < floor {
			continue
		}
		if !found || x < minValid {
			minValid = x
		}
		if !found || x > maxValid {
			maxValid = x
		}
		found = true
	}
	if !found {
		return nowSecs - 60, nowSecs
	}
	hi := math.Min(maxValid, nowSecs)
	lo := math.Max(minValid, hi-chartWindowSecs)
	if lo >= hi {
		lo = hi - 60
	}
	return lo, hi
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
