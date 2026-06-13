// internal/console/metrics_api.go
// JSON endpoint that returns the current chart data for one chart by
// ID. Used by metrics.js to refresh a uPlot instance via setData()
// without re-rendering the whole metrics page. The shape is
// deliberately small: just the (x, series, anomalies) the client
// needs. Two charts ship today: chart-throughput, chart-latency.
package console

import (
	"encoding/json"
	"net/http"
	"strings"
)

// chartDataPayload is the wire shape the JSON endpoint returns. It
// mirrors MetricsChart's exposed fields, dropping the template-only
// hints (Title, Description, etc.). Kept stable so a future change
// can extend without breaking the client decoder.
type chartDataPayload struct {
	ID        string               `json:"id"`
	XAxis     []float64            `json:"x"`
	XMin      float64              `json:"xmin"`
	XMax      float64              `json:"xmax"`
	Series    []chartSeriesPayload `json:"series"`
	Anomalies []AnomalyMarker      `json:"anomalies,omitempty"`
	Empty     bool                 `json:"empty,omitempty"`
}

type chartSeriesPayload struct {
	Label  string    `json:"label"`
	Values []float64 `json:"values"`
	Stroke string    `json:"stroke"`
}

// serveAPIMetricsChart returns the JSON shape for /console/api/metrics/chart/{id}.
// 404 on unknown id; 503 when metrics aren't wired. The handler is
// loopback-trusted via the console mount path.
func serveAPIMetricsChart(
	w http.ResponseWriter, r *http.Request, cfg Config,
) {
	if w == nil {
		panic("serveAPIMetricsChart: w is nil")
	}
	if r == nil {
		panic("serveAPIMetricsChart: r is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		http.Error(w, "method not allowed",
			http.StatusMethodNotAllowed)
		return
	}
	id := extractChartID(r.URL.Path)
	if id == "" {
		http.NotFound(w, r)
		return
	}
	if cfg.Metrics == nil {
		http.Error(w, "metrics not wired",
			http.StatusServiceUnavailable)
		return
	}
	chart, ok := buildChartByID(cfg.Metrics, id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	encoded, err := json.Marshal(chartFromMetrics(chart))
	if err != nil {
		http.Error(w, "encode failed",
			http.StatusInternalServerError)
		return
	}
	_, _ = w.Write(encoded)
}

// extractChartID parses the trailing id segment from
// /console/api/metrics/chart/{id}. Returns empty when the path lacks
// a segment (caller treats as 404).
func extractChartID(path string) string {
	const prefix = "/console/api/metrics/chart/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	if rest == "" || strings.Contains(rest, "/") {
		return ""
	}
	return rest
}

// buildChartByID returns one chart by its public ID. The two ids the
// dashboard renders today come from buildMetricsCharts; we re-derive
// here so the JSON endpoint mirrors the rendered HTML.
func buildChartByID(
	src MetricsSource, id string,
) (MetricsChart, bool) {
	switch id {
	case "chart-throughput":
		return buildThroughputChart(src), true
	case "chart-latency":
		return buildLatencyChart(src), true
	}
	return MetricsChart{}, false
}

// chartFromMetrics translates a MetricsChart into the JSON shape.
// Pure function so unit tests can verify the wire shape without
// spinning up an HTTP fixture.
func chartFromMetrics(c MetricsChart) chartDataPayload {
	out := chartDataPayload{
		ID: c.ID, XAxis: c.XAxis, XMin: c.XMin, XMax: c.XMax,
		Empty: c.Empty, Anomalies: c.Anomalies,
	}
	out.Series = make([]chartSeriesPayload, 0, len(c.Series))
	for _, s := range c.Series {
		// Type conversion is safe: chartSeriesPayload mirrors
		// ChartSeries exactly. If a field is ever added to
		// ChartSeries the compile would catch the drift here.
		out.Series = append(out.Series, chartSeriesPayload(s))
	}
	return out
}
