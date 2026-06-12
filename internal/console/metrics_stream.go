package console

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"github.com/starfederation/datastar-go/datastar"
)

// metrics_stream.go owns /console/sse/metrics. Subscribers receive a
// Datastar PatchElements event per accepted metric ingest: the
// affected tile re-renders with its new value + sparkline. Throttled
// to MetricsPatchHz to keep the wire calm during high-throughput
// bursts.

// MetricsPatchHz is the hard upper bound on patches-per-tile-per
// second the SSE handler emits. 4 Hz matches the brief's "≤4Hz to
// avoid overwhelming the browser" target.
const MetricsPatchHz = 4

// metricsPatchInterval is the per-tile throttle window derived from
// MetricsPatchHz.
const metricsPatchInterval = time.Second / MetricsPatchHz

// serveSSEMetrics streams metric tile re-renders to a client. Each
// accepted aggregator Update produces one Datastar PatchElements
// event targeting the affected tile by ID. The handler runs for the
// lifetime of the client connection and exits when ctx is cancelled.
func serveSSEMetrics(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveSSEMetrics: w is nil")
	}
	if r == nil {
		panic("serveSSEMetrics: r is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	if cfg.Metrics == nil {
		http.Error(w, "metrics not wired",
			http.StatusServiceUnavailable)
		return
	}
	ch, cancel := cfg.Metrics.SubscribeMetric("")
	defer cancel()
	sse := datastar.NewSSE(w, r)
	pumpMetricPatches(r.Context(), sse, ts.base, ch, cfg)
}

// pumpMetricPatches drains the subscription channel, rendering one
// tile patch per accepted Update. Bounded loop count + per-metric
// throttle map.
func pumpMetricPatches(
	ctx context.Context,
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template,
	ch <-chan MetricEvent,
	cfg Config,
) {
	if ctx == nil {
		panic("pumpMetricPatches: ctx is nil")
	}
	if sse == nil {
		panic("pumpMetricPatches: sse is nil")
	}
	lastPatch := make(map[string]time.Time, 16)
	const maxIter = 1_000_000_000
	for i := 0; i < maxIter; i++ {
		select {
		case <-ctx.Done():
			return
		case evt, ok := <-ch:
			if !ok {
				return
			}
			if !shouldPatch(lastPatch, evt.Name) {
				continue
			}
			patchOneMetric(sse, tmpl, evt, cfg)
		}
	}
}

// shouldPatch enforces the per-tile throttle window. Returns true if
// the metric hasn't been patched recently enough to skip; updates the
// last-patch timestamp on success.
func shouldPatch(
	last map[string]time.Time, name string,
) bool {
	now := time.Now()
	prev, ok := last[name]
	if ok && now.Sub(prev) < metricsPatchInterval {
		return false
	}
	last[name] = now
	return true
}

// patchOneMetric renders a fresh tile for the metric the event
// references and writes a Datastar PatchElements event with the new
// HTML. Errors are logged but do not abort the stream.
func patchOneMetric(
	sse *datastar.ServerSentEventGenerator,
	tmpl *template.Template,
	evt MetricEvent,
	cfg Config,
) {
	tile, ok := tileForMetric(cfg.Metrics, evt.Name)
	if !ok {
		return
	}
	html, err := renderMetricTile(tmpl, tile)
	if err != nil {
		cfg.Logger.Warn("metrics sse: render tile",
			"name", evt.Name, "err", err)
		return
	}
	if err := sse.PatchElements(html); err != nil {
		cfg.Logger.Debug("metrics sse: patch dropped",
			"name", evt.Name, "err", err)
	}
}

// tileForMetric maps a metric name to the tile it powers. The
// dashboard has four tiles; updates to non-tile metrics are dropped
// without warning so non-dashboard metrics don't bloat the SSE.
func tileForMetric(
	src MetricsSource, name string,
) (MetricsTile, bool) {
	switch name {
	case "workflow.runs.completed":
		return tileFromCounterRate(src,
			"workflow.runs.completed", "tile-runs-rate",
			"Runs / min", "runs", "/console/runs"), true
	case "workflow.runs.failed":
		return tileFromCounter(src,
			"workflow.runs.failed", "tile-failed",
			"Runs failed", "runs", "/console/dlq"), true
	case "snapshot.save.duration_ms":
		return tileFromHistogramP50(src,
			"snapshot.save.duration_ms", "tile-snapshot-p50",
			"Snapshot p50", "ms", "/console/metrics"), true
	}
	return MetricsTile{}, false
}

// renderMetricTile executes the metric_tile fragment template
// against one tile and returns the rendered HTML.
func renderMetricTile(
	tmpl *template.Template, tile MetricsTile,
) (string, error) {
	if tmpl == nil {
		panic("renderMetricTile: tmpl is nil")
	}
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "metric_tile", tile); err != nil {
		return "", fmt.Errorf("execute metric_tile: %w", err)
	}
	return buf.String(), nil
}
