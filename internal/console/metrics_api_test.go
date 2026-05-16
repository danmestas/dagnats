// internal/console/metrics_api_test.go
// Methodology: integration-style tests around the chart JSON endpoint.
// Uses a stub MetricsSource so the test stays in-process without
// NATS. Assertions cover the happy path, the unknown-chart 404, and
// the unwired-metrics 503 — three explicit branches the handler
// owns.
package console

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// stubMetricsSource is a minimal MetricsSource for tests.
type stubMetricsSource struct {
	series map[string]MetricSeries
}

func (s *stubMetricsSource) MetricNames() []string {
	names := make([]string, 0, len(s.series))
	for k := range s.series {
		names = append(names, k)
	}
	return names
}

func (s *stubMetricsSource) MetricSnapshot(
	name string,
) (MetricSeries, bool) {
	v, ok := s.series[name]
	return v, ok
}

func (s *stubMetricsSource) SubscribeMetric(
	filter string,
) (<-chan MetricEvent, func()) {
	ch := make(chan MetricEvent)
	return ch, func() {}
}

func newStubSource() *stubMetricsSource {
	now := time.Now()
	return &stubMetricsSource{
		series: map[string]MetricSeries{
			"workflow.runs.completed": {
				Name: "workflow.runs.completed",
				Kind: "counter",
				Points: []MetricPoint{
					{Timestamp: now.Add(-2 * time.Minute), Value: 100},
					{Timestamp: now.Add(-1 * time.Minute), Value: 110},
					{Timestamp: now, Value: 125},
				},
			},
			"workflow.runs.failed": {
				Name: "workflow.runs.failed",
				Kind: "counter",
				Points: []MetricPoint{
					{Timestamp: now, Value: 3},
				},
			},
		},
	}
}

func TestServeAPIMetricsChartHappyPath(t *testing.T) {
	src := newStubSource()
	cfg := Config{Metrics: src}
	req := httptest.NewRequest(
		http.MethodGet,
		"/console/api/metrics/chart/chart-throughput", nil,
	)
	rec := httptest.NewRecorder()
	serveAPIMetricsChart(rec, req, cfg)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rec.Code)
	}
	var payload chartDataPayload
	if err := json.Unmarshal(rec.Body.Bytes(), &payload); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if payload.ID != "chart-throughput" {
		t.Fatalf("id: %q", payload.ID)
	}
	if len(payload.Series) == 0 {
		t.Fatal("expected non-empty series")
	}
}

func TestServeAPIMetricsChart404OnUnknown(t *testing.T) {
	src := newStubSource()
	cfg := Config{Metrics: src}
	req := httptest.NewRequest(
		http.MethodGet,
		"/console/api/metrics/chart/no-such-chart", nil,
	)
	rec := httptest.NewRecorder()
	serveAPIMetricsChart(rec, req, cfg)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rec.Code)
	}
}

func TestServeAPIMetricsChart503OnNoMetrics(t *testing.T) {
	cfg := Config{Metrics: nil}
	req := httptest.NewRequest(
		http.MethodGet,
		"/console/api/metrics/chart/chart-throughput", nil,
	)
	rec := httptest.NewRecorder()
	serveAPIMetricsChart(rec, req, cfg)
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", rec.Code)
	}
}

func TestServeAPIMetricsChartRejectsPost(t *testing.T) {
	src := newStubSource()
	cfg := Config{Metrics: src}
	req := httptest.NewRequest(
		http.MethodPost,
		"/console/api/metrics/chart/chart-throughput", nil,
	)
	rec := httptest.NewRecorder()
	serveAPIMetricsChart(rec, req, cfg)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", rec.Code)
	}
}

func TestExtractChartID(t *testing.T) {
	cases := map[string]string{
		"/console/api/metrics/chart/chart-throughput": "chart-throughput",
		"/console/api/metrics/chart/":                 "",
		"/console/api/metrics/chart/x/y":              "",
		"/wrong/prefix/chart/chart-throughput":        "",
	}
	for path, want := range cases {
		if got := extractChartID(path); got != want {
			t.Fatalf("extractChartID(%q) = %q; want %q", path, got, want)
		}
	}
}

func TestChartFromMetricsCopiesAnomalies(t *testing.T) {
	chart := MetricsChart{
		ID:    "chart-latency",
		XAxis: []float64{1, 2, 3},
		Series: []ChartSeries{
			{Label: "p50", Values: []float64{1, 2, 3}, Stroke: "paper-indigo"},
		},
		Anomalies: []AnomalyMarker{
			{TimestampSecs: 1, ValueMs: 100, Reason: "high tail"},
		},
	}
	got := chartFromMetrics(chart)
	if got.ID != "chart-latency" {
		t.Fatalf("id: %q", got.ID)
	}
	if len(got.Anomalies) != 1 {
		t.Fatalf("anomalies: %d", len(got.Anomalies))
	}
	if got.Anomalies[0].Reason != "high tail" {
		t.Fatalf("reason: %q", got.Anomalies[0].Reason)
	}
}
