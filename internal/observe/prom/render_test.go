// render_test.go covers the Prometheus text-format renderer.
//
// Methodology:
//   - Build a freshly-allocated metrics.Aggregator per test, ingest a
//     few synthetic points, render to bytes.Buffer, assert on the
//     resulting text.
//   - Where the assertion is about format shape ("must contain
//     `# TYPE`"), use strings.Contains so subsequent ordering changes
//     don't false-fail the test.
//   - Minimum 2 assertions per test.
package prom

import (
	"bytes"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/observe/metrics"
)

func silentLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestRender_CounterEmitsHelpTypeAndTotalSuffix(t *testing.T) {
	agg := metrics.NewAggregator(silentLogger())
	defer agg.Close()
	meta := metrics.Series{
		Name: "workflow.runs.completed", Kind: metrics.KindCounter,
		Description: "Number of runs that reached a terminal Completed state.",
	}
	if err := agg.Ingest(meta, metrics.Point{Value: 7, Timestamp: time.Now()}); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	var buf bytes.Buffer
	if err := Render(&buf, agg); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := buf.String()
	mustContain(t, out, "# HELP workflow_runs_completed_total Number of runs")
	mustContain(t, out, "# TYPE workflow_runs_completed_total counter")
	mustContain(t, out, "workflow_runs_completed_total 7")
}

// TestRender_CumulativeCounterRendersLatestNoDoubleCount pins the
// consumer side of the delta→cumulative temporality switch. The
// exporter now publishes cumulative counter totals; the prom renderer
// reads s.Latest().Value directly and must emit exactly that latest
// total — it must NOT sum successive samples (which would double-count
// under cumulative). Positive: two cumulative samples (10 then 25)
// render as 25. Negative: the sum 35 must never appear.
func TestRender_CumulativeCounterRendersLatestNoDoubleCount(t *testing.T) {
	agg := metrics.NewAggregator(silentLogger())
	defer agg.Close()
	meta := metrics.Series{
		Name: "workflow.runs.completed", Kind: metrics.KindCounter,
	}
	base := time.Now()
	if err := agg.Ingest(meta, metrics.Point{Value: 10, Timestamp: base}); err != nil {
		t.Fatalf("ingest first: %v", err)
	}
	if err := agg.Ingest(meta, metrics.Point{
		Value: 25, Timestamp: base.Add(time.Minute),
	}); err != nil {
		t.Fatalf("ingest second: %v", err)
	}
	var buf bytes.Buffer
	if err := Render(&buf, agg); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := buf.String()
	mustContain(t, out, "workflow_runs_completed_total 25")
	if strings.Contains(out, "workflow_runs_completed_total 35") {
		t.Fatalf("cumulative counter double-counted (summed deltas):\n%s", out)
	}
}

func TestRender_GaugeNoTotalSuffix(t *testing.T) {
	agg := metrics.NewAggregator(silentLogger())
	defer agg.Close()
	if err := agg.Ingest(
		metrics.Series{Name: "workers_active", Kind: metrics.KindGauge},
		metrics.Point{Value: 12, Timestamp: time.Now()},
	); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	var buf bytes.Buffer
	_ = Render(&buf, agg)
	out := buf.String()
	mustContain(t, out, "# TYPE workers_active gauge")
	if strings.Contains(out, "workers_active_total") {
		t.Fatalf("gauge must not carry _total suffix:\n%s", out)
	}
}

func TestRender_HistogramEmitsBucketSumCount(t *testing.T) {
	agg := metrics.NewAggregator(silentLogger())
	defer agg.Close()
	pt := metrics.Point{
		Timestamp: time.Now(),
		Sum:       1.25,
		Count:     5,
		Buckets: []metrics.HistogramBucket{
			{UpperBound: 0.1, Count: 2},
			{UpperBound: 0.5, Count: 4},
			{UpperBound: 1.0, Count: 5},
		},
	}
	if err := agg.Ingest(
		metrics.Series{Name: "run_duration_seconds", Kind: metrics.KindHistogram},
		pt,
	); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	var buf bytes.Buffer
	_ = Render(&buf, agg)
	out := buf.String()
	mustContain(t, out, "# TYPE run_duration_seconds histogram")
	mustContain(t, out, `run_duration_seconds_bucket{le="0.1"} 2`)
	mustContain(t, out, `run_duration_seconds_bucket{le="1"} 5`)
	mustContain(t, out, "run_duration_seconds_sum")
	mustContain(t, out, "run_duration_seconds_count")
}

func TestRender_EmptyAggregatorEmitsBanner(t *testing.T) {
	agg := metrics.NewAggregator(silentLogger())
	defer agg.Close()
	var buf bytes.Buffer
	if err := Render(&buf, agg); err != nil {
		t.Fatalf("render: %v", err)
	}
	out := buf.String()
	if !strings.HasPrefix(out, "# no metrics yet") {
		t.Fatalf("empty aggregator must emit banner, got:\n%s", out)
	}
	if strings.Contains(out, "# TYPE") {
		t.Fatal("banner output must not contain TYPE lines")
	}
}

func TestRender_LabelsSortedAndEscaped(t *testing.T) {
	agg := metrics.NewAggregator(silentLogger())
	defer agg.Close()
	pt := metrics.Point{
		Value:     1,
		Timestamp: time.Now(),
		Labels:    map[string]string{"z": "Z", "a": "A", "weird": `quote "x"`},
	}
	if err := agg.Ingest(
		metrics.Series{Name: "x_count", Kind: metrics.KindGauge}, pt,
	); err != nil {
		t.Fatalf("ingest: %v", err)
	}
	var buf bytes.Buffer
	_ = Render(&buf, agg)
	out := buf.String()
	// "a" must precede "weird" must precede "z" because labels are sorted.
	want := `{a="A",weird="quote \"x\"",z="Z"}`
	mustContain(t, out, want)
	if !strings.Contains(out, "x_count") {
		t.Fatalf("series name missing from output:\n%s", out)
	}
}

func TestPromMetricName_TranslatesDotsAndAppendsTotal(t *testing.T) {
	cases := []struct {
		in   string
		kind metrics.Kind
		want string
	}{
		{"workflow.runs.completed", metrics.KindCounter, "workflow_runs_completed_total"},
		{"workers.active", metrics.KindGauge, "workers_active"},
		{"x", metrics.KindCounter, "x_total"},
		{"already_total", metrics.KindCounter, "already_total"},
	}
	for _, c := range cases {
		got := promMetricName(c.in, c.kind)
		if got != c.want {
			t.Fatalf("promMetricName(%q,%q) = %q, want %q",
				c.in, c.kind, got, c.want)
		}
	}
	// Defensive: empty name returns empty.
	if got := promMetricName("", metrics.KindCounter); got != "" {
		t.Fatalf("empty name should map to empty, got %q", got)
	}
}

func mustContain(t *testing.T, haystack, needle string) {
	t.Helper()
	if !strings.Contains(haystack, needle) {
		t.Fatalf("output missing %q:\n---\n%s\n---", needle, haystack)
	}
}
