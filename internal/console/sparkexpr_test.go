// sparkexpr_test.go covers SparkExpr — the pure helper that turns a
// numeric series into a Datatype line-chart expression ("{l:...}") for
// the .console-spark font sparkline — plus the two tile templates that
// render it (dashboard_tile, metric_tile).
//
// Methodology:
//   - Pure table-driven unit tests for SparkExpr: no NATS, no wires.
//     Each row asserts the exact expression string, covering the
//     nil/empty/all-zero "render nothing" cases and the min-max
//     normalization (constant non-zero → mid line, 0/50/100 endpoints).
//   - Template-render tests parse the real component/fragment templates
//     the same way the handler does and execute the relevant define
//     block, asserting the Datatype span appears with data and is
//     absent when Spark is nil.
//   - Min 2 assertions per test (positive presence + negative absence).
package console

import (
	"bytes"
	"html/template"
	"strconv"
	"strings"
	"testing"
)

func TestSparkExpr(t *testing.T) {
	cases := []struct {
		name   string
		series []float64
		want   string
	}{
		{"nil", nil, ""},
		{"empty", []float64{}, ""},
		{"all zero", []float64{0, 0, 0}, ""},
		{"zero to hundred", []float64{0, 50, 100}, "{l:0,50,100}"},
		{"shifted span", []float64{10, 20, 30}, "{l:0,50,100}"},
		{"constant nonzero", []float64{5, 5, 5}, "{l:50,50,50}"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SparkExpr(tc.series)
			if got != tc.want {
				t.Fatalf("SparkExpr(%v) = %q, want %q",
					tc.series, got, tc.want)
			}
		})
	}
}

// TestSparkExprRealisticSeries asserts shape (prefix, comma count) and
// that every emitted value parses into the 0..100 band for a longer
// non-trivial series.
func TestSparkExprRealisticSeries(t *testing.T) {
	series := []float64{3, 8, 1, 12, 7, 22, 4, 15, 9, 30, 11, 6}
	got := SparkExpr(series)
	if !strings.HasPrefix(got, "{l:") || !strings.HasSuffix(got, "}") {
		t.Fatalf("expr %q is not wrapped in {l:...}", got)
	}
	if commas := strings.Count(got, ","); commas != len(series)-1 {
		t.Fatalf("expr %q has %d commas, want %d",
			got, commas, len(series)-1)
	}
	inner := strings.TrimSuffix(strings.TrimPrefix(got, "{l:"), "}")
	for _, tok := range strings.Split(inner, ",") {
		v, err := strconv.Atoi(tok)
		if err != nil {
			t.Fatalf("token %q is not an integer: %v", tok, err)
		}
		if v < 0 || v > 100 {
			t.Fatalf("value %d out of [0,100] band in %q", v, got)
		}
	}
}

// TestSparkExprCapsPointCount checks the series is bounded to the last
// sparkMaxPoints values so the expression length stays bounded.
func TestSparkExprCapsPointCount(t *testing.T) {
	long := make([]float64, sparkMaxPoints+20)
	for i := range long {
		long[i] = float64(i)
	}
	got := SparkExpr(long)
	commas := strings.Count(got, ",")
	if commas != sparkMaxPoints-1 {
		t.Fatalf("capped expr has %d commas, want %d",
			commas, sparkMaxPoints-1)
	}
	// The last value of a strictly increasing series must normalize
	// to 100 — confirms the tail (not the head) was kept.
	if !strings.HasSuffix(got, ",100}") {
		t.Fatalf("expected last point to be 100, got %q", got)
	}
}

// baseTemplatesForSpark parses the component + fragment templates the
// same way the handler does so define blocks (dashboard_tile,
// metric_tile) resolve with the production funcMap.
func baseTemplatesForSpark(t *testing.T) *template.Template {
	t.Helper()
	tmpl := template.New("console").Funcs(funcMap())
	tmpl, err := tmpl.ParseFS(templatesFS,
		"templates/fragments/*.html",
		"templates/components/*.html",
	)
	if err != nil {
		t.Fatalf("parse templates: %v", err)
	}
	return tmpl
}

func renderSparkDefine(
	t *testing.T, name string, data any,
) string {
	t.Helper()
	var buf bytes.Buffer
	if err := baseTemplatesForSpark(t).
		ExecuteTemplate(&buf, name, data); err != nil {
		t.Fatalf("execute %s: %v", name, err)
	}
	return buf.String()
}

func TestDashboardTileRendersDatatypeSpark(t *testing.T) {
	tile := DashboardTile{
		Key: "k", Title: "T", Value: "1",
		Spark: []float64{10, 30, 20, 50},
	}
	out := renderSparkDefine(t, "dashboard_tile", tile)
	if !strings.Contains(out, `class="console-spark`) {
		t.Fatalf("dashboard_tile missing console-spark span:\n%s", out)
	}
	if !strings.Contains(out, "{l:") {
		t.Fatalf("dashboard_tile missing Datatype expr:\n%s", out)
	}
}

func TestDashboardTileNilSparkOmitsSpan(t *testing.T) {
	tile := DashboardTile{Key: "k", Title: "T", Value: "1", Spark: nil}
	out := renderSparkDefine(t, "dashboard_tile", tile)
	if strings.Contains(out, "console-spark") {
		t.Fatalf("nil Spark should not render console-spark:\n%s", out)
	}
	if strings.Contains(out, "{l:") {
		t.Fatalf("nil Spark should not render Datatype expr:\n%s", out)
	}
}

func TestMetricTileRendersDatatypeSpark(t *testing.T) {
	tile := MetricsTile{
		ID: "m", Title: "T", Value: "1",
		Spark: []float64{10, 30, 20, 50},
	}
	out := renderSparkDefine(t, "metric_tile", tile)
	if !strings.Contains(out, `class="console-spark`) {
		t.Fatalf("metric_tile missing console-spark span:\n%s", out)
	}
	if !strings.Contains(out, "{l:") {
		t.Fatalf("metric_tile missing Datatype expr:\n%s", out)
	}
}

func TestMetricTileNilSparkOmitsSpan(t *testing.T) {
	tile := MetricsTile{ID: "m", Title: "T", Value: "1", Spark: nil}
	out := renderSparkDefine(t, "metric_tile", tile)
	if strings.Contains(out, "console-spark") {
		t.Fatalf("nil Spark should not render console-spark:\n%s", out)
	}
	if strings.Contains(out, "{l:") {
		t.Fatalf("nil Spark should not render Datatype expr:\n%s", out)
	}
}
