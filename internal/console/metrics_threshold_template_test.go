// internal/console/metrics_threshold_template_test.go
// Methodology: grep-style assertion that the anomaly threshold the
// Go constant defines (AnomalyP99OverP50Ratio) matches the value the
// metrics page glossary <details> text renders. Catches the
// drift-between-two-truths bug the Norman audit called out for PR 7:
// the constant and the marketing text used to live in two places.
package console

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestMetricsTemplateRendersAnomalyThresholdConstant(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	// Seed a histogram so the latency chart renders (the glossary
	// text only appears when a chart is drawn — empty charts skip
	// it). Two buckets, modest count.
	src.addHistogram("snapshot.save.duration_ms", 5,
		[]MetricBucket{
			{UpperBound: 5, Count: 3},
			{UpperBound: 10, Count: 5},
		}, now)
	cfg := makeMetricsCfg(t, src)
	rec := exerciseMetrics(t, cfg, "/console/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("metrics page status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "How to read these percentiles") {
		t.Fatalf("metrics page glossary <details> missing")
	}
	wantNum := strconv.FormatFloat(
		AnomalyP99OverP50Ratio, 'g', -1, 64,
	)
	// The template renders "<strong> <number>&times; p50</strong>"
	// — assert the number is present in the glossary fragment.
	wantToken := wantNum + "&times;"
	if !strings.Contains(body, wantToken) {
		t.Errorf("metrics glossary text missing %q; got fragment:\n%s",
			wantToken, glossaryFragment(body))
	}
}

func TestMetricsTemplateRendersAnomalyClickHint(t *testing.T) {
	src := newFakeMetricsSource()
	now := time.Now()
	src.addHistogram("snapshot.save.duration_ms", 5,
		[]MetricBucket{
			{UpperBound: 5, Count: 3},
			{UpperBound: 10, Count: 5},
		}, now)
	cfg := makeMetricsCfg(t, src)
	rec := exerciseMetrics(t, cfg, "/console/metrics")
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	// PR 8 reworded the glossary to advertise the click affordance.
	// If a future change drops the hint, the test catches it.
	if !strings.Contains(rec.Body.String(),
		"click a marker to inspect runs") {
		t.Errorf("expected click-to-runs hint in glossary, got:\n%s",
			glossaryFragment(rec.Body.String()))
	}
}

// glossaryFragment extracts a slice around the glossary <details>
// for test failure messages. Bounded length keeps failure output
// readable.
func glossaryFragment(body string) string {
	const maxLen = 400
	idx := strings.Index(body, "How to read these")
	if idx < 0 {
		return "(no glossary found)"
	}
	end := idx + maxLen
	if end > len(body) {
		end = len(body)
	}
	return body[idx:end]
}
