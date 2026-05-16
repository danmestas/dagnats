// internal/console/metrics_anomaly_test.go
// Methodology: table-driven tests around the anomaly threshold. The
// detector is a pure function over MetricPoint slices, so no NATS or
// HTTP fixtures are needed. Each case asserts both the positive case
// (anomaly present) and the negative space (no marker emitted).
package console

import (
	"testing"
	"time"
)

// buildHistogramPoint constructs a one-point histogram with the
// given p50/p99 shape via two buckets. The bucket bounds are tuned
// so percentileFromBuckets returns approximately the requested
// values — exact enough for the threshold logic.
func buildHistogramPoint(p50ms, p99ms float64) MetricPoint {
	// Bucket schema: 1% of mass at p99, 50% at p50, 50% at p50 again.
	// The cumulative-count math interpolates between (p50, 50) and
	// (p99, 99); we pad with a zero bucket up front to anchor the
	// interpolation.
	const total = uint64(100)
	return MetricPoint{
		Timestamp: time.Unix(1700000000, 0),
		Count:     total,
		Sum:       float64(total) * p50ms,
		Buckets: []MetricBucket{
			{UpperBound: 0, Count: 0},
			{UpperBound: p50ms, Count: 50},
			{UpperBound: p99ms, Count: 99},
			{UpperBound: p99ms * 2, Count: total},
		},
	}
}

func TestDetectAnomaliesFlagsHighP99(t *testing.T) {
	// p99 = 50, p50 = 10 → ratio 5 > 3. Should emit a marker.
	pts := []MetricPoint{buildHistogramPoint(10, 50)}
	got := DetectAnomalies(pts)
	if len(got) != 1 {
		t.Fatalf("expected 1 marker, got %d", len(got))
	}
	if got[0].ValueMs < 40 || got[0].ValueMs > 60 {
		t.Fatalf("unexpected ValueMs %f", got[0].ValueMs)
	}
	if got[0].Reason == "" {
		t.Fatal("marker.Reason must not be empty")
	}
}

func TestDetectAnomaliesIgnoresHealthyPoints(t *testing.T) {
	// p99 = 12, p50 = 10 → ratio 1.2, well below threshold.
	pts := []MetricPoint{buildHistogramPoint(10, 12)}
	got := DetectAnomalies(pts)
	if len(got) != 0 {
		t.Fatalf("expected no markers, got %d", len(got))
	}
}

func TestDetectAnomaliesIgnoresNearZeroP50(t *testing.T) {
	// p50 = 0.5ms (below AnomalyMinP50Ms=1.0). Even though the
	// ratio is huge (200x), the floor suppresses the marker.
	pts := []MetricPoint{buildHistogramPoint(0.5, 100)}
	got := DetectAnomalies(pts)
	if len(got) != 0 {
		t.Fatalf("expected no markers (sub-floor p50), got %d", len(got))
	}
}

func TestDetectAnomaliesSkipsZeroCount(t *testing.T) {
	p := buildHistogramPoint(10, 50)
	p.Count = 0
	got := DetectAnomalies([]MetricPoint{p})
	if len(got) != 0 {
		t.Fatalf("expected no markers on zero-count point, got %d", len(got))
	}
}

func TestDetectAnomaliesNilInput(t *testing.T) {
	if got := DetectAnomalies(nil); got != nil {
		t.Fatalf("expected nil on nil input, got %v", got)
	}
}

func TestIsAnomalousThresholdConstants(t *testing.T) {
	// Pin the constants so future refactors that drift the
	// thresholds will fail this guard and demand intentional review.
	if AnomalyP99OverP50Ratio != 3.0 {
		t.Fatalf("threshold drift: got %f", AnomalyP99OverP50Ratio)
	}
	if AnomalyMinP50Ms != 1.0 {
		t.Fatalf("floor drift: got %f", AnomalyMinP50Ms)
	}
}

func TestDetectAnomaliesEmitsMultipleMarkers(t *testing.T) {
	pts := []MetricPoint{
		buildHistogramPoint(10, 50),  // anomaly
		buildHistogramPoint(10, 12),  // healthy
		buildHistogramPoint(15, 100), // anomaly
	}
	got := DetectAnomalies(pts)
	if len(got) != 2 {
		t.Fatalf("expected 2 markers, got %d", len(got))
	}
}

func TestFormatRatioBoundaries(t *testing.T) {
	cases := map[float64]string{
		3.2: "3.2", 11.7: "12", 0.0: "0.0",
	}
	for in, want := range cases {
		if got := formatRatio(in); got != want {
			t.Fatalf("formatRatio(%v) = %q; want %q", in, got, want)
		}
	}
}
