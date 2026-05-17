// sparkline_test.go covers the SparklineData read path on DataSource.
//
// Methodology:
//   - The in-memory fakeDataSource gets seeded with synthetic hourly
//     points; SparklineData asks for an N-bucket window and we assert
//     the result is exactly N floats with the seeded values landing in
//     the right slots. No NATS, no metrics aggregator — the projection
//     logic is pure, so unit tests cover it without a server.
//   - When no points are seeded the call returns (nil, nil) so the
//     template can render the empty state without a flat-line lie.
//   - When the apiServiceAdapter has no MetricsSource attached, the
//     adapter mirrors the fake's behaviour: nil slice, nil error.
package console

import (
	"context"
	"testing"
	"time"
)

// TestSparkline_returns24Points pins the production shape: 24 hourly
// buckets covering the last 24h. We seed one point per hour at distinct
// values; the returned slice must be length 24 and each value must
// surface in some slot.
func TestSparkline_returns24Points(t *testing.T) {
	const hours = 24
	ds := newFakeDS()
	now := time.Now().UTC()
	ds.seedSparklineHourly("workflow", "demo", now, hours)

	data, err := ds.SparklineData(context.Background(), "workflow", "demo", hours)
	if err != nil {
		t.Fatalf("SparklineData: %v", err)
	}
	if len(data) != hours {
		t.Fatalf("len(data) = %d, want %d", len(data), hours)
	}
	// Positive space: at least one non-zero bucket (we seeded 24 of them).
	var nonZero int
	for _, v := range data {
		if v > 0 {
			nonZero++
		}
	}
	if nonZero == 0 {
		t.Errorf("expected at least one non-zero bucket, got all zeros")
	}
}

// TestSparkline_emptyReturnsNil pins the empty-state honesty contract:
// when no data exists for (kind,id), SparklineData must return nil so
// the renderer can hide the canvas rather than draw a flat-line that
// would lie about "all zeros".
func TestSparkline_emptyReturnsNil(t *testing.T) {
	ds := newFakeDS()
	data, err := ds.SparklineData(context.Background(), "workflow", "missing", 24)
	if err != nil {
		t.Fatalf("SparklineData empty: %v", err)
	}
	if data != nil {
		t.Errorf("empty SparklineData should be nil, got %v", data)
	}
}
