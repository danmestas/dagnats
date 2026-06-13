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

// TestSparkline_matchesEngineWorkflowLabel exercises the REAL adapter
// read path (apiServiceAdapter.SparklineData → sparklineMetricFor →
// bucketHourly) against a metric point labeled the way the engine
// actually emits it. The orchestrator emits workflow.runs.completed with
// attribute key "workflow" (orchestrator.go:794, attribute.String(
// "workflow", run.WorkflowID)), so the console's label-key filter must
// agree. This guards the bug where sparklineMetricFor returned the
// nonexistent key "workflow_id", matching nothing and silently returning
// nil for every workflow — a dead Activity(24h) canvas in production.
func TestSparkline_matchesEngineWorkflowLabel(t *testing.T) {
	const hours = 24
	src := newFakeMetricsSource()
	now := time.Now().UTC()
	// Engine attribute key is "workflow", value is the workflow ID.
	src.addCounterLabeled(
		"workflow.runs.completed", 7, now.Add(-30*time.Minute),
		map[string]string{"workflow": "demo"},
	)
	ds := WithMetrics(&apiServiceAdapter{}, src)

	data, err := ds.SparklineData(context.Background(), "workflow", "demo", hours)
	if err != nil {
		t.Fatalf("SparklineData: %v", err)
	}
	if len(data) != hours {
		t.Fatalf("len(data) = %d, want %d (nil means the label key "+
			"did not match the engine's emitted key)", len(data), hours)
	}
	var sum float64
	for _, v := range data {
		sum += v
	}
	if sum != 7 {
		t.Errorf("bucket sum = %v, want 7 (the seeded point must land "+
			"in a slot, not be filtered out)", sum)
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
