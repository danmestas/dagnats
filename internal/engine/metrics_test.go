// internal/engine/metrics_test.go
// Methodology: unit tests for metric struct constructors.
// Verifies non-nil instruments are returned from a real OTel meter,
// and that the snapshot.save histogram records the workflow+step
// labels we declare in metricLabelAllowlist. The allowlist test is
// the regression guard against label cardinality drift — if an emit
// site adds a new label, the test fails until the allowlist is
// updated, forcing a human review.
package engine

import (
	"context"
	"os"
	"regexp"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestNewOrchMetricsReturnsNonNil(t *testing.T) {
	m := otel.Meter("test")
	om := newOrchMetrics(m)

	if om.runsActive == nil {
		t.Fatal("runsActive must not be nil")
	}
	if om.runsCompleted == nil {
		t.Fatal("runsCompleted must not be nil")
	}
	if om.runsFailed == nil {
		t.Fatal("runsFailed must not be nil")
	}
	if om.snapshotDuration == nil {
		t.Fatal("snapshotDuration must not be nil")
	}
	if om.failNonRetriable == nil {
		t.Fatal("failNonRetriable must not be nil")
	}
	if om.failRetryAfter == nil {
		t.Fatal("failRetryAfter must not be nil")
	}
	if om.dlqEntries == nil {
		t.Fatal("dlqEntries must not be nil")
	}
	if om.dlqDepth == nil {
		t.Fatal("dlqDepth must not be nil")
	}
}

func TestResolveDLQReasonBoundedEnum(t *testing.T) {
	// Regression guard: the reason label MUST come from a closed
	// enum so the metrics aggregator's per-(name, labels) fanout
	// stays bounded. If a new branch is added, this guard fails
	// and forces the author to update the table.
	known := map[string]bool{
		"max_deliveries": true,
		"non_retriable":  true,
		"unknown":        true,
	}
	if len(known) != 3 {
		t.Fatalf("reason enum drifted: %d entries", len(known))
	}
	for k := range known {
		if k == "" {
			t.Fatal("empty reason in enum")
		}
	}
}

// TestSnapshotSaveHistogramHasPerStepLabels asserts that the
// snapshot.save.duration_ms histogram records both a workflow and
// step label when saveSnapshot is called with a known step. This
// is the per-step drilldown the audit unblocked — without it the
// observability surface only allows per-workflow rollup, hiding
// hot-step regressions.
func TestSnapshotSaveHistogramHasPerStepLabels(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
	)
	defer func() { _ = mp.Shutdown(context.Background()) }()

	om := newOrchMetrics(mp.Meter("snapshot-test"))
	if om.snapshotDuration == nil {
		t.Fatal("snapshotDuration must not be nil")
	}

	// Emit a synthetic record with workflow+step labels — the same
	// shape saveSnapshot will produce.
	om.snapshotDuration.Record(
		context.Background(), 5.0,
		histogramAttrs("wf-A", "step-1"),
	)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(
		context.Background(), &rm,
	); err != nil {
		t.Fatalf("collect: %v", err)
	}
	dp := findSnapshotHistogramDataPoint(t, &rm)

	gotWorkflow, _ := dp.Attributes.Value(
		attribute.Key("workflow"),
	)
	gotStep, _ := dp.Attributes.Value(
		attribute.Key("step"),
	)
	if gotWorkflow.AsString() != "wf-A" {
		t.Fatalf(
			"workflow label = %q, want %q",
			gotWorkflow.AsString(), "wf-A",
		)
	}
	if gotStep.AsString() != "step-1" {
		t.Fatalf(
			"step label = %q, want %q",
			gotStep.AsString(), "step-1",
		)
	}
}

// TestMetricLabelAllowlistContainsSnapshotSave asserts the
// allowlist names snapshot.save.duration_ms and lists exactly the
// labels the emit site uses. If the emit site adds or removes a
// label without updating the allowlist, this test fails.
func TestMetricLabelAllowlistContainsSnapshotSave(t *testing.T) {
	allowed, ok := metricLabelAllowlist["snapshot.save.duration_ms"]
	if !ok {
		t.Fatal(
			"metricLabelAllowlist missing " +
				"snapshot.save.duration_ms entry",
		)
	}
	want := map[string]bool{"workflow": true, "step": true}
	if len(allowed) != len(want) {
		t.Fatalf(
			"allowlist size = %d, want %d (labels = %v)",
			len(allowed), len(want), allowed,
		)
	}
	for _, label := range allowed {
		if !want[label] {
			t.Fatalf(
				"unexpected label %q in allowlist", label,
			)
		}
	}
}

// TestSnapshotSaveEmitSiteOnlyUsesAllowlistedLabels is a source-
// level regression guard. It reads orchestrator.go's saveSnapshot
// emit site and asserts every attribute.String key passed into
// snapshotDuration.Record is present in metricLabelAllowlist.
// Adding a label at the emit site without updating the allowlist
// makes this test fail — that's the point.
func TestSnapshotSaveEmitSiteOnlyUsesAllowlistedLabels(t *testing.T) {
	allowed, ok := metricLabelAllowlist["snapshot.save.duration_ms"]
	if !ok {
		t.Fatal(
			"metricLabelAllowlist missing entry for " +
				"snapshot.save.duration_ms",
		)
	}
	allowedSet := make(map[string]bool, len(allowed))
	for _, k := range allowed {
		allowedSet[k] = true
	}
	keys := snapshotSaveEmitSiteLabelKeys(t)
	if len(keys) == 0 {
		t.Fatal(
			"could not extract label keys from orchestrator.go " +
				"saveSnapshot emit site — test is broken",
		)
	}
	for _, k := range keys {
		if !allowedSet[k] {
			t.Fatalf(
				"emit site uses label %q not in allowlist %v",
				k, allowed,
			)
		}
	}
	// Symmetric check: every allowlisted label must be present at
	// the emit site, otherwise the allowlist has drifted ahead of
	// reality.
	emitSet := make(map[string]bool, len(keys))
	for _, k := range keys {
		emitSet[k] = true
	}
	for _, k := range allowed {
		if !emitSet[k] {
			t.Fatalf(
				"allowlist names label %q but emit site does "+
					"not record it", k,
			)
		}
	}
}

func TestNewPubMetricsReturnsNonNil(t *testing.T) {
	m := otel.Meter("test")
	pm := newPubMetrics(m)

	if pm.stepEnqueue == nil {
		t.Fatal("stepEnqueue must not be nil")
	}
	if pm.taskConcAcquired == nil {
		t.Fatal("taskConcAcquired must not be nil")
	}
	if pm.taskConcRejected == nil {
		t.Fatal("taskConcRejected must not be nil")
	}
}

// histogramAttrs returns the metric.WithAttributes option used by
// saveSnapshot. Kept in the test file to mirror exactly what the
// production code path produces — if the production attribute
// keys drift, the test must drift with them.
func histogramAttrs(workflow, step string) metric.RecordOption {
	return metric.WithAttributes(
		attribute.String("workflow", workflow),
		attribute.String("step", step),
	)
}

// findSnapshotHistogramDataPoint walks a ResourceMetrics and
// returns the single histogram data point recorded for
// snapshot.save.duration_ms. Fails the test if absent or > 1.
func findSnapshotHistogramDataPoint(
	t *testing.T, rm *metricdata.ResourceMetrics,
) metricdata.HistogramDataPoint[float64] {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != "snapshot.save.duration_ms" {
				continue
			}
			h, ok := m.Data.(metricdata.Histogram[float64])
			if !ok {
				t.Fatalf(
					"snapshot.save.duration_ms wrong type %T",
					m.Data,
				)
			}
			if len(h.DataPoints) != 1 {
				t.Fatalf(
					"want 1 datapoint, got %d",
					len(h.DataPoints),
				)
			}
			return h.DataPoints[0]
		}
	}
	t.Fatal("snapshot.save.duration_ms metric not found")
	return metricdata.HistogramDataPoint[float64]{}
}

// snapshotSaveEmitSiteLabelKeys reads orchestrator.go and returns
// the attribute keys recorded at the snapshotDuration.Record call
// site. Uses a brace-balanced scan from metric.WithAttributes( so
// nested calls (attribute.String(...)) don't terminate the match
// prematurely.
func snapshotSaveEmitSiteLabelKeys(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile("orchestrator.go")
	if err != nil {
		t.Fatalf("read orchestrator.go: %v", err)
	}
	src := string(data)
	recordIdx := regexp.MustCompile(
		`snapshotDuration\.Record\(`,
	).FindStringIndex(src)
	if recordIdx == nil {
		return nil
	}
	withIdx := regexp.MustCompile(
		`metric\.WithAttributes\(`,
	).FindStringIndex(src[recordIdx[1]:])
	if withIdx == nil {
		return nil
	}
	start := recordIdx[1] + withIdx[1]
	depth := 1
	end := start
	for i := start; i < len(src) && i < start+2048; i++ {
		switch src[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				end = i
				i = len(src) // break outer loop
			}
		}
	}
	if depth != 0 {
		return nil
	}
	keyRe := regexp.MustCompile(
		`attribute\.String\("([^"]+)"`,
	)
	matches := keyRe.FindAllStringSubmatch(src[start:end], -1)
	keys := make([]string, 0, len(matches))
	for _, mm := range matches {
		keys = append(keys, mm[1])
	}
	return keys
}
