// bridge/metrics_test.go
// Methodology: register the bridge's observable gauge against a manual
// metric reader (sdkmetric.NewManualReader), mutate the AckMap through
// its real Store/Delete surface, and collect after each mutation. The
// assertions compare the collected data point against AckMap.Count()
// rather than a literal, so a wrong sign or a double-count fails —
// asserting merely that a value was emitted would pass for a gauge that
// reports a constant. Precedent: internal/trigger/scheduler_metrics_test.go.
package bridge

import (
	"context"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collectAckMapSize collects once and returns the ackmap size gauge's
// value. Reports ok=false when the instrument emitted no data point, so
// a caller can distinguish "absent" from "zero" — those mean different
// things and conflating them is how a dead gauge went unnoticed.
func collectAckMapSize(
	t *testing.T, reader *sdkmetric.ManualReader,
) (int64, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != ackMapSizeMetricName {
				continue
			}
			gauge, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("%s is %T, want Gauge[int64]",
					ackMapSizeMetricName, m.Data)
			}
			if len(gauge.DataPoints) != 1 {
				t.Fatalf("%s has %d data points, want 1",
					ackMapSizeMetricName, len(gauge.DataPoints))
			}
			return gauge.DataPoints[0].Value, true
		}
	}
	return 0, false
}

func TestAckMapSizeGaugeTracksCount(t *testing.T) {
	b := &Bridge{ackMap: NewAckMap()}

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	reg, err := RegisterBridgeMetrics(mp.Meter("bridge-metrics-test"), b)
	if err != nil {
		t.Fatalf("RegisterBridgeMetrics failed: %v", err)
	}
	defer func() { _ = reg.Unregister() }()

	got, ok := collectAckMapSize(t, reader)
	if !ok {
		t.Fatal("gauge emitted no data point on an empty ackmap")
	}
	if got != 0 {
		t.Fatalf("empty ackmap reported %d, want 0", got)
	}

	// Store two, then delete one: a gauge with an inverted sign or a
	// double-count cannot satisfy both samples.
	b.ackMap.Store("task-a", &stubMsg{subject: "task.a"})
	b.ackMap.Store("task-b", &stubMsg{subject: "task.b"})
	got, _ = collectAckMapSize(t, reader)
	if want := b.ackMap.Count(); got != want {
		t.Fatalf("after 2 stores gauge = %d, Count() = %d", got, want)
	}
	if got != 2 {
		t.Fatalf("after 2 stores gauge = %d, want 2", got)
	}

	b.ackMap.Delete("task-a")
	got, _ = collectAckMapSize(t, reader)
	if want := b.ackMap.Count(); got != want {
		t.Fatalf("after delete gauge = %d, Count() = %d", got, want)
	}
	if got != 1 {
		t.Fatalf("after delete gauge = %d, want 1", got)
	}
}

// TestRegisterBridgeMetricsRejectsNil pins the contract that a caller
// cannot silently register a gauge over a nil bridge, which would
// report a constant zero and look exactly like an idle bridge — the
// failure mode this instrument had before it was wired up at all.
func TestRegisterBridgeMetricsRejectsNil(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	if _, err := RegisterBridgeMetrics(
		mp.Meter("bridge-metrics-nil-test"), nil,
	); err == nil {
		t.Fatal("expected an error for a nil bridge, got nil")
	}
	if _, err := RegisterBridgeMetrics(nil, &Bridge{
		ackMap: NewAckMap(),
	}); err == nil {
		t.Fatal("expected an error for a nil meter, got nil")
	}
}
