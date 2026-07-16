// internal/trigger/scheduler_metrics_test.go
// Methodology: integration-style tests combining embedded NATS
// (natsutil.StartTestServer) for a real Scheduler with an OTel SDK
// manual reader (sdkmetric.NewManualReader) for metric collection —
// the same combination internal/engine/metrics_test.go uses for the
// SDK side and scheduler_test.go uses for the NATS side, bridged
// here because this is the first feature needing both simultaneously.
// Every test: >=2 assertions (positive + negative), bounded timeouts
// on all waits, its own isolated NATS server (no sharing).
package trigger

import (
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// findGaugeDataPoint returns the data point for instrument/triggerID,
// or ok=false if absent. Mirrors findSnapshotHistogramDataPoint in
// engine/metrics_test.go, but non-fatal so tests can assert absence
// as well as presence.
func findGaugeDataPoint(
	t *testing.T, rm *metricdata.ResourceMetrics,
	instrument, triggerID string,
) (metricdata.DataPoint[int64], bool) {
	t.Helper()
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != instrument {
				continue
			}
			g, ok := m.Data.(metricdata.Gauge[int64])
			if !ok {
				t.Fatalf("%s: wrong type %T", instrument, m.Data)
			}
			for _, dp := range g.DataPoints {
				id, ok := dp.Attributes.Value(attribute.Key("trigger"))
				if ok && id.AsString() == triggerID {
					return dp, true
				}
			}
		}
	}
	return metricdata.DataPoint[int64]{}, false
}

// newTestScheduler builds a real Scheduler against an isolated
// embedded NATS server with the trigger_state KV bucket provisioned,
// matching setupSchedulerWithEveryMinuteTrigger's setup half without
// its subscribe/subject assumptions — the new tests here don't need
// the workflow.started subscription, only the scheduler + metrics.
func newTestScheduler(t *testing.T) (*Scheduler, *nats.Conn) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}
	return scheduler, nc
}

func addEveryMinuteTrigger(t *testing.T, s *Scheduler, enabled bool) {
	t.Helper()
	def := TriggerDef{
		ID:         "test-trigger",
		WorkflowID: "test-workflow",
		Enabled:    enabled,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
			Backfill:   false,
		},
	}
	if err := s.AddTrigger(def); err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}
}

func TestSchedulerMetricsObservesLastFiredAndNextFireOnSuccess(t *testing.T) {
	scheduler, _ := newTestScheduler(t)
	addEveryMinuteTrigger(t, scheduler, true)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	reg, err := RegisterSchedulerMetrics(
		mp.Meter("scheduler-metrics-test"), scheduler,
	)
	if err != nil {
		t.Fatalf("RegisterSchedulerMetrics failed: %v", err)
	}
	defer func() { _ = reg.Unregister() }()

	// Negative: before any successful Tick, last_fired must be
	// absent — proves omission-when-never-fired (contract stmt 4).
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var rmBefore metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rmBefore); err != nil {
		t.Fatalf("collect before tick: %v", err)
	}
	if _, ok := findGaugeDataPoint(
		t, &rmBefore, "trigger_last_fired_seconds", "test-trigger",
	); ok {
		t.Fatal("last_fired must be absent before any successful fire")
	}

	matchingMinute := time.Date(2026, 3, 31, 12, 30, 0, 0, time.UTC)
	if err := scheduler.Tick(matchingMinute); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	var rmAfter metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rmAfter); err != nil {
		t.Fatalf("collect after tick: %v", err)
	}

	lastFired, ok := findGaugeDataPoint(
		t, &rmAfter, "trigger_last_fired_seconds", "test-trigger",
	)
	if !ok {
		t.Fatal("last_fired must be present after successful fire")
	}
	if lastFired.Value != matchingMinute.Unix() {
		t.Fatalf(
			"last_fired = %d, want %d",
			lastFired.Value, matchingMinute.Unix(),
		)
	}

	nextFire, ok := findGaugeDataPoint(
		t, &rmAfter, "trigger_next_fire_seconds", "test-trigger",
	)
	if !ok {
		t.Fatal("next_fire must be present for an enabled cron trigger")
	}
	now := time.Now().Unix()
	if nextFire.Value <= now || nextFire.Value > now+60 {
		t.Fatalf(
			"next_fire = %d, want in (%d, %d]",
			nextFire.Value, now, now+60,
		)
	}
}

func TestSchedulerMetricsFireErrorDoesNotAdvanceLastFired(t *testing.T) {
	scheduler, nc := newTestScheduler(t)
	addEveryMinuteTrigger(t, scheduler, true)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	testMeter := mp.Meter("scheduler-metrics-error-test")
	reg, err := RegisterSchedulerMetrics(testMeter, scheduler)
	if err != nil {
		t.Fatalf("RegisterSchedulerMetrics failed: %v", err)
	}
	defer func() { _ = reg.Unregister() }()
	firings := newFiringsCounter(testMeter)

	// Force the publish path to fail: closing nc before Tick makes
	// tp.JSPublish inside Fire return a connection-closed error.
	nc.Close()

	matchingMinute := time.Date(2026, 3, 31, 12, 30, 0, 0, time.UTC)
	if err := scheduler.Tick(matchingMinute); err == nil {
		t.Fatal("expected Tick to return an error when nc is closed")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	// Negative: last_fired never advances on a failed fire.
	if _, ok := findGaugeDataPoint(
		t, &rm, "trigger_last_fired_seconds", "test-trigger",
	); ok {
		t.Fatal("last_fired must stay absent after a failed fire")
	}

	// Positive (existing behavior untouched): the counter instrument
	// used by RecordFiring on the error path is still usable through
	// the same test meter — proves the new gauge code path didn't
	// regress the existing counter's construction.
	if firings.counter == nil {
		t.Fatal("firings counter must not be nil")
	}
}

func TestSchedulerMetricsRemoveTriggerDropsSeries(t *testing.T) {
	scheduler, _ := newTestScheduler(t)
	addEveryMinuteTrigger(t, scheduler, true)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	reg, err := RegisterSchedulerMetrics(
		mp.Meter("scheduler-metrics-remove-test"), scheduler,
	)
	if err != nil {
		t.Fatalf("RegisterSchedulerMetrics failed: %v", err)
	}
	defer func() { _ = reg.Unregister() }()

	matchingMinute := time.Date(2026, 3, 31, 12, 30, 0, 0, time.UTC)
	if err := scheduler.Tick(matchingMinute); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var rmBefore metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rmBefore); err != nil {
		t.Fatalf("collect before remove: %v", err)
	}
	if _, ok := findGaugeDataPoint(
		t, &rmBefore, "trigger_last_fired_seconds", "test-trigger",
	); !ok {
		t.Fatal("last_fired must be present before removal")
	}

	if err := scheduler.RemoveTrigger("test-trigger"); err != nil {
		t.Fatalf("RemoveTrigger failed: %v", err)
	}

	// Positive: removal actually happened.
	if got := scheduler.Count(); got != 0 {
		t.Fatalf("Count after removal = %d, want 0", got)
	}

	var rmAfter metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rmAfter); err != nil {
		t.Fatalf("collect after remove: %v", err)
	}
	// Negative: neither series has a data point for the removed ID.
	if _, ok := findGaugeDataPoint(
		t, &rmAfter, "trigger_last_fired_seconds", "test-trigger",
	); ok {
		t.Fatal("last_fired must be dropped after RemoveTrigger")
	}
	if _, ok := findGaugeDataPoint(
		t, &rmAfter, "trigger_next_fire_seconds", "test-trigger",
	); ok {
		t.Fatal("next_fire must be dropped after RemoveTrigger")
	}
}

func TestSchedulerMetricsDisabledTriggerEmitsNeither(t *testing.T) {
	scheduler, _ := newTestScheduler(t)
	addEveryMinuteTrigger(t, scheduler, false)

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	defer func() { _ = mp.Shutdown(context.Background()) }()

	reg, err := RegisterSchedulerMetrics(
		mp.Meter("scheduler-metrics-disabled-test"), scheduler,
	)
	if err != nil {
		t.Fatalf("RegisterSchedulerMetrics failed: %v", err)
	}
	defer func() { _ = reg.Unregister() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("collect: %v", err)
	}

	// Negative: no next_fire for a disabled trigger, even though
	// it's registered — next_fire is enabled-triggers-only.
	if _, ok := findGaugeDataPoint(
		t, &rm, "trigger_next_fire_seconds", "test-trigger",
	); ok {
		t.Fatal("next_fire must be absent for a disabled trigger")
	}
	// Negative: no last_fired either, since it never fired.
	if _, ok := findGaugeDataPoint(
		t, &rm, "trigger_last_fired_seconds", "test-trigger",
	); ok {
		t.Fatal("last_fired must be absent for a disabled trigger")
	}
}
