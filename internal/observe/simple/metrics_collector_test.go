// observe/simple/metrics_collector_test.go
// Tests for MetricsCollector. Methodology: integration tests with a real embedded
// NATS server per test. Each test creates a MetricsCollector, performs a metric
// operation, and verifies the correct MetricPoint is published to the TELEMETRY
// stream. Assertions cover both the positive case (field correctness) and the
// negative space (no unexpected values).
package simple

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
)

func TestMetricsCollectorCounter(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("SetupTelemetryStream: %v", err)
	}

	sub, err := js.SubscribeSync("telemetry.metrics.>",
		nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	mc := NewMetricsCollector(js, "engine")
	counter := mc.Counter("requests_total", map[string]string{"env": "test"})
	counter.Inc()

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}

	var pt MetricPoint
	if err := json.Unmarshal(msg.Data, &pt); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if pt.Type != "counter" {
		t.Errorf("Type = %q, want counter", pt.Type)
	}
	if pt.Value != 1.0 {
		t.Errorf("Value = %f, want 1.0", pt.Value)
	}
	if pt.Name != "requests_total" {
		t.Errorf("Name = %q, want requests_total", pt.Name)
	}
	if pt.Service != "engine" {
		t.Errorf("Service = %q, want engine", pt.Service)
	}
}

func TestMetricsCollectorHistogram(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("SetupTelemetryStream: %v", err)
	}

	sub, err := js.SubscribeSync("telemetry.metrics.>",
		nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	mc := NewMetricsCollector(js, "worker")
	hist := mc.Histogram("step.duration_ms", map[string]string{"task": "llm-coder"})
	hist.Observe(42.5)

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}

	var pt MetricPoint
	if err := json.Unmarshal(msg.Data, &pt); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if pt.Type != "histogram" {
		t.Errorf("Type = %q, want histogram", pt.Type)
	}
	if pt.Value != 42.5 {
		t.Errorf("Value = %f, want 42.5", pt.Value)
	}
	if pt.Service != "worker" {
		t.Errorf("Service = %q, want worker", pt.Service)
	}
}

func TestMetricsCollectorGauge(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("SetupTelemetryStream: %v", err)
	}

	sub, err := js.SubscribeSync("telemetry.metrics.>",
		nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	mc := NewMetricsCollector(js, "api")
	gauge := mc.Gauge("active_runs", nil)
	gauge.Set(10.0)

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}

	var pt MetricPoint
	if err := json.Unmarshal(msg.Data, &pt); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	if pt.Type != "gauge" {
		t.Errorf("Type = %q, want gauge", pt.Type)
	}
	if pt.Value != 10.0 {
		t.Errorf("Value = %f, want 10.0", pt.Value)
	}
	if pt.Service != "api" {
		t.Errorf("Service = %q, want api", pt.Service)
	}
}
