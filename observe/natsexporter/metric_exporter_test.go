// metric_exporter_test.go
// Integration tests for the NATS-backed MetricExporter. Uses
// real embedded NATS with JetStream — no mocks. Validates that
// metric data arrives on the correct subject as valid JSON.
package natsexporter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	otelmetric "go.opentelemetry.io/otel/metric"
)

func TestMetricExporter_Export(t *testing.T) {
	_, nc := startNATS(t)
	js := setupStream(t, nc)

	exporter := NewMetricExporter(js)

	res := resource.NewSchemaless(
		attribute.String("service.name", "metric-test-svc"),
	)

	// PeriodicReader wraps our exporter; ForceFlush triggers
	// immediate collection and export without waiting.
	reader := metric.NewPeriodicReader(exporter)

	mp := metric.NewMeterProvider(
		metric.WithReader(reader),
		metric.WithResource(res),
	)
	defer func() {
		err := mp.Shutdown(context.Background())
		if err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}()

	meter := mp.Meter("test-meter")
	counter, err := meter.Int64Counter(
		"test_requests_total",
		otelmetric.WithDescription("test counter"),
	)
	if err != nil {
		t.Fatalf("create counter: %v", err)
	}

	counter.Add(context.Background(), 42)

	// Force collection — sends data through our exporter.
	err = reader.ForceFlush(context.Background())
	if err != nil {
		t.Fatalf("force flush: %v", err)
	}

	subject := "telemetry.metrics.metric-test-svc." +
		"test_requests_total"
	cons, err := js.CreateOrUpdateConsumer(
		context.Background(), "TELEMETRY",
		jetstream.ConsumerConfig{
			FilterSubject: subject,
		},
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	msgs, err := cons.Fetch(
		1, jetstream.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	count := 0
	for msg := range msgs.Messages() {
		count++

		var parsed map[string]interface{}
		if err := json.Unmarshal(
			msg.Data(), &parsed,
		); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}

		// Verify metric name.
		name, ok := parsed["name"]
		if !ok {
			t.Fatal("JSON missing 'name' field")
		}
		if name != "test_requests_total" {
			t.Errorf(
				"name = %v, want 'test_requests_total'",
				name,
			)
		}

		// Verify serviceName.
		svc, ok := parsed["serviceName"]
		if !ok {
			t.Fatal("JSON missing 'serviceName' field")
		}
		if svc != "metric-test-svc" {
			t.Errorf(
				"serviceName = %v, want 'metric-test-svc'",
				svc,
			)
		}

		// Verify data field is present (contains aggregation).
		_, ok = parsed["data"]
		if !ok {
			t.Fatal("JSON missing 'data' field")
		}
	}

	if count != 1 {
		t.Errorf("message count = %d, want 1", count)
	}
}
