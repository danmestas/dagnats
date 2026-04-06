// span_exporter_test.go
// Integration tests for the NATS-backed SpanExporter. Uses real
// embedded NATS with JetStream — no mocks. Validates that spans
// arrive on the correct subject as valid OTLP JSON.
package natsexporter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

func TestSpanExporter_ExportSpans(t *testing.T) {
	_, nc := startNATS(t)
	js := setupStream(t, nc)

	exporter := NewSpanExporter(js)

	res := resource.NewSchemaless(
		attribute.String("service.name", "test-svc"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithResource(res),
	)
	defer func() {
		err := tp.Shutdown(context.Background())
		if err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}()

	tracer := tp.Tracer("test")
	_, span := tracer.Start(
		context.Background(), "test-op",
		trace.WithAttributes(
			attribute.String("dagnats.run.id", "run-42"),
		),
	)
	span.End()

	// Read message from the expected subject.
	subject := "telemetry.spans.test-svc.run-42"
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

		// Verify it is valid JSON with expected span name.
		var parsed map[string]interface{}
		if err := json.Unmarshal(msg.Data(), &parsed); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		name, ok := parsed["name"]
		if !ok {
			t.Fatal("JSON missing 'name' field")
		}
		if name != "test-op" {
			t.Errorf("name = %v, want test-op", name)
		}
	}
	if count != 1 {
		t.Errorf("message count = %d, want 1", count)
	}
}

func TestSpanExporter_NoRunID_DefaultsSubject(t *testing.T) {
	_, nc := startNATS(t)
	js := setupStream(t, nc)

	exporter := NewSpanExporter(js)

	res := resource.NewSchemaless(
		attribute.String("service.name", "default-svc"),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithResource(res),
	)
	defer func() {
		err := tp.Shutdown(context.Background())
		if err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}()

	// Create span WITHOUT dagnats.run.id attribute.
	tracer := tp.Tracer("test")
	_, span := tracer.Start(
		context.Background(), "no-run-op",
	)
	span.End()

	// Should land on the no-run default subject.
	subject := "telemetry.spans.default-svc.no-run"
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
		if err := json.Unmarshal(msg.Data(), &parsed); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		name, ok := parsed["name"]
		if !ok {
			t.Fatal("JSON missing 'name' field")
		}
		if name != "no-run-op" {
			t.Errorf("name = %v, want no-run-op", name)
		}
	}
	if count != 1 {
		t.Errorf("message count = %d, want 1", count)
	}
}
