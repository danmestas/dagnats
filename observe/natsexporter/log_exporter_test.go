// log_exporter_test.go
// Integration tests for the NATS-backed LogExporter. Uses real
// embedded NATS with JetStream — no mocks. Validates that log
// records arrive on the correct subject as valid JSON.
package natsexporter

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"

	"go.opentelemetry.io/otel/attribute"
)

func TestLogExporter_Export(t *testing.T) {
	_, nc := startNATS(t)
	js := setupStream(t, nc)

	exporter := NewLogExporter(js, "log-test-svc")

	res := resource.NewSchemaless(
		attribute.String("service.name", "log-test-svc"),
	)

	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(
			sdklog.NewSimpleProcessor(exporter),
		),
		sdklog.WithResource(res),
	)
	defer func() {
		err := provider.Shutdown(context.Background())
		if err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}()

	logger := provider.Logger("test-logger")

	// Emit a log record with severity and body.
	var rec log.Record
	rec.SetSeverity(log.SeverityError)
	rec.SetSeverityText("ERROR")
	rec.SetBody(log.StringValue("something went wrong"))
	rec.SetTimestamp(time.Now())
	rec.AddAttributes(
		log.String("component", "engine"),
	)

	logger.Emit(context.Background(), rec)

	// Read message from the expected subject.
	subject := "telemetry.logs.log-test-svc.error"
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

		// Verify the body field contains our log message.
		body, ok := parsed["body"]
		if !ok {
			t.Fatal("JSON missing 'body' field")
		}
		if body != "something went wrong" {
			t.Errorf(
				"body = %v, want 'something went wrong'",
				body,
			)
		}

		// Verify severity is present and correct.
		sev, ok := parsed["severity"]
		if !ok {
			t.Fatal("JSON missing 'severity' field")
		}
		if sev != "error" {
			t.Errorf("severity = %v, want 'error'", sev)
		}

		// Verify serviceName is present.
		svc, ok := parsed["serviceName"]
		if !ok {
			t.Fatal("JSON missing 'serviceName' field")
		}
		if svc != "log-test-svc" {
			t.Errorf(
				"serviceName = %v, want 'log-test-svc'",
				svc,
			)
		}
	}

	if count != 1 {
		t.Errorf("message count = %d, want 1", count)
	}
}

func TestLogExporter_DefaultSeverity(t *testing.T) {
	_, nc := startNATS(t)
	js := setupStream(t, nc)

	exporter := NewLogExporter(js, "default-sev-svc")

	res := resource.NewSchemaless(
		attribute.String(
			"service.name", "default-sev-svc",
		),
	)

	provider := sdklog.NewLoggerProvider(
		sdklog.WithProcessor(
			sdklog.NewSimpleProcessor(exporter),
		),
		sdklog.WithResource(res),
	)
	defer func() {
		err := provider.Shutdown(context.Background())
		if err != nil {
			t.Errorf("shutdown: %v", err)
		}
	}()

	logger := provider.Logger("test-logger")

	// Emit without setting severity text — should default
	// to "info" in the subject.
	var rec log.Record
	rec.SetBody(log.StringValue("no severity set"))
	rec.SetTimestamp(time.Now())

	logger.Emit(context.Background(), rec)

	subject := "telemetry.logs.default-sev-svc.info"
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

		body, ok := parsed["body"]
		if !ok {
			t.Fatal("JSON missing 'body' field")
		}
		if body != "no severity set" {
			t.Errorf(
				"body = %v, want 'no severity set'", body,
			)
		}
	}

	if count != 1 {
		t.Errorf("message count = %d, want 1", count)
	}
}
