// observe/simple/error_reporter_test.go
// Tests for ErrorReporter. Methodology: verify span-aware error capture
// and logger fallback when no active span. Asserts both code paths.
package simple

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestErrorReporterWithActiveSpan(t *testing.T) {
	records := make(chan SpanRecord, 10)
	span := &LiveSpan{
		traceID:   "trace-1",
		spanID:    "span-1",
		name:      "test.op",
		service:   "engine",
		kind:      "internal",
		startTime: time.Now(),
		records:   records,
		metrics:   observe.NewNoopMetrics(),
	}
	ctx := context.WithValue(
		context.Background(), spanContextKey{}, span)
	reporter := NewErrorReporter(
		observe.NewNoopTracer(), observe.NewNoopLogger())
	reporter.CaptureError(ctx, errors.New("boom"),
		map[string]string{"step": "s1"})
	span.End()

	select {
	case rec := <-records:
		if rec.Error != "boom" {
			t.Fatalf("Error = %q, want boom", rec.Error)
		}
		if rec.Status != "error" {
			t.Fatalf("Status = %q, want error", rec.Status)
		}
	case <-time.After(time.Second):
		t.Fatal("no SpanRecord received")
	}
}

func TestErrorReporterWithoutSpanFallsBack(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	logger := NewLogCollector(js, "test-svc")
	reporter := NewErrorReporter(
		observe.NewNoopTracer(), logger)

	jsOld, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	sub, err := jsOld.SubscribeSync(
		"telemetry.logs.>", nats.DeliverAll(),
	)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	reporter.CaptureError(context.Background(),
		errors.New("no-span-error"),
		map[string]string{"k": "v"})

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("no log received: %v", err)
	}
	var rec LogRecord
	if err := json.Unmarshal(msg.Data, &rec); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if rec.Level != "error" {
		t.Fatalf("Level = %q, want error", rec.Level)
	}
	if rec.Error != "no-span-error" {
		t.Fatalf("Error = %q, want no-span-error", rec.Error)
	}
}
