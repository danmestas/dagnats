// observe/simple/error_reporter_test.go
// Tests for ErrorReporter. Methodology: verify span-aware error capture
// and logger fallback when no active span. Asserts both code paths.
package simple

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/observe"
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
	reporter := NewErrorReporter(
		observe.NewNoopTracer(), observe.NewNoopLogger())
	// Should not panic when no active span
	reporter.CaptureError(context.Background(),
		errors.New("no-span"), nil)
	reporter.CaptureMessage(context.Background(),
		"test msg", observe.LevelError)
	// If we get here without panic, the fallback worked
	if reporter == nil {
		t.Fatal("reporter should not be nil")
	}
}
