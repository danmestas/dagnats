// observe/observe_test.go

// Tests for observability interfaces and noop implementations.
// Methodology: verify noop implementations satisfy interfaces at compile time,
// that they don't panic when called, and that Logger.With returns a usable Logger.
package observe

import (
	"context"
	"testing"
)

func TestNoopLoggerSatisfiesInterface(t *testing.T) {
	var logger Logger = NewNoopLogger()
	if logger == nil {
		t.Fatal("NewNoopLogger returned nil")
	}
	logger.Info("test message", String("key", "val"))
	logger.Error("test error", nil, String("key", "val"))
	child := logger.With(String("component", "test"))
	if child == nil {
		t.Fatal("Logger.With returned nil")
	}
}

func TestNoopErrorReporterSatisfiesInterface(t *testing.T) {
	var reporter ErrorReporter = NewNoopErrorReporter()
	if reporter == nil {
		t.Fatal("NewNoopErrorReporter returned nil")
	}
	ctx := context.Background()
	reporter.CaptureError(ctx, nil, nil)
	reporter.CaptureMessage(ctx, "test", LevelInfo)
}

func TestNoopMetricsSatisfiesInterface(t *testing.T) {
	var metrics Metrics = NewNoopMetrics()
	if metrics == nil {
		t.Fatal("NewNoopMetrics returned nil")
	}
	counter := metrics.Counter("test_counter", nil)
	if counter == nil {
		t.Fatal("Counter returned nil")
	}
	counter.Inc()
	histogram := metrics.Histogram("test_hist", nil)
	if histogram == nil {
		t.Fatal("Histogram returned nil")
	}
	histogram.Observe(1.5)
	gauge := metrics.Gauge("test_gauge", nil)
	if gauge == nil {
		t.Fatal("Gauge returned nil")
	}
	gauge.Set(42.0)
}

func TestLevelString(t *testing.T) {
	tests := []struct {
		level    Level
		expected string
	}{
		{LevelDebug, "debug"},
		{LevelInfo, "info"},
		{LevelWarn, "warn"},
		{LevelError, "error"},
	}
	for _, tt := range tests {
		got := tt.level.String()
		if got != tt.expected {
			t.Fatalf("Level.String() = %q, want %q", got, tt.expected)
		}
	}
}
