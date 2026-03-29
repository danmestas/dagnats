package observe

import "context"

// noopLogger discards all log output. Used as a safe default so callers never
// need to nil-check their Logger. With returns the same instance — no allocation
// needed because fields are never stored.
type noopLogger struct{}

// NewNoopLogger returns a Logger that discards all output.
func NewNoopLogger() Logger { return &noopLogger{} }

func (n *noopLogger) Info(msg string, fields ...Field)               {}
func (n *noopLogger) Error(msg string, err error, fields ...Field)   {}
func (n *noopLogger) With(fields ...Field) Logger                    { return n }

// noopErrorReporter discards all error captures. Used as the default when no
// external error-tracking adapter has been configured.
type noopErrorReporter struct{}

// NewNoopErrorReporter returns an ErrorReporter that discards all captures.
func NewNoopErrorReporter() ErrorReporter { return &noopErrorReporter{} }

func (n *noopErrorReporter) CaptureError(ctx context.Context, err error, tags map[string]string) {}
func (n *noopErrorReporter) CaptureMessage(ctx context.Context, msg string, level Level)         {}

// noopMetrics and its instrument types discard all observations. Each factory
// call allocates a fresh instrument so callers can safely compare pointers to nil.
type noopMetrics struct{}
type noopCounter struct{}
type noopHistogram struct{}
type noopGauge struct{}

// NewNoopMetrics returns a Metrics factory that produces no-op instruments.
func NewNoopMetrics() Metrics { return &noopMetrics{} }

func (n *noopMetrics) Counter(name string, tags map[string]string) Counter {
	return &noopCounter{}
}
func (n *noopMetrics) Histogram(name string, tags map[string]string) Histogram {
	return &noopHistogram{}
}
func (n *noopMetrics) Gauge(name string, tags map[string]string) Gauge {
	return &noopGauge{}
}

func (n *noopCounter) Inc()              {}
func (n *noopCounter) Add(float64)       {}
func (n *noopHistogram) Observe(float64) {}
func (n *noopGauge) Set(float64)         {}
func (n *noopGauge) Inc()                {}
func (n *noopGauge) Dec()                {}
