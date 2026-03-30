package observe

import "context"

// noopLogger discards all log output. Used as a safe default so callers never
// need to nil-check their Logger. With returns the same instance — no allocation
// needed because fields are never stored.
type noopLogger struct{}

// NewNoopLogger returns a Logger that discards all output.
func NewNoopLogger() Logger { return &noopLogger{} }

func (n *noopLogger) Info(msg string, fields ...Field)             {}
func (n *noopLogger) Error(msg string, err error, fields ...Field) {}
func (n *noopLogger) With(fields ...Field) Logger                  { return n }

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

// noopTracer and noopSpan discard all tracing operations. They serve as safe
// defaults so callers never need to nil-check their Tracer or guard Span calls.
type noopTracer struct{}
type noopSpan struct{}

// NewNoopTracer returns a Tracer that produces no-op spans and discards all data.
func NewNoopTracer() Tracer { return &noopTracer{} }

// Start returns the context unchanged and a no-op span. No allocation is needed
// for the context because no span is propagated.
func (n *noopTracer) Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span) {
	return ctx, &noopSpan{}
}

func (n *noopSpan) End()                                          {}
func (n *noopSpan) SetStatus(code StatusCode, description string) {}
func (n *noopSpan) SetAttributes(attrs ...Attribute)              {}
func (n *noopSpan) RecordError(err error)                         {}
func (n *noopSpan) AddEvent(name string, attrs ...Attribute)      {}
