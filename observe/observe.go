package observe

import "context"

// Level represents the severity of an observability event.
// Ordered from least to most severe; only values in levelStrings are valid.
type Level int

const (
	LevelDebug Level = iota
	LevelInfo
	LevelWarn
	LevelError
)

var levelStrings = [...]string{"debug", "info", "warn", "error"}

// String returns the lowercase name of the level. Panics on unknown levels
// to surface programmer errors at the call site rather than silently corrupting output.
func (l Level) String() string {
	if int(l) < len(levelStrings) {
		return levelStrings[l]
	}
	panic("unknown Level")
}

// Field is a typed key-value pair for structured log and error report context.
type Field struct {
	Key   string
	Value any
}

// String constructs a Field with a string value.
func String(key, val string) Field { return Field{Key: key, Value: val} }

// Int constructs a Field with an int value.
func Int(key string, val int) Field { return Field{Key: key, Value: val} }

// Err constructs a Field that captures an error under the conventional "error" key.
func Err(err error) Field { return Field{Key: "error", Value: err} }

// Logger is the structured logging contract. Implementations must be safe for
// concurrent use. With returns a new Logger that prepends the given fields to
// every subsequent log call — it must never return nil.
type Logger interface {
	Info(msg string, fields ...Field)
	Error(msg string, err error, fields ...Field)
	With(fields ...Field) Logger
}

// ErrorReporter captures exceptions and messages to an external error-tracking
// backend. The concrete backend (e.g. Sentry) lives in a separate adapter package;
// this interface keeps engine/worker/api independent of any vendor import.
type ErrorReporter interface {
	CaptureError(ctx context.Context, err error, tags map[string]string)
	CaptureMessage(ctx context.Context, msg string, level Level)
}

// Counter is a monotonically increasing metric instrument.
type Counter interface {
	Inc()
	Add(delta float64)
}

// Histogram records observations in configurable buckets (e.g. latency, payload size).
type Histogram interface {
	Observe(value float64)
}

// Gauge is a metric that can go up or down (e.g. queue depth, active goroutines).
type Gauge interface {
	Set(value float64)
	Inc()
	Dec()
}

// Metrics is the factory for named metric instruments. Tags are label key-value
// pairs that the underlying backend attaches to each observation. The concrete
// backend (e.g. Prometheus, OTel) lives in a separate adapter package.
type Metrics interface {
	Counter(name string, tags map[string]string) Counter
	Histogram(name string, tags map[string]string) Histogram
	Gauge(name string, tags map[string]string) Gauge
}

// Telemetry bundles all observability interfaces. Passed as a single
// argument to component constructors instead of separate parameters.
// All fields must be non-nil — use Noop constructors for safe defaults.
type Telemetry struct {
	Tracer  Tracer
	Logger  Logger
	Metrics Metrics
	Errors  ErrorReporter
}

// NewNoopTelemetry returns a Telemetry with all noop implementations.
// Safe default for tests and environments where telemetry is not needed.
func NewNoopTelemetry() *Telemetry {
	return &Telemetry{
		Tracer:  NewNoopTracer(),
		Logger:  NewNoopLogger(),
		Metrics: NewNoopMetrics(),
		Errors:  NewNoopErrorReporter(),
	}
}
