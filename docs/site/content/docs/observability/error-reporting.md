---
title: Error Reporting
weight: 4
---

The `observe.ErrorReporter` interface captures exceptions and messages to an external error-tracking backend without coupling application code to any vendor.

## Interface

```go
type ErrorReporter interface {
    CaptureError(ctx context.Context, err error, tags map[string]string)
    CaptureMessage(ctx context.Context, msg string, level Level)
}
```

**CaptureError** records an error with optional key-value tags for grouping and filtering. The `ctx` parameter carries trace context so the error can be correlated with the active span.

**CaptureMessage** records a text message at a given severity level. Use for non-error conditions that still warrant attention (e.g., deprecation warnings, unusual but valid states).

## Usage

```go
tel.Errors.CaptureError(ctx, err, map[string]string{
    "run_id":    runID,
    "task_type": "send-email",
})

tel.Errors.CaptureMessage(ctx, "rate limit approaching threshold",
    observe.LevelWarn,
)
```

## Built-in Behavior

The internal `simple.ErrorReporter` implementation bridges errors to the active trace span:

1. When a span exists in `ctx`, `CaptureError` calls `span.RecordError(err)` and sets error status on the span
2. When no span exists, it falls back to `logger.Error()` with tags as structured fields
3. `CaptureMessage` adds a span event when a span is active, or logs at the appropriate level otherwise

This means errors are automatically visible in distributed traces without any extra wiring.

## Adapter Pattern

The noop implementation discards all captures:

```go
reporter := observe.NewNoopErrorReporter()
```

For production error tracking (Sentry, Bugsnag, Rollbar), implement the interface in a separate adapter package. The adapter is the only code that imports the vendor SDK:

```go
type SentryReporter struct {
    hub *sentry.Hub
}

func (s *SentryReporter) CaptureError(
    ctx context.Context, err error, tags map[string]string,
) {
    s.hub.WithScope(func(scope *sentry.Scope) {
        for k, v := range tags {
            scope.SetTag(k, v)
        }
        s.hub.CaptureException(err)
    })
}

func (s *SentryReporter) CaptureMessage(
    ctx context.Context, msg string, level observe.Level,
) {
    s.hub.CaptureMessage(msg)
}
```

Pass the adapter into the `observe.Telemetry` bundle alongside your other observability implementations:

```go
tel := &observe.Telemetry{
    Errors:  &SentryReporter{hub: sentry.CurrentHub()},
    Logger:  myLogger,
    Tracer:  myTracer,
    Metrics: myMetrics,
}
```

## Telemetry Bundle

All four observability interfaces are bundled into a single `observe.Telemetry` struct that is passed to component constructors:

```go
type Telemetry struct {
    Tracer  Tracer
    Logger  Logger
    Metrics Metrics
    Errors  ErrorReporter
}
```

All fields must be non-nil. Use `observe.NewNoopTelemetry()` for a safe default with all noop implementations.

## Related

- [Structured Logging](/docs/observability/structured-logging) -- leveled structured logs
- [Distributed Tracing](/docs/observability/distributed-tracing) -- span-based tracing
- [Metrics](/docs/observability/metrics) -- counters, histograms, gauges
