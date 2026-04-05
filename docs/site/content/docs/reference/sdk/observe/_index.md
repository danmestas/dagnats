---
title: observe
weight: 4
---

```
import "github.com/danmestas/dagnats/observe"
```

Provider-agnostic telemetry interfaces for logging, tracing, metrics, and error reporting. DagNats defines interfaces in this package and ships adapter implementations separately, following the adapter pattern.

## Key Interfaces

| Interface | Description |
|-----------|-------------|
| `Logger` | Structured logging with level-based methods (`Info`, `Warn`, `Error`, `Debug`) |
| `Tracer` | Distributed tracing: `Start(ctx, name)` returns a `(context.Context, Span)` |
| `Span` | Individual trace span with `End()`, `RecordError()`, `SetStatus()`, `SetAttributes()` |
| `SpanContext` | Optional interface on Span for extracting `TraceID()` and `SpanID()` |
| `Metrics` | Instrument factory: `Counter(name)`, `Histogram(name)`, `Gauge(name)` |
| `Counter` | Monotonic counter: `Inc()`, `Add(delta)` |
| `Histogram` | Distribution recorder: `Observe(value)` |
| `Gauge` | Point-in-time value: `Set(value)` |
| `ErrorReporter` | Error reporting interface: `CaptureError(err)`, `CaptureMessage(msg)` |

## Telemetry Bundle

The `Telemetry` struct bundles all four concerns into a single value passed through the system:

```go
type Telemetry struct {
    Logger        Logger
    Tracer        Tracer
    Metrics       Metrics
    ErrorReporter ErrorReporter
}
```

## No-op Implementations

For testing and development, the package provides no-op implementations that satisfy all interfaces without producing output:

| Function | Description |
|----------|-------------|
| `NewNoopTelemetry()` | Returns a `*Telemetry` with all no-op implementations |
| `NewNoopLogger()` | Returns a `Logger` that discards all output |
| `NewNoopTracer()` | Returns a `Tracer` with no-op spans |
| `NewNoopMetrics()` | Returns a `Metrics` that discards all recordings |

## Attribute Helpers

Helper functions for creating span and log attributes:

| Function | Returns |
|----------|---------|
| `String(key, value)` | Key-value string attribute |
| `Int(key, value)` | Key-value int attribute |
| `Bool(key, value)` | Key-value bool attribute |
| `StringAttr(key, value)` | Span attribute (string) |
| `Int64Attr(key, value)` | Span attribute (int64) |
| `BoolAttr(key, value)` | Span attribute (bool) |

## Adapter Pattern

To integrate with a real telemetry backend (e.g., OpenTelemetry, Sentry):

1. Implement the interfaces in this package
2. Wire them into a `Telemetry` struct
3. Pass to `server.New()`, `worker.NewWorker()`, or `api.NewService()`

The `internal/observe/simple` package provides a NATS JetStream-backed implementation used by the embedded server.

## Usage

```go
// Production: wire real implementations
tel := &observe.Telemetry{
    Logger:  myOtelLogger,
    Tracer:  myOtelTracer,
    Metrics: myOtelMetrics,
}

// Testing: use no-ops
tel := observe.NewNoopTelemetry()
```
