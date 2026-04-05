---
title: Structured Logging
weight: 1
---

The `observe.Logger` interface provides structured, leveled logging that is decoupled from any specific logging backend.

## Interface

```go
type Logger interface {
    Info(msg string, fields ...Field)
    Error(msg string, err error, fields ...Field)
    With(fields ...Field) Logger
}
```

**Info** emits an informational log entry with optional structured fields. **Error** emits an error-level entry with the causing error and optional fields. **With** returns a new Logger that prepends the given fields to every subsequent call -- useful for adding context like `run_id` or `step_id` once and carrying it through a call chain.

## Fields

`Field` is a typed key-value pair for structured context. Constructor helpers keep call sites concise:

```go
observe.String("run_id", runID)
observe.Int("attempt", 3)
observe.Err(err)
```

| Constructor | Value Type |
|-------------|-----------|
| `String(key, val)` | `string` |
| `Int(key, val)` | `int` |
| `Err(err)` | `error` (key is always `"error"`) |

## Levels

The `Level` enum orders severity from least to most severe:

| Level | Value | Use |
|-------|-------|-----|
| `LevelDebug` | 0 | Verbose diagnostic output |
| `LevelInfo` | 1 | Normal operational events |
| `LevelWarn` | 2 | Degraded but recoverable situations |
| `LevelError` | 3 | Failures requiring attention |

Levels are used by `ErrorReporter.CaptureMessage` to route messages to the appropriate severity. The Logger interface itself uses method names (`Info`, `Error`) rather than a level parameter.

## Contextual Chaining

`With` creates a child logger that inherits all parent fields. The parent's field slice is never mutated -- each `With` call copies the parent fields into a new slice.

```go
logger := tel.Logger.With(
    observe.String("run_id", runID),
    observe.String("workflow", "deploy"),
)
logger.Info("step started", observe.String("step_id", "build"))
// Output includes: run_id, workflow, step_id
logger.Info("step finished", observe.String("step_id", "test"))
// Output includes: run_id, workflow, step_id
```

## Adapter Pattern

DagNats ships with two Logger implementations:

**NoopLogger** -- discards all output. Used as the default when no telemetry is configured. `With` returns the same instance (no allocation).

**LogCollector** (internal) -- publishes log records as JSON to the NATS TELEMETRY stream on subject `telemetry.logs.{service}.{level}`. Each log call is independent and safe for concurrent use.

To integrate with an external backend (e.g., structured JSON to stdout, or a third-party service), implement the `Logger` interface and pass it into the `observe.Telemetry` bundle:

```go
tel := &observe.Telemetry{
    Logger:  myCustomLogger,
    Tracer:  observe.NewNoopTracer(),
    Metrics: observe.NewNoopMetrics(),
    Errors:  observe.NewNoopErrorReporter(),
}
w := worker.NewWorker(nc, tel)
```

## Related

- [Distributed Tracing](/docs/observability/distributed-tracing) -- span-based tracing
- [Metrics](/docs/observability/metrics) -- counters, histograms, gauges
- [Error Reporting](/docs/observability/error-reporting) -- exception capture
- [Telemetry Stream](/docs/observability/telemetry-stream) -- NATS-native telemetry transport
