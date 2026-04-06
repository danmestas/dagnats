---
title: Distributed Tracing
weight: 2
---

The `observe.Tracer` interface creates spans that track units of work across process and message boundaries using W3C trace context propagation.

## Interface

```go
type Tracer interface {
    Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span)
}
```

`Start` creates a new span, stores it in the returned context, and returns both. If a parent span exists in `ctx`, the child inherits its trace ID and links to the parent. The returned context carries the new span so downstream calls can continue the trace.

## Span

```go
type Span interface {
    End()
    SetStatus(code StatusCode, description string)
    SetAttributes(attrs ...Attribute)
    RecordError(err error)
    AddEvent(name string, attrs ...Attribute)
}
```

**End** must be called exactly once per span. **SetStatus** marks the span as OK or Error. **RecordError** captures an error message and sets error status. **AddEvent** records a timestamped event within the span's lifetime.

## Span Options

Options control span creation:

```go
ctx, span := tel.Tracer.Start(ctx, "worker.executeTask",
    observe.WithSpanKind(observe.SpanKindServer),
    observe.WithAttributes(
        observe.StringAttr("run_id", runID),
        observe.StringAttr("step_id", stepID),
    ),
)
defer span.End()
```

| Option | Description |
|--------|-------------|
| `WithSpanKind(kind)` | Sets the span kind: `SpanKindInternal`, `SpanKindServer`, `SpanKindClient` |
| `WithAttributes(attrs...)` | Attaches key-value pairs at creation time |

## Attributes

Attribute constructors mirror the Field constructors used by Logger:

| Constructor | Value Type |
|-------------|-----------|
| `StringAttr(key, val)` | `string` |
| `Int64Attr(key, val)` | `int64` |
| `Float64Attr(key, val)` | `float64` |
| `BoolAttr(key, val)` | `bool` |

## W3C Trace Context Propagation

DagNats propagates trace context across NATS messages using the W3C `traceparent` header format: `00-{traceID}-{spanID}-{flags}`.

On the **producer side**, the engine writes `traceparent` to the NATS message header and the event's `TraceParent` field (dual-write for compatibility with different consumers).

On the **consumer side**, the worker extracts `traceparent` from the message header, parses the trace and span IDs, and stores them in the context via `observe.ContextWithParentInfo`. The tracer implementation then links child spans to the remote parent.

```go
// Extraction (worker internal)
info, ok := observe.ParentInfoFromContext(ctx)
// info.TraceID, info.SpanID available for linking
```

## Adapter Pattern

DagNats does not ship a production tracing backend. The internal `simple.TraceCollector` publishes spans to the NATS TELEMETRY stream for development and debugging. For production, implement the `Tracer` interface with your preferred backend (OpenTelemetry, Datadog, etc.):

```go
type OTelTracerAdapter struct {
    tracer trace.Tracer // from go.opentelemetry.io/otel
}

func (a *OTelTracerAdapter) Start(
    ctx context.Context, name string, opts ...observe.SpanOption,
) (context.Context, observe.Span) {
    // Convert observe.SpanOption to OTel options
    // Return wrapped span that implements observe.Span
}
```

Pass the adapter into the `observe.Telemetry` bundle. All DagNats components (engine, worker, bridge) use the same `Tracer` interface, so swapping backends requires no code changes outside the adapter.

## Built-in OTLP Bridge

DagNats includes a built-in OTLP/HTTP exporter that bridges the NATS `TELEMETRY` stream to any OTLP-compatible backend (SigNoz, Jaeger, Grafana Tempo, etc.). The bridge consumes spans, logs, and metrics from the telemetry stream and POSTs them to the standard OTLP endpoints (`/v1/traces`, `/v1/logs`, `/v1/metrics`).

Run it as a standalone process:

```bash
dagnats otlp-bridge --endpoint=http://localhost:4318
```

Or set the endpoint via environment variable:

```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
dagnats otlp-bridge
```

The bridge creates a durable consumer (`otlp-bridge`) on the `TELEMETRY` stream with explicit ack policy. Messages are fetched in batches (default: 100) and flushed on a configurable interval (default: 5s). Failed exports are retried via `NakWithDelay` up to 4 delivery attempts.

| Setting | Default | Description |
|---------|---------|-------------|
| Batch size | 100 | Messages per fetch |
| Flush interval | 5s | Max wait between flushes |
| Max deliveries | 4 | Retry limit before dead-letter |
| Service name | `dagnats` | `service.name` resource attribute |

The bridge routes messages by subject prefix: `telemetry.spans.*` to traces, `telemetry.logs.*` to logs, and `telemetry.metrics.*` to metrics. Custom headers can be configured programmatically via `BridgeConfig.Headers` for backends that require authentication tokens.

## SpanContext

Span implementations may optionally implement the `SpanContext` interface to expose trace and span IDs for cross-process propagation:

```go
type SpanContext interface {
    TraceID() string
    SpanID() string
}
```

Callers use type assertion: `if sc, ok := span.(observe.SpanContext); ok { ... }`

## Related

- [Structured Logging](/docs/observability/structured-logging) -- leveled structured logs
- [Metrics](/docs/observability/metrics) -- counters, histograms, gauges
- [Telemetry Stream](/docs/observability/telemetry-stream) -- NATS-native telemetry transport
