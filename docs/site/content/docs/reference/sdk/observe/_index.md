---
title: observe
weight: 4
---

```
import "github.com/danmestas/dagnats/observe"
```

OpenTelemetry setup and trace-context propagation for DagNats processes. The engine, workers and bridge use the global OpenTelemetry providers; this package installs real ones and carries trace context across NATS messages.

## Key Functions

| Function | Description |
|----------|-------------|
| `InitTelemetry(ctx, cfg)` | Creates and registers the tracer, meter and logger providers, exporting spans, metrics and logs to NATS JetStream and, when `OTLPEndpoint` is set, over OTLP/HTTP. Returns a shutdown function. |
| `EnsureDefaultPropagator()` | Installs the W3C trace-context and baggage propagator unless one is already set |
| `InjectTraceContext`, `InjectTraceContextHeader` | Write trace context into an event and NATS headers |
| `ExtractTraceContext`, `ExtractTraceContextHeader`, `ExtractTraceContextRaw` | Rebuild a context from a NATS message or headers |
| `TraceContextFromTask(task)` | Rebuild a context from a `protocol.TaskPayload` (its `traceparent` and `tracestate`) |

## Config

| Field | Description |
|-------|-------------|
| `ServiceName` | Required. Identifies the process in telemetry data. |
| `NATSConn` | Required. Connection used for the NATS-backed exporters. The caller owns it. |
| `OTLPEndpoint` | When non-empty, turns on OTLP export. The endpoint, headers and protocol come from the standard `OTEL_EXPORTER_OTLP_*` environment variables. |
| `Resource` | Extra resource attributes |

`InitTelemetry` panics if `NATSConn` is nil or `ServiceName` is empty.

## Usage

```go
shutdown, err := observe.InitTelemetry(ctx, observe.Config{
    ServiceName: "my-worker",
    NATSConn:    nc,
})
if err != nil {
    log.Fatal(err)
}
defer shutdown(context.Background())

w := worker.NewWorker(nc)
```

Workers need no telemetry argument. Once `InitTelemetry` has run, they pick up the global providers.
