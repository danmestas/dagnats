---
title: Distributed Tracing
weight: 2
---

DagNats traces with OpenTelemetry. Spans from the engine, API, bridge and workers are written to the `TELEMETRY` stream, optionally exported over OTLP, and linked across processes by W3C trace context carried in NATS message headers.

## Setup

`observe.InitTelemetry` registers the global OpenTelemetry tracer, meter and logger providers for a process. `dagnats serve`, `dagnats-engine` and `dagnats-api` call it for you. A worker you write calls it once at startup:

```go
shutdown, err := observe.InitTelemetry(ctx, observe.Config{
    ServiceName:  "my-worker",
    NATSConn:     nc,
    OTLPEndpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
})
if err != nil {
    panic(err)
}
defer shutdown(ctx)
```

`ServiceName` and `NATSConn` are required. `InitTelemetry` panics if either is empty. Spans always go to NATS. When `OTLPEndpoint` is non-empty, they also go over OTLP/HTTP. The value only switches the OTLP exporters on; where they send is read by the OpenTelemetry SDK from its standard environment variables (`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_EXPORTER_OTLP_PROTOCOL` and the per-signal variants). `Config.Resource` adds resource attributes, and `OTEL_RESOURCE_ATTRIBUTES` and `OTEL_SERVICE_NAME` are honored. Each resource also carries the service version, host name, OS, architecture and process ID.

After that, use the standard OpenTelemetry API for your own spans:

```go
ctx, span := otel.Tracer("my-worker").Start(ctx, "fetch-document")
defer span.End()
span.SetAttributes(attribute.String("run_id", runID))
```

Set `run_id` (or `dagnats.run.id`) on a span so it is filed under that run: the span is published on `telemetry.spans.{service}.{run_id}`, and spans with neither attribute go to `telemetry.spans.{service}.no-run`.

## What gets traced

The engine, API, bridge, trigger service and worker each use their own tracer (`dagnats/engine`, `dagnats/api`, `dagnats/bridge`, `dagnats/trigger`, `dagnats/worker`). Spans cover event handling in the orchestrator, task enqueue, task execution in the worker, the worker's `complete`, `fail`, `failPermanent`, `failRetryAfter` and `continue` calls, bridge requests, and trigger fires. Errors are recorded on the span with `RecordError` and an error status.

## W3C trace context over NATS

Trace context travels in the standard `traceparent` (and `tracestate`) headers on NATS messages.

On the **producer side**, the engine and the API write `traceparent` to the message headers and also to the event's `TraceParent` and `TraceState` fields. The event copy keeps the trace linkable when a message is replayed from `WORKFLOW_HISTORY`.

On the **consumer side**, `observe.ExtractTraceContext` reads the headers first and falls back to the event's `TraceParent`. The worker uses the result as the parent of its execution span. Requests to the micro endpoints (`api.>`) are handled the same way, from the request headers.

When you publish to dagnats from your own code, put the context on the message with `observe.InjectTraceContext(ctx, msg, nil)`.

## Viewing traces

```bash
# The span tree for one run
dagnats trace <run-id>

# Find traces
dagnats trace search --service=dagnats --status=error --since=1h --limit=20
```

`trace <run-id>` reads the run's spans from the `TELEMETRY` stream, so it works without any external backend, up to the stream's 7-day retention. Both commands take `--json`.

## Exporting to a backend

Set `OTEL_EXPORTER_OTLP_ENDPOINT` on `dagnats serve` (or the `otlp_endpoint` config key). Each process then exports its own traces, metrics and logs over OTLP/HTTP while still writing to NATS. Any OTLP/HTTP backend works: SigNoz, Jaeger, Grafana Tempo. There is no separate bridge process. Export failures are handled by the OpenTelemetry SDK and do not affect workflow execution.

```bash
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 dagnats serve
```

For anything else, consume the `TELEMETRY` stream directly. See [Telemetry Stream]({{< ref "/docs/observability/telemetry-stream" >}}).

## Related

- [Structured Logging](/docs/observability/structured-logging) -- log records that carry trace IDs
- [Metrics](/docs/observability/metrics) -- counters and histograms
- [Telemetry Stream](/docs/observability/telemetry-stream) -- NATS-native telemetry transport
