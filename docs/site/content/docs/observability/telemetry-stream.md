---
title: Telemetry Stream
weight: 5
---

The `TELEMETRY` JetStream stream is the NATS-native store for all observability signals. Spans, metrics and logs flow through it as JSON, so you can read them with the `dagnats` CLI or any NATS client, with no external backend.

## Stream Configuration

The stream is provisioned by `natsutil.SetupAll()`:

| Parameter | Value |
|-----------|-------|
| **Name** | `TELEMETRY` |
| **Subjects** | `telemetry.>` |
| **Retention** | Limits policy |
| **Storage** | File |
| **MaxAge** | 7 days |
| **MaxBytes** | 10% of `max_store_bytes` |
| **Dedup Window** | 5 seconds |
| **Replicas** | The cluster's replication factor (1 standalone) |

Whichever limit is hit first prunes the oldest messages. Before 0.0.27 the stream was single-replica in a cluster; an existing deployment is upgraded in place on restart.

## Subject Hierarchy

| Subject Pattern | Signal Type | Example |
|----------------|-------------|---------|
| `telemetry.spans.{service}.{run_id}` | Trace spans | `telemetry.spans.dagnats.abc123` |
| `telemetry.metrics.{service}.{name}` | Metrics | `telemetry.metrics.my-worker.step.duration_ms` |
| `telemetry.logs.{service}.{severity}` | Log records | `telemetry.logs.dagnats.error` |

`{service}` is the process's `service.name`: `dagnats` for `dagnats serve` (which runs the engine, API and any embedded workers in one process), `dagnats-engine` and `dagnats-api` for the separate binaries, and whatever name your own worker passes to `InitTelemetry`. Spans that carry no run ID go to `telemetry.spans.{service}.no-run`. Metric names contain dots, so a metric subject has more than four tokens. Subscribe to `telemetry.logs.dagnats.error` to see only errors from a `dagnats serve` process, or to `telemetry.spans.my-worker.>` for all spans from one worker.

## Wire Formats

### Spans

Each message is one span as OTLP proto-JSON (the `Span` message of the OpenTelemetry trace protocol). Binary IDs are base64 and 64-bit integers are strings, as protojson writes them.

```json
{
  "traceId": "obLUw9Xm...",
  "spanId": "9uXUw7Kh...",
  "parentSpanId": "GiwtPlZv...",
  "name": "executeTask build",
  "kind": "SPAN_KIND_SERVER",
  "startTimeUnixNano": "1760869800000000000",
  "endTimeUnixNano": "1760869801234000000",
  "attributes": [
    {"key": "run_id", "value": {"stringValue": "abc123"}},
    {"key": "step_id", "value": {"stringValue": "build"}}
  ],
  "status": {"code": "STATUS_CODE_OK"}
}
```

The `Nats-Msg-Id` is `{traceID}.{spanID}`, so a span published twice is stored once.

### Metrics

Each message is one metric with all its data points. `data` is the OpenTelemetry SDK's metric data (a histogram, sum or gauge with its points), serialized by `encoding/json`:

```json
{
  "name": "step.duration_ms",
  "unit": "",
  "serviceName": "my-worker",
  "data": {"DataPoints": [], "Temporality": "CumulativeTemporality"},
  "timestamp": "2026-10-19T10:30:01.5Z"
}
```

`data` is abbreviated here. Points are cumulative since process start. The `Nats-Msg-Id` is `metric.{name}.{unix_nanos}.{sequence}`.

### Logs

```json
{
  "timestamp": "2026-10-19T10:30:01.123456789Z",
  "severity": "error",
  "body": "task handler returned error, will retry",
  "attributes": {"run_id": "abc123", "task_type": "build"},
  "traceId": "a1b2c3d4e5f60718293a4b5c6d7e8f90",
  "spanId": "1a2b3c4d5e6f7081",
  "serviceName": "dagnats"
}
```

The `Nats-Msg-Id` is `log.{unix_nanos}.{sequence}`.

## Reading with the CLI

```bash
dagnats logs --tail=100                         # last 100 log records
dagnats trace <run-id>                          # span tree for a run
dagnats metrics show --name=step.duration_ms    # one metric
dagnats observe status                          # pipeline health
```

## External Collection

Any NATS client can create a durable consumer on the stream and forward signals to a backend such as SigNoz, Grafana or Datadog:

```go
cons, err := js.CreateOrUpdateConsumer(ctx, "TELEMETRY",
    jetstream.ConsumerConfig{
        Durable:       "my-collector",
        FilterSubject: "telemetry.spans.>",
        AckPolicy:     jetstream.AckExplicitPolicy,
        DeliverPolicy: jetstream.DeliverAllPolicy,
    },
)
```

If you only need OTLP, you do not need a consumer: set `OTEL_EXPORTER_OTLP_ENDPOINT` and each process exports directly. See [Distributed Tracing]({{< ref "/docs/observability/distributed-tracing" >}}).

## Related

- [Structured Logging](/docs/observability/structured-logging) -- log records
- [Distributed Tracing](/docs/observability/distributed-tracing) -- spans
- [Metrics](/docs/observability/metrics) -- metrics
