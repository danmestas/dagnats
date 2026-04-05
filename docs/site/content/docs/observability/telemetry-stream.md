---
title: Telemetry Stream
weight: 5
---

The TELEMETRY JetStream stream is the NATS-native transport for all observability signals -- spans, metrics, and logs flow through a single durable stream.

## Stream Configuration

The stream is provisioned by `natsutil.SetupTelemetryStream()` (called automatically by `natsutil.SetupAll()`):

| Parameter | Value |
|-----------|-------|
| **Name** | `TELEMETRY` |
| **Subjects** | `telemetry.>` |
| **Retention** | Limits policy |
| **Storage** | File |
| **MaxAge** | 7 days |
| **MaxBytes** | 1 GB |
| **Dedup Window** | 5 seconds |

The 7-day retention and 1 GB cap ensure telemetry does not grow unbounded. Whichever limit is hit first triggers pruning.

## Subject Hierarchy

All telemetry is published under `telemetry.>` with a structured subject hierarchy:

| Subject Pattern | Signal Type | Example |
|----------------|-------------|---------|
| `telemetry.spans.{service}.{run_id}` | Trace spans | `telemetry.spans.worker.abc123` |
| `telemetry.metrics.{service}.{name}` | Metric points | `telemetry.metrics.worker.step.duration_ms` |
| `telemetry.logs.{service}.{level}` | Log records | `telemetry.logs.engine.info` |

This hierarchy enables targeted subscriptions. Subscribe to `telemetry.logs.engine.error` to see only engine errors, or `telemetry.spans.worker.>` to see all worker traces.

## Wire Formats

### Span Records

```json
{
    "trace_id": "a1b2c3d4e5f6...",
    "span_id": "f6e5d4c3b2a1...",
    "parent_id": "1a2b3c4d5e6f...",
    "name": "worker.executeTask",
    "service": "worker",
    "kind": "server",
    "start_time": "2024-01-15T10:30:00Z",
    "end_time": "2024-01-15T10:30:01.234Z",
    "duration_ms": 1234,
    "status": "ok",
    "attributes": {"run_id": "abc123", "step_id": "build"},
    "events": [],
    "error": ""
}
```

Spans use `Nats-Msg-Id` (`{traceID}.{spanID}`) for deduplication. Duplicate spans from retried publishes are silently dropped.

### Metric Points

```json
{
    "name": "step.duration_ms",
    "type": "histogram",
    "value": 1234.0,
    "tags": {"task_type": "build"},
    "service": "worker",
    "timestamp": "2024-01-15T10:30:01Z"
}
```

### Log Records

```json
{
    "level": "error",
    "message": "task handler returned error, will retry",
    "service": "engine",
    "timestamp": "2024-01-15T10:30:01Z",
    "fields": {"run_id": "abc123", "task_type": "build"},
    "error": "connection refused"
}
```

## Tailing with the CLI

The `dagnats logs` command subscribes to the TELEMETRY stream for real-time observation:

```bash
# Tail all telemetry
dagnats logs --tail

# Filter by signal type
dagnats logs --tail --subject "telemetry.logs.engine.error"

# Filter by run
dagnats logs --tail --subject "telemetry.spans.worker.abc123"
```

## External Collection

The TELEMETRY stream is designed for consumption by external collectors. An OTel Collector, custom aggregator, or any NATS client can create a durable consumer on the stream and forward signals to a backend like SigNoz, Grafana, or Datadog.

```go
cons, _ := js.CreateOrUpdateConsumer(ctx, "TELEMETRY",
    jetstream.ConsumerConfig{
        FilterSubject: "telemetry.spans.>",
        AckPolicy:     jetstream.AckExplicitPolicy,
        DeliverPolicy: jetstream.DeliverAllPolicy,
    },
)
```

The 5-second dedup window prevents duplicate signals when publishers retry, so consumers do not need their own deduplication logic.

## Related

- [Structured Logging](/docs/observability/structured-logging) -- the Logger interface
- [Distributed Tracing](/docs/observability/distributed-tracing) -- the Tracer interface
- [Metrics](/docs/observability/metrics) -- the Metrics interface
