---
title: Metrics
weight: 3
---

DagNats records metrics with the OpenTelemetry metrics API. Each process exports them to the `TELEMETRY` stream every 10 seconds, and optionally over OTLP.

## Pipeline

`observe.InitTelemetry` (see [Distributed Tracing]({{< ref "/docs/observability/distributed-tracing" >}})) registers a global meter provider with two readers:

- a NATS reader, which publishes each metric as JSON on `telemetry.metrics.{service}.{name}` every 10 seconds;
- an OTLP/HTTP reader, added when `OTEL_EXPORTER_OTLP_ENDPOINT` is set.

The NATS exporter uses cumulative temporality: each point carries the running total since the process started, not the change in the last interval. Counters therefore only go up until the process restarts.

Create your own instruments with the standard API:

```go
meter := otel.Meter("my-worker")
processed, _ := meter.Int64Counter("documents.processed")
latency, _ := meter.Float64Histogram("documents.latency_ms")

processed.Add(ctx, 1, metric.WithAttributes(attribute.String("task_type", "ocr")))
latency.Record(ctx, 123.4)
```

Instrument names are not prefixed, so pick names that will not collide with the ones below.

## Metrics DagNats emits

| Metric | Type | Component | Description |
|--------|------|-----------|-------------|
| `step.duration_ms` | Histogram | Worker | Time to execute a task handler |
| `step.retries` | Counter | Worker | Handler errors that led to a retry |
| `worker.tasks.active` | UpDownCounter | Worker | Tasks currently being processed |
| `worker.tasks.cancelled_skipped` | Counter | Worker | Tasks skipped because their run was already cancelled |
| `worker.log_chunk.publish_failures` | Counter | Worker | Log chunks that failed to publish |
| `workflow.runs.active` | UpDownCounter | Engine | Runs currently running |
| `workflow.runs.completed` | Counter | Engine | Runs that completed |
| `workflow.runs.failed` | Counter | Engine | Runs that failed |
| `workflow.runs.reconciled` | Counter | Engine | Runs the reconciler repaired |
| `workflow.runs.scan_degraded` | Counter | Engine | Bounded run scans that had to cap or skip keys; a signal to prune or tune retention |
| `step.enqueue.count` | Counter | Engine | Tasks published to `TASK_QUEUES` |
| `step.failure.non_retriable` | Counter | Engine | Non-retriable step failures |
| `step.failure.retry_after` | Counter | Engine | Step failures with a requested retry delay |
| `task.concurrency.acquired` | Counter | Engine | Task concurrency slots taken |
| `task.concurrency.rejected` | Counter | Engine | Tasks held back by a full concurrency limit |
| `snapshot.save.duration_ms` | Histogram | Engine | Time to save a run snapshot |
| `dlq_entries_total` | Counter | Engine | Entries written to the dead letter queue |
| `dlq_depth` | UpDownCounter | Engine | Current dead letter queue depth |
| `api.requests` | Counter | API | Control-plane requests |
| `api.errors` | Counter | API | Control-plane requests that failed |
| `api.request.duration_ms` | Histogram | API | Control-plane request latency |
| `bridge.requests` | Counter | Bridge | HTTP bridge requests |
| `bridge.request.duration_ms` | Histogram | Bridge | HTTP bridge request latency |
| `trigger_firings_total` | Counter | Trigger | Trigger fires |
| `trigger_last_fired_seconds` | Gauge | Trigger | Unix seconds of each cron trigger's last successful fire in this process |
| `trigger_next_fire_seconds` | Gauge | Trigger | Unix seconds of each enabled cron trigger's next fire |
| `console_audit_events_total` | Counter | Console | Console audit events written |

The engine also emits repair and failure counters for its internal indexes and run finalization (names starting `engine.run_active.`, `engine.run_index.`, `engine.finalize.` and `engine.run_event.`), `engine.concurrency.legacy_reset`, `engine.def_pin.missing_version`, `trigger.run_terminal.depth_refusals`, `bridge.log_chunk.publish_failures` and `workertoken.watch_failures`. A non-zero value on any of those is worth a look.

## Reading metrics

```bash
dagnats metrics show --name=step.duration_ms --since=5m --service=<service>
```

`--name` is required. `--since` defaults to 5 minutes, `--service` filters by the emitting process's service name, and `--json` gives machine-readable output.

### Prometheus

`dagnats serve` aggregates the metrics it sees on the stream and serves them in Prometheus text format at `/metrics` on the HTTP port. Counters get a `_total` suffix and dots become underscores, so `workflow.runs.completed` is `workflow_runs_completed_total`. By default only loopback callers may scrape it. Set `METRICS_AUTH` to change that:

| `METRICS_AUTH` | Behavior |
|----------------|----------|
| `loopback` (default) | Only `127.0.0.1` and `::1` |
| `basic` | HTTP Basic auth with `METRICS_BASIC_USER` and `METRICS_BASIC_PASS` |
| `forward` | Trust `X-Forwarded-User` from a reverse proxy |
| `none` | Open to every caller; logs a warning at startup |

An unknown value falls back to `loopback`.

`dagnats observe status` reports whether the telemetry pipeline is healthy.

## Related

- [Structured Logging](/docs/observability/structured-logging) -- log records
- [Distributed Tracing](/docs/observability/distributed-tracing) -- spans
- [Telemetry Stream](/docs/observability/telemetry-stream) -- NATS-native telemetry transport
