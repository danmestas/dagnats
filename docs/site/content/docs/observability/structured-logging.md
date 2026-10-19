---
title: Structured Logging
weight: 1
---

DagNats components log with Go's `log/slog`. A separate OpenTelemetry log pipeline publishes records to the `TELEMETRY` stream, and `dagnats logs` reads them back.

## What dagnats writes

`dagnats serve` installs a text `slog` handler on stderr as the process default. Every record also goes through an in-memory ring (the last 10,000 entries, no older than 30 minutes) that backs the console's live log page. Nothing is written to disk by dagnats itself, so capture stderr with your process supervisor or container runtime.

Use `slog` fields for context, as the codebase does:

```go
logger := slog.Default().With("run_id", runID, "workflow", "deploy")
logger.Info("step started", "step_id", "build")
logger.Error("task failed", "step_id", "build", "error", err)
```

## The OpenTelemetry log pipeline

`observe.InitTelemetry` sets up an OpenTelemetry `LoggerProvider` next to the tracer and meter providers. Its records are exported two ways:

- always, to NATS: each record is published as JSON on `telemetry.logs.{service}.{severity}` on the [TELEMETRY stream]({{< ref "/docs/observability/telemetry-stream" >}});
- when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, also over OTLP/HTTP to that backend.

The `dagnats serve` process, `dagnats-engine` and `dagnats-api` call `InitTelemetry` at startup. dagnats's own `slog` output is not forwarded into this provider, so records appear on `telemetry.logs.*` only when code in the process emits OpenTelemetry log records. If you run your own worker and want its logs on the stream, call `InitTelemetry` and emit records through an OpenTelemetry log bridge.

A record looks like this:

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

`severity` is the record's severity text in lowercase, or `info` when it has none. `attributes` holds string values only. `traceId` and `spanId` are present when the record was emitted inside a span, which is how you tie a log line to a trace. Each record is published with a unique `Nats-Msg-Id` of the form `log.{unix_nanos}.{sequence}`.

## Reading logs

```bash
# Follow all log records
dagnats logs

# Show the last 100, then exit
dagnats logs --tail=100

# One service, one level
dagnats logs --service=dagnats --level=error

# Search history
dagnats logs search --search="connection refused" --since=1h --level=error --limit=50
```

`--tail=N` takes a number from 1 to 10000. Without it the command follows the stream until interrupted. The same records are available to any NATS client on `telemetry.logs.>`.

## Related

- [Distributed Tracing](/docs/observability/distributed-tracing) -- spans that logs link to
- [Metrics](/docs/observability/metrics) -- counters and histograms
- [Error Reporting](/docs/observability/error-reporting) -- how failures surface
- [Telemetry Stream](/docs/observability/telemetry-stream) -- NATS-native telemetry transport
