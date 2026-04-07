# Observability

For deployment modes (embedded sidecar, distributed S3, external
collector) and usage guide, see [../observability.md](../observability.md).
This document covers architecture and internals.

## Design Decision: NATS-Native + OTel SDK

All observability uses the official OpenTelemetry Go SDK directly.
No custom interfaces or abstractions — packages import
`go.opentelemetry.io/otel/trace`, `go.opentelemetry.io/otel/metric`,
and `go.opentelemetry.io/otel/log` for instrumentation.

Telemetry is always written to a NATS `TELEMETRY` JetStream stream
(7-day retention, 1 GB cap). Optionally, an OTLP/HTTP exporter sends
data to an external collector (SigNoz, Grafana Tempo, Jaeger) when
`OTEL_EXPORTER_OTLP_ENDPOINT` is set. Observability failures never
break workflows — export errors are silently dropped.

## Signal Types

Subjects within the `TELEMETRY` stream:

- **Spans:** `telemetry.spans.{service}.{run_id}`
- **Metrics:** `telemetry.metrics.{service}.{metric_name}`
- **Logs:** `telemetry.logs.{service}.{level}`

All published as protobuf JSON (human-debuggable via `nats sub`).

## Trace Propagation

W3C Trace Context (traceparent) dual-written to:
- NATS message headers (runtime propagation between components)
- Event payload fields `TraceParent`, `TraceState` (persisted in
  the WORKFLOW_HISTORY stream for later correlation)

## Instrumentation Points

| Component | Spans |
|-----------|-------|
| Engine | handleEvent, advanceDAG, enqueueTask, saveSnapshot |
| Worker | executeTask, complete, fail, continue |
| API | registerWorkflow, startRun, getRun |

## Metrics

- Request count, duration histogram, error count per API operation
- Active runs, completed runs, failed runs on engine
- Task execution duration on workers

## Components

| File | Purpose |
|------|---------|
| `observe/setup.go` | `InitTelemetry()` — bootstraps OTel SDK providers |
| `observe/config.go` | `Config` struct (service name, NATS conn, OTLP endpoint) |
| `observe/carrier.go` | `NATSHeaderCarrier` for W3C propagation over NATS |
| `observe/propagation.go` | `ExtractTraceContext` from NATS messages |
| `observe/natsexporter/` | NATS JetStream exporters for spans, metrics, logs |
| `sidecar/supervisor.go` | Process supervisor for sidecar child processes |
| `sidecar/collector.go` | OTel Collector YAML config generation |
| `sidecar/config.go` | Sidecar config (local/S3 storage, backend forwarding) |
| `sidecar/process.go` | Child process management with health checks |
| `sidecar/install.go` | Binary detection and installer for otelcol, otlp2parquet |
| `cmd/dagnats-mcp-duckdb/` | DuckDB MCP server over Parquet telemetry files |

## Configuration

```bash
# Export to any OTLP/HTTP-compatible backend
OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4318 dagnats serve

# Or set in dagnats.yaml
otlp_endpoint: http://collector:4318
```

Without OTLP export configured, telemetry is still available via:

```bash
dagnats trace <run-id>        # span tree for a run
dagnats logs search --level=error  # search log stream
dagnats metrics show           # metric snapshots
dagnats run inspect <id> --trace   # unified debug view with spans
```

## CLI Observability Commands

- `dagnats trace <run-id>` — view distributed trace tree
- `dagnats trace search` — find traces by service/status
- `dagnats logs` — tail or search telemetry log stream
- `dagnats metrics show` — view metric snapshots
- `dagnats run inspect --trace` — unified debug view with inline
  span tree
- `dagnats status --detail` — stream health, queue depth, DLQ count
