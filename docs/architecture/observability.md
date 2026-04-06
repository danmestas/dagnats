# Observability

## Design Decision: NATS-Native Telemetry

Zero external dependencies. Single `TELEMETRY` stream as sole backend. External OTel collectors (e.g., SigNoz) consume from the stream. Observability failures never break workflows (noop fallback).

## Signal Types

**Spans:** `telemetry.spans.{service}.{run_id}` — enables per-run trace queries
**Metrics:** `telemetry.metrics.{service}.{metric_name}` — per-metric dashboards
**Logs:** `telemetry.logs.{service}.{level}` — severity-based filtering

All published as JSON (human-debuggable via `nats sub`).

## Trace Propagation

W3C Trace Context (traceparent) dual-written to:
- NATS message headers (runtime propagation)
- Event payload fields `TraceParent`, `TraceState` (persistence in event log)

## Data Model

- **SpanRecord:** trace_id, span_id, parent_id, name, service, kind, duration_ms, status, attributes, events, error
- **MetricPoint:** name, type (counter/gauge/histogram), value, tags, service, timestamp
- **LogRecord:** level, message, service, trace_id, span_id, fields, timestamp, error

## Instrumentation Points

**Engine:** orchestrator.handleEvent, orchestrator.advanceDAG, orchestrator.enqueueTask, orchestrator.saveSnapshot
**Worker:** worker.executeTask, worker.complete, worker.fail, worker.continue
**API:** api.registerWorkflow, api.startRun, api.getRun (wrapper/inner pattern with span attributes)

## Metrics

- Request count, duration histogram, error count per API operation
- Active runs, completed runs, failed runs on engine
- Task execution duration on workers

## Components

- `observe/setup.go` — `InitTelemetry()` bootstraps OTel SDK (TracerProvider, MeterProvider, LoggerProvider)
- `observe/config.go` — `Config` struct for service name, NATS conn, OTLP endpoint
- `observe/carrier.go` — `NATSHeaderCarrier` for W3C propagation over NATS headers
- `observe/natsexporter/` — NATS JetStream exporters for spans, metrics, and logs

## OTel SDK

All observability uses the official OpenTelemetry Go SDK directly. No custom interfaces or abstractions. Packages import `go.opentelemetry.io/otel/trace`, `go.opentelemetry.io/otel/metric`, and `go.opentelemetry.io/otel/log` for instrumentation.
