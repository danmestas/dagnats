# Observability

## Design Decision: NATS-Native Telemetry

Zero external dependencies. Single `TELEMETRY` stream as sole backend. Optional Jaeger OTLP/HTTP exporter activated by env var. Observability failures never break workflows (noop fallback).

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

## Components (~400 LOC target in `observe/simple/`)

- TraceCollector: async publish with bounded buffer
- MetricsCollector: counter/gauge/histogram wrappers
- LogCollector: structured logging with trace context
- ErrorReporter: span-aware error capture
- Propagation: W3C inject/extract
- Jaeger exporter: batch OTLP/HTTP upload
- StorageMonitor: stream capacity warnings via `alerts.storage.TELEMETRY`

## Provider-Agnostic Interfaces

All observability must go through in-house interfaces. No direct imports of Sentry, Jaeger, etc. outside adapter packages. Pattern: `interface` in `observe/` → `adapter` in `observe/{provider}/`.
