# Observability Design — Hipp-Simple

Minimal, self-contained observability for DagNats. Zero external dependencies beyond
nats.go. Telemetry flows to a single NATS stream as JSON. Optional embedded Jaeger
exporter activated by environment variable. Observability failures never break
workflows.

## Decisions

- **Philosophy:** Hipp — small, fast, reliable, independent, zero-config
- **Dependencies:** Zero beyond nats.go and stdlib. No OTEL SDK.
- **Implementation:** ~400 LOC in `observe/simple/`, pure Go
- **Format:** JSON always (debuggable via `nats sub`)
- **Stream:** Single `TELEMETRY` stream for all signals
- **Export:** Embedded Jaeger OTLP/HTTP exporter (~100 LOC, `net/http` only)
- **Activation:** `JAEGER_ENDPOINT` env var. Unset = telemetry stays in NATS.
- **Failure mode:** Log and continue. Observability never breaks workflows.
- **Propagation:** W3C Trace Context in NATS headers + event payloads for replay
- **Config:** Zero. Service name from binary name. Retention from stream limits.

## Architecture Overview

```
Component (engine/worker/api)
    |
    v
observe.Tracer / Logger / Metrics  (interfaces — unchanged)
    |
    v
observe/simple/  (pure Go, ~400 LOC)
    |
    v
NATS JetStream  (single TELEMETRY stream, JSON, 7-day retention)
    |
    v  (only if JAEGER_ENDPOINT is set)
Embedded goroutine  (subscribes to stream, POSTs to Jaeger)
```

**Core Principle:** No separate binaries. No external SDKs. The telemetry
implementation is embedded in existing engine/worker/api binaries. If Jaeger is
not configured, telemetry is still queryable in NATS via `nats sub telemetry.>`.

## Data Model

### Single NATS Stream

#### TELEMETRY

- Subjects: `telemetry.{signal}.{service}.{run_id}`
- Signals: `spans`, `metrics`, `logs`
- Services: `engine`, `worker`, `api`
- Payload: JSON (always)
- Retention: 7 days by message age (NATS stream limit)
- Storage: File, limits policy
- Deduplication: 5s window via `Nats-Msg-Id`

No DLQ. No consumer config KV. No secrets KV. One stream.

### Subject Structure

The fourth subject segment varies by signal type for optimal filtering:

- **Spans:** `telemetry.spans.{service}.{run_id}` — enables per-run trace queries
- **Metrics:** `telemetry.metrics.{service}.{metric_name}` — enables per-metric dashboards
- **Logs:** `telemetry.logs.{service}.{level}` — enables severity-based filtering

This is intentional. Each signal has a different primary query axis. Use
`telemetry.spans.engine.>` for all engine spans, or `telemetry.>.>.{run_id}` for
all signals from a specific run.

**Open decision:** If querying by metric name / log level in NATS proves uncommon
(e.g., Jaeger handles all trace queries), simplify all subjects to
`telemetry.{signal}.{service}.{run_id}` and filter by name/level after
subscription. Prototype both during implementation and decide based on actual
usage patterns.

### Span Format

```go
type SpanRecord struct {
    TraceID     string            `json:"trace_id"`
    SpanID      string            `json:"span_id"`
    ParentID    string            `json:"parent_id,omitempty"`
    Name        string            `json:"name"`
    Service     string            `json:"service"`
    Kind        string            `json:"kind"` // internal, server, client
    StartTime   time.Time         `json:"start_time"`
    EndTime     time.Time         `json:"end_time"`
    DurationMS  int64             `json:"duration_ms"`
    Status      string            `json:"status"` // ok, error
    Attributes  map[string]any    `json:"attributes,omitempty"`
    Events      []SpanEvent       `json:"events,omitempty"`
    Error       string            `json:"error,omitempty"`
}

type SpanEvent struct {
    Name       string         `json:"name"`
    Time       time.Time      `json:"time"`
    Attributes map[string]any `json:"attributes,omitempty"`
}
```

`run_id` is extracted from the `run_id` span attribute. If no `run_id` attribute
is set, the fallback subject is `telemetry.spans.{service}.no-run`.

Published with `Nats-Msg-Id: {trace_id}.{span_id}`.

### Metric Format

```go
type MetricPoint struct {
    Name      string         `json:"name"`
    Type      string         `json:"type"` // counter, gauge, histogram
    Value     float64        `json:"value"`
    Tags      map[string]string `json:"tags,omitempty"`
    Service   string         `json:"service"`
    Timestamp time.Time      `json:"timestamp"`
}
```

Published to `telemetry.metrics.{service}.{metric_name}`.

### Log Format

```go
type LogRecord struct {
    Level      string         `json:"level"` // debug, info, warn, error
    Message    string         `json:"message"`
    Service    string         `json:"service"`
    TraceID    string         `json:"trace_id,omitempty"`
    SpanID     string         `json:"span_id,omitempty"`
    Fields     map[string]any `json:"fields,omitempty"`
    Timestamp  time.Time      `json:"timestamp"`
    Error      string         `json:"error,omitempty"`
}
```

Published to `telemetry.logs.{service}.{level}`.

### Trace Context in Events

Existing `protocol.Event` gains two fields for W3C trace context:

```go
type Event struct {
    // ... existing fields ...
    TraceParent string `json:"trace_parent,omitempty"`
    TraceState  string `json:"trace_state,omitempty"`
}
```

Both fields use `omitempty` so existing events without trace context serialize
identically. Stored in the history stream for replay. Propagated in NATS message
headers for runtime trace continuity.

## Observe Interfaces

### New: Tracer Interface

Added to `observe/` package (stdlib only):

```go
// Tracer creates and manages distributed trace spans.
type Tracer interface {
    Start(ctx context.Context, name string, opts ...SpanOption) (context.Context, Span)
}

// Span represents an active trace span. End must be called exactly once.
type Span interface {
    End()
    SetStatus(code StatusCode, description string)
    SetAttributes(attrs ...Attribute)
    RecordError(err error)
    AddEvent(name string, attrs ...Attribute)
}

// SpanOption configures span creation.
type SpanOption interface{ /* sealed */ }

func WithSpanKind(kind SpanKind) SpanOption
func WithAttributes(attrs ...Attribute) SpanOption

// Attribute is a typed key-value pair.
// Must be constructed via typed constructors — the simple adapter panics on
// unsupported Value types as an assertion contract (TigerStyle).
type Attribute struct {
    Key   string
    Value any // string, int64, float64, bool only
}

func StringAttr(key, val string) Attribute
func Int64Attr(key string, val int64) Attribute
func Float64Attr(key string, val float64) Attribute
func BoolAttr(key string, val bool) Attribute
```

### Existing: Unchanged

- `observe.Logger` — the simple adapter writes JSON log records to the TELEMETRY
  stream with trace context from the current span (if any in context).
- `observe.Metrics` — the simple adapter publishes metric points to the TELEMETRY
  stream. Tags are bound at creation time via `Counter(name, tags)` and included
  in every published point.
- `observe.ErrorReporter` — records error on the active span from context. If no
  active span, publishes a standalone error log record.

### Noop Defaults Extended

`observe/noop.go` gains `noopTracer` and `noopSpan` for safe defaults:

```go
func NewNoopTracer() Tracer { return &noopTracer{} }
```

## Simple Implementation

### observe/simple/ Package (~400 LOC)

Pure Go. Only imports: `nats.go`, `protocol/`, stdlib. No OTEL SDK.

#### TraceCollector (trace_collector.go, ~80 LOC)

```go
type TraceCollector struct {
    js          nats.JetStreamContext
    metrics     observe.Metrics // for telemetry.spans.dropped counter
    serviceName string
    records     chan SpanRecord // buffered channel, async publish
}

func NewTraceCollector(
    js nats.JetStreamContext,
    serviceName string,
    metrics observe.Metrics,
) *TraceCollector

// Start implements observe.Tracer. Creates a span, extracts parent from
// context if present, generates trace/span IDs via crypto/rand.
func (t *TraceCollector) Start(
    ctx context.Context,
    name string,
    opts ...observe.SpanOption,
) (context.Context, observe.Span)

// Flush drains the buffered span channel, blocking until all pending spans
// are published or a 5s timeout expires. Called during shutdown.
func (t *TraceCollector) Flush()
```

`LiveSpan` implements `observe.Span`. On `End()`, the completed span is
serialized to a `SpanRecord` and sent to the `records` channel for async
publish to `telemetry.spans.{service}.{run_id}`. The channel has a bounded
size (1024). If full, the span is dropped and `telemetry.spans.dropped` counter
is incremented — making overflow observable via the same metrics pipeline. Also
logged once per minute to avoid log spam.

#### MetricsCollector (metrics_collector.go, ~60 LOC)

```go
type MetricsCollector struct {
    js          nats.JetStreamContext
    serviceName string
}

func NewMetricsCollector(js nats.JetStreamContext, serviceName string) *MetricsCollector
```

Implements `observe.Metrics`. Each `Counter`/`Histogram`/`Gauge` call returns
a lightweight wrapper that publishes a JSON `MetricPoint` to
`telemetry.metrics.{service}.{metric_name}` on every observation. Counters
and gauges publish immediately. Histograms publish each observation.

#### LogCollector (log_collector.go, ~50 LOC)

```go
type LogCollector struct {
    js          nats.JetStreamContext
    serviceName string
}

func NewLogCollector(js nats.JetStreamContext, serviceName string) *LogCollector
```

Implements `observe.Logger`. Each `Info`/`Error` call publishes a JSON
`LogRecord` to `telemetry.logs.{service}.{level}`.

**Log-trace correlation:** The existing `observe.Logger` interface does not accept
a `context.Context`, so automatic trace context extraction is not possible.
Instead, callers who want log-trace correlation use `logger.With(fields)` to
create a sub-logger with trace context pre-bound:

```go
// At span start, create a correlated logger:
spanLogger := logger.With(
    observe.String("trace_id", span.TraceID()),
    observe.String("span_id", span.SpanID()),
)
spanLogger.Info("processing step", observe.String("step_id", stepID))
```

This is explicit but lightweight. A context-aware `Logger` variant can be added
later if the pattern becomes too verbose. For now, the `With` approach avoids
changing the existing interface.

#### Trace Context Propagation (propagation.go, ~40 LOC)

Dual-write to NATS headers and event payloads is handled atomically by a single
function call. It is impossible to forget one location.

```go
// InjectTraceContext writes W3C traceparent + tracestate to BOTH the NATS
// message headers AND the event payload fields. Single call, both locations.
// Makes it impossible to update one without the other.
func InjectTraceContext(ctx context.Context, msg *nats.Msg, evt *protocol.Event)

// ExtractTraceContext reads W3C traceparent from NATS message headers first
// (runtime). Falls back to event payload fields (replay). Returns a context
// with the parent span set.
func ExtractTraceContext(msg *nats.Msg, evt *protocol.Event) context.Context
```

W3C `traceparent` format: `00-{32 hex chars trace_id}-{16 hex chars span_id}-{2 hex chars flags}`.
Generated from crypto/rand (16 bytes = 32 hex chars for trace ID, 8 bytes = 16 hex
chars for span ID). No external propagator library.

#### Jaeger Exporter (jaeger.go, ~70 LOC)

```go
// ExportToJaeger subscribes to the TELEMETRY stream and POSTs span batches
// to a Jaeger OTLP/HTTP endpoint. Runs as an embedded goroutine.
// On failure: logs error, continues. Observability never breaks workflows.
func ExportToJaeger(
    ctx context.Context,
    js nats.JetStreamContext,
    endpoint string,
    logger observe.Logger,
)
```

- Subscribes to `telemetry.spans.>` on the TELEMETRY stream
- Batches spans (up to 100 or 5s, whichever comes first)
- Converts to OTLP JSON format
- POSTs to `{endpoint}/v1/traces`
- On HTTP failure: log error, drop batch, continue
- On shutdown (ctx cancelled): flush remaining batch, exit

No retry policy. No DLQ. If Jaeger is down, spans stay in NATS (7-day retention)
and can be manually replayed or queried via `nats sub`.

### SetupTelemetry Entry Point (setup.go, ~30 LOC)

Returns a single `Telemetry` struct to reduce parameter passing at call sites.

```go
// Telemetry bundles all observability interfaces. Passed as a single argument
// to component constructors instead of four separate parameters.
type Telemetry struct {
    Tracer   observe.Tracer
    Logger   observe.Logger
    Metrics  observe.Metrics
    Errors   observe.ErrorReporter
}

// SetupTelemetry creates the simple telemetry stack. Zero-config: service name
// defaults to binary name, Jaeger export activates only if JAEGER_ENDPOINT is set.
func SetupTelemetry(nc *nats.Conn) (*Telemetry, func()) {
    serviceName := filepath.Base(os.Args[0])

    js, err := nc.JetStream()
    if err != nil {
        // JetStream unavailable — return noop telemetry. Log to stderr
        // since we have no logger yet. This is not fatal: the application
        // runs without telemetry rather than crashing.
        log.Printf("SetupTelemetry: JetStream unavailable, using noop: %v", err)
        return &Telemetry{
            Tracer:  observe.NewNoopTracer(),
            Logger:  observe.NewNoopLogger(),
            Metrics: observe.NewNoopMetrics(),
            Errors:  observe.NewNoopErrorReporter(),
        }, func() {}
    }

    metrics := NewMetricsCollector(js, serviceName)  // create first
    collector := NewTraceCollector(js, serviceName, metrics) // needs metrics
    logger := NewLogCollector(js, serviceName)
    errors := NewErrorReporter(collector, logger)

    ctx, cancel := context.WithCancel(context.Background())

    // Optional: embedded Jaeger exporter
    if endpoint := os.Getenv("JAEGER_ENDPOINT"); endpoint != "" {
        go ExportToJaeger(ctx, js, endpoint, logger)
    }

    shutdown := func() {
        cancel()
        collector.Flush()
    }

    return &Telemetry{
        Tracer:  collector,
        Logger:  logger,
        Metrics: metrics,
        Errors:  errors,
    }, shutdown
}
```

No error return. If JetStream is unavailable, returns noop implementations
and logs a warning. If individual NATS publishes fail at runtime, the adapter
logs and continues. Telemetry never prevents the application from starting.

Note: `SetupTelemetry` obtains the JetStream context once and passes it to all
collectors. This avoids each collector independently calling `nc.JetStream()`.

#### ErrorReporter (error_reporter.go, ~30 LOC)

```go
// NewErrorReporter creates an ErrorReporter backed by a Tracer and Logger.
// CaptureError: extracts active span from ctx, calls RecordError + SetStatus.
// If no active span, falls back to logger.Error() with the error and tags
// as fields — ensures errors are always recorded somewhere.
// CaptureMessage: same pattern, adds message as span event or logs via logger.
//
// NOTE: The fallback path (logger.Error) writes to telemetry.logs, NOT to
// telemetry.spans. These errors will NOT appear in Jaeger (which only
// subscribes to telemetry.spans.>). They remain queryable in NATS and
// visible in structured logs. If all errors must reach Jaeger, callers
// should ensure an active span exists before calling CaptureError.
func NewErrorReporter(tracer observe.Tracer, logger observe.Logger) observe.ErrorReporter
```

Component constructors accept `*Telemetry` instead of separate interfaces:

```go
// Before:
engine.NewOrchestrator(nc, logger, metrics)

// After:
engine.NewOrchestrator(nc, telemetry)
```

This is a breaking change to component constructors. All call sites in `cmd/`
binaries and test files must be updated. The migration is mechanical: replace
separate logger/metrics parameters with a single `*Telemetry` argument.

### Migration Checklist

The following call sites must be updated when introducing `*Telemetry`:

- `engine.NewOrchestrator(nc, logger, metrics)` → `(nc, telemetry)`
- `worker.NewWorker(nc, logger)` → `(nc, telemetry)`
- `api.NewService(nc, logger)` → `(nc, telemetry)`
- `cmd/dagnats-engine/main.go` — call `SetupTelemetry`, pass result
- `cmd/dagnats-api/main.go` — call `SetupTelemetry`, pass result
- All test files using `observe.NewNoopLogger()` / `observe.NewNoopMetrics()`
  — replace with `&simple.Telemetry{...}` using noop implementations
- `e2e_test.go` — update orchestrator, worker, service construction

## Instrumentation Points

Unchanged from previous design. All spans, metrics, and trace propagation
apply identically.

### Engine (engine/orchestrator.go)

**Spans:**

| Span | When | Key Attributes |
|------|------|----------------|
| `orchestrator.handleEvent` | Every history event consumed | `run_id`, `event_type`, `step_id` |
| `orchestrator.advanceDAG` | After resolving ready steps | `run_id`, `ready_steps_count` |
| `orchestrator.enqueueTask` | Publishing task to TASK_QUEUES | `run_id`, `step_id`, `task_name` |
| `orchestrator.saveSnapshot` | KV snapshot write | `run_id`, `revision` |

**Metrics:**

- `workflow.runs.active` (gauge) — inc on WorkflowStarted, dec on Completed/Failed
- `workflow.runs.completed` (counter)
- `workflow.runs.failed` (counter)
- `step.enqueue.count` (counter) — per task type
- `snapshot.save.duration_ms` (histogram)

### Worker (worker/worker.go)

**Spans:**

| Span | When | Key Attributes |
|------|------|----------------|
| `worker.executeTask` | Entire task handler execution | `run_id`, `step_id`, `task_name` |
| `worker.complete` | ctx.Complete() called | `run_id`, `step_id`, `output_size_bytes` |
| `worker.fail` | ctx.Fail() called | `run_id`, `step_id`, `error` |
| `worker.continue` | ctx.Continue() called | `run_id`, `step_id`, `iteration` |

**Metrics:**

- `step.duration_ms` (histogram) — per task type
- `step.retries` (counter) — per task type
- `agent.loop.iterations` (histogram) — per workflow
- `worker.tasks.active` (gauge) — currently executing handlers
- `nats.consumer.pending` (gauge) — task queue depth per consumer

### API (api/rest.go, api/natsapi.go)

**Spans:**

| Span | When | Key Attributes |
|------|------|----------------|
| `api.registerWorkflow` | Workflow registration | `workflow_name` |
| `api.startRun` | New run started | `workflow_name`, `run_id` |
| `api.getRun` | Run status queried | `run_id` |

**Metrics:**

- `api.requests` (counter) — per endpoint, per transport (REST/NATS)
- `api.request.duration_ms` (histogram) — per endpoint
- `api.errors` (counter) — per endpoint, per error type

### Trace Propagation Flow

A single workflow run produces one distributed trace:

```
api.startRun (root span)
  +-- orchestrator.handleEvent (child, linked via NATS headers)
       +-- orchestrator.enqueueTask (child)
            +-- worker.executeTask (child, linked via NATS headers)
                 +-- worker.complete
                 +-- orchestrator.handleEvent (next step, linked)
                      +-- orchestrator.enqueueTask
                           +-- worker.executeTask (next worker)
```

All spans share the same trace ID, correlated via `run_id` attribute and W3C
`traceparent` propagation through NATS message headers.

## Storage Monitoring

### StorageMonitor (observe/simple/monitor.go, ~40 LOC)

Embedded goroutine checking TELEMETRY stream stats periodically:

```go
type StorageMonitor struct {
    js         nats.JetStreamContext // StreamInfo + Publish for advisories
    interval   time.Duration        // Default: 60s
    warnBytes  int64                // Default: 80% of stream MaxBytes
}

// Start checks stream stats on interval, publishes advisory to
// alerts.storage.TELEMETRY when threshold exceeded. Exits on ctx cancel.
func (m *StorageMonitor) Start(ctx context.Context)
```

Advisory published to `alerts.storage.TELEMETRY`:

```json
{
  "stream": "TELEMETRY",
  "usage_bytes": 858993459,
  "max_bytes": 1073741824,
  "usage_percent": 80.0,
  "message": "TELEMETRY stream at 80% capacity",
  "timestamp": "2026-03-30T12:00:00Z"
}
```

Health endpoint (`GET /health`) extended with single stream status:

```json
{
  "status": "healthy",
  "telemetry": {
    "stream": {"messages": 55720, "bytes": 524288000, "percent": 48.8},
    "jaeger": "exporting"
  }
}
```

`"jaeger"` is `"exporting"`, `"disabled"` (no endpoint), or `"error: ..."`.

## NATS Resource Setup

`natsutil.SetupAll()` adds one stream:

```go
func SetupTelemetryStream(js nats.JetStreamContext) error {
    _, err := js.AddStream(&nats.StreamConfig{
        Name:       "TELEMETRY",
        Subjects:   []string{"telemetry.>"},
        Retention:  nats.LimitsPolicy,
        Storage:    nats.FileStorage,
        MaxAge:     7 * 24 * time.Hour,     // 7-day retention
        MaxBytes:   1 << 30,                 // 1 GB
        Duplicates: 5 * time.Second,           // 5s dedup window
    })
    return err
}
```

One stream. One call. No KV buckets for telemetry config.

## Security Model

Minimal — uses existing NATS account/user permissions:

| User | Publish | Subscribe |
|------|---------|-----------|
| Engine/Worker/API | `telemetry.>` | `telemetry.>` (for embedded Jaeger exporter) |
| Admin | `telemetry.>`, `alerts.>` | `telemetry.>`, `alerts.>` |

No separate consumer user. No secrets KV. Jaeger endpoint is an env var on the
binary that runs the embedded exporter.

## Testing Strategy

### Layer 1: Pure Unit Tests (observe/simple/)

No NATS, no I/O:

- Span creation: trace/span ID generation, parent linking, attribute setting
- Span serialization: JSON round-trip, all fields present
- Metric point serialization: counter/gauge/histogram values correct
- Log record serialization: level, message, trace context correlation
- Trace context: W3C traceparent format, inject/extract round-trip
- Negative: dropped spans on full channel (bounded buffer), invalid attributes panic

### Layer 2: Integration Tests (observe/simple/)

Real embedded NATS per test:

- TraceCollector: start span, end span, message arrives on `telemetry.spans.>`
- MetricsCollector: inc counter, message on `telemetry.metrics.>` with correct value
- LogCollector: log info, message on `telemetry.logs.>` with trace context
- Deduplication: same span ID published twice, one message in stream
- SetupTelemetry: zero-config defaults produce working collectors
- Negative: NATS publish failure does not panic or propagate error

### Layer 3: Jaeger Exporter Tests (observe/simple/)

Real embedded NATS + mock HTTP server:

- Happy path: spans in stream, exporter POSTs batch to mock Jaeger endpoint
- Batching: 100 spans trigger immediate flush, <100 flush after 5s timeout
- Jaeger down: exporter logs error, does not crash, continues consuming
- Shutdown: remaining spans flushed before exit
- Negative: malformed endpoint URL logged, exporter continues

### Layer 4: End-to-End Telemetry Tests

Full pipeline with real NATS, orchestrator, workers:

- Trace propagation: spans for api.startRun, orchestrator.handleEvent,
  worker.executeTask share same trace ID
- Trace context in history: event payloads contain matching traceparent
- Storage advisory: tiny stream limits trigger advisory on alerts.storage.>
- Health endpoint: reflects stream usage and Jaeger exporter status
- NATS-only mode: no JAEGER_ENDPOINT, telemetry queryable via NATS subscribe

### Testing Rules

- Each test file opens with methodology comment
- Minimum 2 assertions per test (positive + negative space)
- Bounded timeouts on all waits
- No shared NATS servers between tests
- 70-line function limit, split into named helpers
- Mock HTTP server for Jaeger tests (no real Jaeger in CI)

## Project Structure

```
dagnats/
+-- observe/                        # Interfaces (stdlib only)
|   +-- observe.go                  # Logger, Metrics, ErrorReporter (unchanged)
|   +-- tracer.go                   # NEW: Tracer, Span, SpanOption, Attribute
|   +-- noop.go                     # MODIFIED: add noopTracer, noopSpan
|   +-- simple/                     # NEW: pure Go telemetry (~400 LOC)
|       +-- trace_collector.go      #   TraceCollector (observe.Tracer impl)
|       +-- metrics_collector.go    #   MetricsCollector (observe.Metrics impl)
|       +-- log_collector.go        #   LogCollector (observe.Logger impl)
|       +-- error_reporter.go       #   ErrorReporter (observe.ErrorReporter impl)
|       +-- propagation.go          #   W3C trace context inject/extract
|       +-- jaeger.go               #   Embedded Jaeger OTLP/HTTP exporter
|       +-- monitor.go              #   Storage pressure monitoring
|       +-- setup.go                #   SetupTelemetry() entry point
|
+-- natsutil/
|   +-- conn.go                     # MODIFIED: SetupAll adds TELEMETRY stream
|
+-- engine/
|   +-- orchestrator.go             # MODIFIED: inject tracer, emit spans/metrics
|
+-- worker/
|   +-- worker.go                   # MODIFIED: inject tracer, emit spans/metrics
|
+-- api/
|   +-- service.go                  # MODIFIED: inject tracer, emit spans
|   +-- rest.go                     # MODIFIED: emit request spans/metrics
|   +-- natsapi.go                  # MODIFIED: emit request spans/metrics
|
+-- protocol/
|   +-- protocol.go                 # MODIFIED: TraceParent/TraceState on Event
|
+-- cmd/
    +-- dagnats-engine/main.go      # MODIFIED: call SetupTelemetry
    +-- dagnats-api/main.go         # MODIFIED: call SetupTelemetry
```

No consumer binary. No consumer framework. No provider packages. Everything
is embedded in existing binaries.

### Dependency Rules

- `observe/` — stdlib only (interfaces + noop). Applies to `observe/*.go` files,
  not sub-packages.
- `observe/simple/` — depends on nats.go, `protocol/`, and stdlib. No OTEL SDK.
  The `protocol/` import is needed for `InjectTraceContext`/`ExtractTraceContext`
  which read and write `protocol.Event` fields.
- `engine/`, `worker/`, `api/` — depend on `observe/` interfaces only (never
  import `simple/` directly)
- OTEL wiring happens exclusively in `cmd/` binaries via `SetupTelemetry()`

### Complexity Budget

| Component | LOC | Dependencies |
|-----------|-----|-------------|
| trace_collector.go | ~80 | nats.go, crypto/rand |
| metrics_collector.go | ~60 | nats.go |
| log_collector.go | ~50 | nats.go |
| error_reporter.go | ~30 | observe interfaces |
| propagation.go | ~40 | protocol/, stdlib |
| jaeger.go | ~70 | nats.go, net/http |
| monitor.go | ~40 | nats.go |
| setup.go | ~30 | nats.go, os, path/filepath |
| **Total** | **~400** | **nats.go + protocol/ + stdlib** |

## What This Design Does NOT Include

Deliberately excluded to keep complexity minimal:

- **No OTEL SDK** — pure Go, zero heavy dependencies
- **No separate consumer binary** — embedded in existing binaries
- **No consumer framework** — no Runner, no ConfigManager, no DLQ
- **No multiple providers** — Jaeger only. Add more later if needed.
- **No protobuf** — JSON always, human-debuggable
- **No KV config for telemetry** — env vars only (JAEGER_ENDPOINT)
- **No secrets management** — endpoint is an env var, no auth tokens (yet)
- **No retry/backoff on export** — log and continue
- **No configurable retention** — 7 days from stream limits

These can be added incrementally if the need arises. The interfaces are stable
and the simple implementation can be replaced without changing instrumented code.
