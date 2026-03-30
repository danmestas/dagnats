# OpenTelemetry + NATS Observability Design

Full observability for DagNats: distributed tracing, metrics, and structured logging
backed by OpenTelemetry SDK with NATS JetStream as the buffer layer. Telemetry flows
from instrumented components through NATS streams to external backends via independent
consumer processes.

## Decisions

- **Approach:** Keep existing `observe` interfaces, add `Tracer`, implement all with
  OTEL SDK adapters (Approach 2 from brainstorming)
- **Buffer:** NATS JetStream stores all telemetry (configurable retention, max 30 days)
- **Export:** Self-contained consumer processes ship telemetry to external backends
- **Sampling:** None. Store everything. Surface storage pressure via advisories + health
  endpoint
- **Emission:** OTEL SDK default async batching (BatchSpanProcessor, PeriodicReader,
  BatchLogRecordProcessor)
- **Propagation:** W3C Trace Context in NATS headers + event payloads for replay
- **Secrets:** NATS account/user permissions restrict access to secrets KV bucket
- **Failure handling:** Retry with exponential backoff, then dead letter queue

## Architecture Overview

```
Component (engine/worker/api)
    |
    v
observe.Tracer / Logger / Metrics  (interfaces)
    |
    v
observe/otel/ adapters  (OTEL SDK)
    |
    v
observe/otel/natsexporter/  (custom OTEL exporters)
    |
    v
NATS JetStream streams  (TELEMETRY_TRACES, TELEMETRY_METRICS, TELEMETRY_LOGS)
    |
    v
Consumer processes  (one per export destination)
    |                       |  (on failure after retries)
    v                       v
External backend       TELEMETRY_DLQ + advisory alert
(Jaeger/Tempo/Prom)
```

**Core Principle:** Engine, worker, and API never import OTEL packages. They depend
only on `observe` interfaces. OTEL wiring happens in `cmd/` binaries via
`SetupTelemetry()`.

## Data Model

### NATS Streams

#### TELEMETRY_TRACES

- Subjects: `telemetry.traces.{service}.{run_id}`
- Services: `engine`, `worker`, `api`
- Payload: OTEL span data (protobuf default, JSON for development)
- Retention: Configurable by message age (default 7 days, max 30 days)
- Storage: File, limits policy
- Deduplication: 5s window via `Nats-Msg-Id: {trace_id}.{span_id}`

#### TELEMETRY_METRICS

- Subjects: `telemetry.metrics.{service}.{metric_name}`
- Payload: OTEL metric data points (counter/histogram/gauge values)
- Retention: Same as traces (configurable, default 7 days)
- Storage: File, limits policy
- Published on SDK export interval (default 60s batches)

#### TELEMETRY_LOGS

- Subjects: `telemetry.logs.{service}.{level}.{run_id}`
- Levels: `debug`, `info`, `warn`, `error`
- Payload: OTEL log record with trace context and attributes
- Retention: Same as traces/metrics
- Deduplication: 5s window via `Nats-Msg-Id: {run_id}.{timestamp_ns}.{hash}`

#### TELEMETRY_DLQ

- Subject: `telemetry.dlq.{consumer_name}.{signal}`
- Payload: Failed export batch + metadata (destination, error, attempts)
- Retention: 30 days (longer than telemetry streams)
- Used for manual replay after fixing downstream issues

### KV Buckets

#### telemetry_consumers

- Key: consumer name (e.g., `jaeger-prod`)
- Value: JSON config (provider, signals, settings, enabled flag)
- Permissions: consumers read, admin writes

#### telemetry_secrets

- Key: `{consumer_name}.{secret_key}` (e.g., `jaeger-prod.auth_token`)
- Value: secret string
- Permissions: consumers read own keys only, admin writes

### Trace Context in Events

Existing `protocol.Event` gains two fields for W3C trace context:

```go
type Event struct {
    // ... existing fields ...
    TraceParent string // W3C traceparent header value
    TraceState  string // W3C tracestate header value
}
```

Stored in the history stream for replay. Propagated in NATS message headers for
runtime trace continuity.

### Telemetry Configuration

```go
type TelemetryConfig struct {
    RetentionDays  int    // Default: 7, max: 30
    Format         string // "proto" (default) or "json"
    MaxStreamBytes int64  // Per-stream storage limit (default: 1GB, max: 10GB)
}

// Validate enforces bounded configuration. Panics on programmer error (invalid
// defaults), returns error on operator error (bad env var values).
func (c TelemetryConfig) Validate() error {
    // RetentionDays: 1 <= n <= 30
    // Format: must be "proto" or "json"
    // MaxStreamBytes: 1MB <= n <= 10GB
}
```

Configurable via environment variables:
- `TELEMETRY_RETENTION_DAYS=14`
- `TELEMETRY_FORMAT=json`
- `TELEMETRY_MAX_STREAM_BYTES=5368709120`

All values are validated at startup. Out-of-range values produce a clear error
message and prevent the process from starting.

## Observe Interfaces

### New: Tracer Interface

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

// Attribute is a typed key-value pair for span context.
// Must be constructed via typed constructors below — direct struct creation is
// a programmer error. The OTEL adapter panics on unsupported Value types as
// an assertion contract (TigerStyle).
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

- `observe.Logger` -- maps to OTEL logs SDK
- `observe.Metrics` -- maps to OTEL metrics SDK (Counter/Histogram/Gauge). The existing
  interface creates instruments with `Counter(name, tags)` where tags are bound at
  creation time. The OTEL adapter bridges this by pre-binding the tags as OTEL
  attributes: each `Inc()`/`Add()`/`Observe()` call records with the pre-bound
  attribute set. One OTEL instrument per unique (name, tags) combination.
- `observe.ErrorReporter` -- implemented by extracting the active span from
  `context.Context`. If a span is active: calls `span.RecordError(err)` and
  `span.SetStatus(StatusError, msg)`. If no active span: creates a new root error
  span, records the error, and ends it immediately. `CaptureMessage` follows the
  same pattern but adds the message as a span event instead of an error.

### Noop Defaults Extended

`observe/noop.go` gains `noopTracer` and `noopSpan` for safe defaults:

```go
func NewNoopTracer() Tracer { return &noopTracer{} }
```

## OTEL Adapters

### observe/otel/ Package

Implements all four observe interfaces using OTEL SDK primitives:

```go
func NewTracer(serviceName string, exporter trace.SpanExporter) observe.Tracer
func NewLogger(serviceName string, exporter logs.Exporter) observe.Logger
func NewMetrics(serviceName string, exporter metric.Exporter) observe.Metrics
func NewErrorReporter(tracer observe.Tracer) observe.ErrorReporter
```

`NewMetrics` wraps the provided `metric.Exporter` in OTEL's built-in
`PeriodicReader` (default 60s interval). The adapter does not implement
`metric.Reader` directly -- it delegates collection to the SDK's periodic
mechanism.

### Trace Context Propagation (observe/otel/propagation.go)

- **Inject:** Before publishing to NATS, extract trace context from `context.Context`,
  write W3C `traceparent` and `tracestate` to NATS message headers and event payload
- **Extract:** When consuming, read from NATS headers first (runtime), fall back to
  event payload (replay)
- W3C format in both locations for standards compliance

### SetupTelemetry Entry Point (observe/otel/setup.go)

Single function called at binary startup:

```go
func SetupTelemetry(nc *nats.Conn, serviceName string) (
    tracer observe.Tracer,
    logger observe.Logger,
    metrics observe.Metrics,
    shutdown func(context.Context) error,
    err error,
)
```

Creates OTEL providers with NATS exporters. Returns an error if JetStream
initialization or stream validation fails. Returns a shutdown function that flushes
all providers on graceful exit. Callers must call shutdown during process termination
to flush buffered telemetry -- Tracer, Logger, and Metrics do not have individual
Close methods.

## NATS Exporters

### observe/otel/natsexporter/ Package

Three custom OTEL exporters that write batches to JetStream streams.

### TraceExporter

```go
type TraceExporter struct {
    js          nats.JetStreamContext
    serviceName string
}

func (e *TraceExporter) ExportSpans(
    ctx context.Context,
    spans []trace.ReadOnlySpan,
) error
```

- Serializes spans to OTLP protobuf (or JSON based on config)
- Subject: `telemetry.traces.{serviceName}.{runID}`
- Dedup: `Nats-Msg-Id: {traceID}.{spanID}`
- Returns error on publish failure (SDK retries the batch)

### MetricExporter

```go
type MetricExporter struct {
    js          nats.JetStreamContext
    serviceName string
}

func (e *MetricExporter) Export(
    ctx context.Context,
    rm *metricdata.ResourceMetrics,
) error

func (e *MetricExporter) Temporality(
    kind metric.InstrumentKind,
) metricdata.Temporality

func (e *MetricExporter) Aggregation(
    kind metric.InstrumentKind,
) metric.Aggregation
```

Implements `metric.Exporter` from the OTEL SDK. Paired with OTEL's built-in
`PeriodicReader` which drives collection on a configurable interval (default 60s).
The exporter only handles serialization and publishing -- the SDK owns the
collection schedule.

- Subject: `telemetry.metrics.{serviceName}.{metricName}`
- Serializes metric data points to OTLP or JSON
- Publishes batch to TELEMETRY_METRICS stream

### LogExporter

```go
type LogExporter struct {
    js          nats.JetStreamContext
    serviceName string
}

func (e *LogExporter) Export(ctx context.Context, logs []logs.Record) error
```

- Subject: `telemetry.logs.{serviceName}.{level}.{runID}`
- Includes trace context for log-trace correlation
- Publishes to TELEMETRY_LOGS stream

### Serialization Format

- Production: OTLP protobuf (compact, standard)
- Development: JSON (human-readable, debuggable via `nats sub`)
- Controlled by `TELEMETRY_FORMAT` environment variable

### Backpressure

OTEL SDK owns all queuing and backpressure. Exporters publish synchronously within
SDK batch callbacks. No additional queuing in the exporter layer.

## Consumer Framework

### consumer/ Package

Core abstractions for building telemetry export consumers.

### Types

```go
// Exporter sends telemetry batches to an external destination.
type Exporter interface {
    Export(ctx context.Context, batch Batch) error
    Name() string
    Signals() []Signal
}

type Signal string

const (
    SignalTraces  Signal = "traces"
    SignalMetrics Signal = "metrics"
    SignalLogs    Signal = "logs"
)

type Batch struct {
    Signal     Signal
    Data       []byte   // Serialized OTLP or provider-specific format
    MessageIDs []string // NATS message IDs for ack/nak
}

type Config struct {
    Name     string            // Consumer identifier
    Enabled  bool              // Whether to run
    Signals  []Signal          // Which telemetry types to export
    Provider string            // "jaeger", "tempo", "prometheus", "grafanacloud"
    Settings map[string]string // Provider-specific config
}
```

### Runner

```go
type Runner struct {
    exporter Exporter
    nc       *nats.Conn
    js       nats.JetStreamContext
    config   Config
}

func (r *Runner) Start(ctx context.Context) error
func (r *Runner) Stop() error
```

Start subscribes to relevant telemetry streams based on `config.Signals`. For each
message batch: call `exporter.Export()`, ack on success, retry with backoff on failure,
publish to DLQ after exhaustion. Watches config KV for changes and reloads.

### Configuration Management

```go
type ConfigManager struct {
    configKV  nats.KeyValue // "telemetry_consumers" bucket
    secretsKV nats.KeyValue // "telemetry_secrets" bucket
}

func (m *ConfigManager) GetConfig(consumerName string) (Config, error)
func (m *ConfigManager) Watch(consumerName string) (<-chan Config, error)
func (m *ConfigManager) UpdateConfig(cfg Config) error
```

Bootstrap via request/reply on startup. Live updates via KV watch.

### Retry Policy

```go
type RetryPolicy struct {
    MaxAttempts  int           // Default: 5
    InitialDelay time.Duration // Default: 1s
    MaxDelay     time.Duration // Default: 32s
    Multiplier   float64       // Default: 2.0
}
```

On export failure: 5 attempts with application-level exponential backoff
(1s, 2s, 4s, 8s, 16s). Application-level retry is used here instead of
`NakWithDelay` because consumers export to HTTP endpoints, not NATS subjects.
The retry loop runs inside the Runner between the JetStream fetch and the HTTP
export — NATS `NakWithDelay` is not applicable since the target is external.
After exhaustion: publish batch to `TELEMETRY_DLQ`, send advisory to
`alerts.export.failed`, ack original messages.

### DLQ Entry Format

```go
type DLQEntry struct {
    ConsumerName string
    Signal       Signal
    Batch        []byte
    Destination  string
    Error        string
    Attempts     int
    FailedAt     time.Time
}
```

### Advisory Messages

Published to `alerts.export.failed` when a DLQ entry is created:

```json
{
  "consumer": "jaeger-prod",
  "signal": "traces",
  "error": "connection refused",
  "dlq_subject": "telemetry.dlq.jaeger-prod.traces",
  "timestamp": "2026-03-30T10:15:30Z"
}
```

## Provider Implementations

Each provider implements `consumer.Exporter` with vendor-specific export logic.
No vendor SDKs -- all providers use standard HTTP with OTLP or provider-native
wire formats.

### Jaeger (consumer/jaeger/)

- Export: OTLP/HTTP POST to Jaeger endpoint
- Auth: Optional bearer token from secrets KV
- Signals: Traces only
- Config keys: `endpoint`, `auth_token` (secrets)

### Tempo (consumer/tempo/)

- Export: OTLP/HTTP to Grafana Tempo endpoint
- Auth: Basic auth (instance ID + API token)
- Signals: Traces only
- Config keys: `endpoint`, `username`, `api_token` (secrets)

### Prometheus (consumer/prometheus/)

- Export: Prometheus remote write protocol (Snappy-compressed protobuf)
- Auth: Optional bearer token
- Signals: Metrics only
- Config keys: `endpoint`, `auth_token` (secrets)

### Grafana Cloud (consumer/grafanacloud/)

- Export: OTLP/HTTP to Tempo (traces), Mimir (metrics), Loki (logs)
- Auth: Basic auth (instance ID + API key) for all endpoints
- Signals: Traces, metrics, and logs
- Config keys: `instance_id`, `api_token` (secrets), `traces_endpoint`,
  `metrics_endpoint`, `logs_endpoint`

### Provider Factory

```go
func NewExporter(
    config consumer.Config,
    secrets map[string]string,
) (consumer.Exporter, error)
```

Routes to provider-specific constructor based on `config.Provider`.

### Consumer Binary (cmd/dagnats-consumer/)

Single binary for all providers. Provider determined by config.

**Required environment variables:**

| Variable | Description |
|----------|-------------|
| `CONSUMER_NAME` | Consumer identifier, matches key in `telemetry_consumers` KV |
| `NATS_URL` | NATS server URL (e.g., `nats://localhost:4222`) |

All other configuration (provider, signals, endpoints, secrets) is loaded from
NATS KV at startup and reloaded on change via KV watch.

```bash
CONSUMER_NAME=jaeger-prod NATS_URL=nats://... ./dagnats-consumer
CONSUMER_NAME=grafana-cloud NATS_URL=nats://... ./dagnats-consumer
```

Message acknowledgement is owned by the Runner, not the Exporter. Provider
implementations must never ack or nak NATS messages -- they only handle HTTP
export logic.

## Instrumentation Points

### Engine (engine/orchestrator.go)

**Spans:**

| Span | When | Key Attributes |
|------|------|----------------|
| `orchestrator.handleEvent` | Every history event consumed | `run_id`, `event_type`, `step_id` |
| `orchestrator.advanceDAG` | After resolving ready steps | `run_id`, `ready_steps_count` |
| `orchestrator.enqueueTask` | Publishing task to TASK_QUEUES | `run_id`, `step_id`, `task_name` |
| `orchestrator.saveSnapshot` | KV snapshot write | `run_id`, `revision` |

**Metrics:**

- `workflow.runs.active` (gauge) -- inc on WorkflowStarted, dec on Completed/Failed
- `workflow.runs.completed` (counter)
- `workflow.runs.failed` (counter)
- `step.enqueue.count` (counter) -- per task type
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

- `step.duration_ms` (histogram) -- per task type
- `step.retries` (counter) -- per task type
- `agent.loop.iterations` (histogram) -- per workflow
- `worker.tasks.active` (gauge) -- currently executing handlers
- `nats.consumer.pending` (gauge) -- task queue depth per consumer

### API (api/rest.go, api/natsapi.go)

**Spans:**

| Span | When | Key Attributes |
|------|------|----------------|
| `api.registerWorkflow` | Workflow registration | `workflow_name` |
| `api.startRun` | New run started | `workflow_name`, `run_id` |
| `api.getRun` | Run status queried | `run_id` |

**Metrics:**

- `api.requests` (counter) -- per endpoint, per transport (REST/NATS)
- `api.request.duration_ms` (histogram) -- per endpoint
- `api.errors` (counter) -- per endpoint, per error type

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

## Security Model

### NATS Account/User Permissions

| User | Publish | Subscribe |
|------|---------|-----------|
| Engine/Worker/API | `telemetry.traces.>`, `telemetry.metrics.>`, `telemetry.logs.>` | -- |
| Consumer | `telemetry.dlq.>`, `alerts.>` | `telemetry.traces.>`, `telemetry.metrics.>`, `telemetry.logs.>` |
| Admin | `telemetry.>`, `alerts.>` | `telemetry.>`, `alerts.>` |

### KV Bucket Access

| Bucket | Consumer Read | Consumer Write | Admin Read | Admin Write |
|--------|--------------|----------------|------------|-------------|
| `telemetry_consumers` | Own key | No | All | All |
| `telemetry_secrets` | Own keys | No | All | All |

Secrets never leave NATS. Consumers read them at startup and on config reload.

## Storage Monitoring

### StorageMonitor (observe/monitor/)

Goroutine that checks stream stats periodically. Exits when context is cancelled.

```go
type StorageMonitor struct {
    js         nats.JetStreamContext
    nc         *nats.Conn
    interval   time.Duration // Default: 60s
    thresholds Thresholds
}

type Thresholds struct {
    WarnPercent  float64 // Default: 80%
    AlertPercent float64 // Default: 95%
}

// Start begins periodic monitoring. Blocks until ctx is cancelled.
// Publishes advisories to alerts.storage.{stream_name} when thresholds
// are exceeded. Updates shared state for the health endpoint.
func (m *StorageMonitor) Start(ctx context.Context) error

// Status returns the current telemetry health for the /health endpoint.
// Safe for concurrent use — called by HTTP handler while monitor runs.
func (m *StorageMonitor) Status() TelemetryHealth
```

### Advisory Messages

Published to `alerts.storage.{stream_name}`:

```json
{
  "stream": "TELEMETRY_TRACES",
  "level": "warn",
  "usage_bytes": 858993459,
  "max_bytes": 1073741824,
  "usage_percent": 80.0,
  "message": "TELEMETRY_TRACES at 80% capacity",
  "timestamp": "2026-03-30T12:00:00Z"
}
```

### Health Endpoint

`GET /health` extended with telemetry status:

```json
{
  "status": "healthy",
  "telemetry": {
    "streams": {
      "TELEMETRY_TRACES": {"messages": 15420, "bytes": 524288000, "percent": 48.8},
      "TELEMETRY_METRICS": {"messages": 8200, "bytes": 102400000, "percent": 9.5},
      "TELEMETRY_LOGS": {"messages": 32100, "bytes": 204800000, "percent": 19.1}
    },
    "consumers": {
      "jaeger-prod": {"status": "running", "lag": 12},
      "grafana-cloud": {"status": "running", "lag": 0}
    },
    "dlq": {"messages": 0}
  }
}
```

## NATS Resource Setup

`natsutil.SetupAll()` expanded to include telemetry infrastructure:

```go
func SetupTelemetryStreams(js nats.JetStreamContext, cfg TelemetryConfig) error
func SetupTelemetryKV(js nats.JetStreamContext) error
```

Called as part of `SetupAll()` alongside existing stream and KV bucket creation.

## Testing Strategy

### Layer 1: Pure Unit Tests (observe/otel/)

No NATS, no I/O. Test adapter logic:

- Tracer adapter: Start creates span, End finalizes, attributes propagated
- Logger adapter: Info/Error produce OTEL log records with correct severity + fields
- Metrics adapter: Counter.Inc produces data point, Histogram.Observe correct bucket
- Trace context: W3C traceparent inject/extract round-trips
- Negative: Invalid attributes rejected, nil span returns safe no-op

### Layer 2: NATS Exporter Integration Tests (observe/otel/natsexporter/)

Real embedded NATS per test:

- Trace exporter: spans arrive on `telemetry.traces.{service}.>` subject
- Metric exporter: data points arrive on `telemetry.metrics.{service}.>` subject
- Log exporter: records arrive on `telemetry.logs.{service}.{level}.>` subject
- Deduplication: same span ID exported twice produces one message
- Serialization: proto and JSON formats round-trip correctly
- Backpressure: export when NATS is slow, SDK queue behavior verified
- Negative: export to nonexistent stream returns error

### Layer 3: Consumer Framework Integration Tests (consumer/)

Real embedded NATS per test:

- Happy path: telemetry published, consumer exports via mock exporter, messages acked
- Retry: mock exporter fails 3x then succeeds, messages eventually exported
- DLQ: mock exporter fails all attempts, batch in TELEMETRY_DLQ, advisory published
- Config reload: KV update picked up by consumer within watch interval
- Multi-signal: consumer subscribed to traces + metrics, both exported
- Negative: disabled consumer consumes nothing

### Layer 4: Provider Integration Tests (consumer/{provider}/)

Real embedded NATS + mock HTTP server simulating provider endpoints:

- Jaeger: OTLP/HTTP POST to mock, correct headers and body format
- Tempo: Basic auth included, correct instance ID + token
- Prometheus: Remote write format, Snappy-compressed protobuf
- Grafana Cloud: Routes signals to correct endpoints
- Auth failure: 401 triggers retry then DLQ
- Negative: invalid endpoint errors surfaced cleanly

### Layer 5: End-to-End Telemetry Tests

Full pipeline with real NATS, orchestrator, workers, and telemetry:

- Trace propagation: spans for api.startRun, orchestrator.handleEvent,
  worker.executeTask share same trace ID
- Trace context in history: event payloads contain matching traceparent
- Storage advisory: tiny stream limits trigger advisory on alerts.storage.>
- Health endpoint: reflects correct stream usage and consumer status

### Testing Rules

- Each test file opens with methodology comment
- Minimum 2 assertions per test (positive + negative space)
- Bounded timeouts on all waits
- No shared NATS servers between tests
- 70-line function limit, split into named helpers
- Mock HTTP servers for provider tests (no real Jaeger/Tempo in CI)

## Project Structure

```
dagnats/
+-- observe/                        # Interfaces (stdlib only)
|   +-- observe.go                  # Logger, Metrics, ErrorReporter (unchanged)
|   +-- tracer.go                   # NEW: Tracer, Span, SpanOption, Attribute
|   +-- noop.go                     # MODIFIED: add noopTracer, noopSpan
|   +-- otel/                       # NEW: OTEL SDK adapters
|   |   +-- tracer.go               #   observe.Tracer -> OTEL tracer
|   |   +-- logger.go               #   observe.Logger -> OTEL logs
|   |   +-- metrics.go              #   observe.Metrics -> OTEL metrics
|   |   +-- error_reporter.go       #   observe.ErrorReporter -> OTEL spans
|   |   +-- propagation.go          #   W3C trace context inject/extract
|   |   +-- setup.go                #   SetupTelemetry() entry point
|   +-- otel/natsexporter/          # NEW: Custom OTEL exporters -> NATS
|   |   +-- trace_exporter.go       #   SpanExporter -> TELEMETRY_TRACES
|   |   +-- metric_exporter.go      #   MetricExporter -> TELEMETRY_METRICS
|   |   +-- log_exporter.go         #   LogExporter -> TELEMETRY_LOGS
|   +-- monitor/                    # NEW: Storage pressure monitoring
|       +-- monitor.go              #   Stream stats -> advisories + /health
|
+-- consumer/                       # NEW: Consumer framework
|   +-- consumer.go                 #   Exporter interface, Batch, Signal
|   +-- runner.go                   #   Runner: subscribe, export, retry, DLQ
|   +-- config.go                   #   ConfigManager: KV watch + request/reply
|   +-- retry.go                    #   RetryPolicy, backoff logic
|   +-- dlq.go                      #   DLQ entry format, publish, replay
|   +-- jaeger/                     #   Jaeger provider
|   |   +-- exporter.go
|   +-- tempo/                      #   Tempo provider
|   |   +-- exporter.go
|   +-- prometheus/                 #   Prometheus provider
|   |   +-- exporter.go
|   +-- grafanacloud/               #   Grafana Cloud provider
|       +-- exporter.go
|
+-- natsutil/
|   +-- conn.go                     # MODIFIED: SetupAll includes telemetry
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
    +-- dagnats-consumer/           # NEW: consumer binary
        +-- main.go
```

### Dependency Rules

- `observe/` -- stdlib only (interfaces + noop). This constraint applies to the
  `observe` Go package (`observe/*.go`), not to sub-packages. Sub-packages like
  `observe/otel/` are separate Go packages with their own import rules.
- `observe/otel/` -- depends on OTEL SDK packages
- `observe/otel/natsexporter/` -- depends on OTEL SDK + nats.go
- `consumer/` -- depends on nats.go, observe interfaces
- `consumer/{provider}/` -- depends on consumer, net/http (no vendor SDKs)
- `engine/`, `worker/`, `api/` -- depend on `observe/` interfaces only (never
  import `otel/` directly)
