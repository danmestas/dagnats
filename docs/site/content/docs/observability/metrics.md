---
title: Metrics
weight: 3
---

The `observe.Metrics` interface is a factory for named metric instruments that decouple measurement from the collection backend.

## Interface

```go
type Metrics interface {
    Counter(name string, tags map[string]string) Counter
    Histogram(name string, tags map[string]string) Histogram
    Gauge(name string, tags map[string]string) Gauge
}
```

Each factory method returns a typed instrument. Tags are label key-value pairs that the backend attaches to every observation.

## Instruments

### Counter

A monotonically increasing value. Use for request counts, error counts, retry counts.

```go
type Counter interface {
    Inc()
    Add(delta float64)
}
```

### Histogram

Records observations in configurable buckets. Use for latencies, payload sizes, queue depths.

```go
type Histogram interface {
    Observe(value float64)
}
```

### Gauge

A value that can go up or down. Use for active tasks, queue length, memory usage.

```go
type Gauge interface {
    Set(value float64)
    Inc()
    Dec()
}
```

## Standard Metrics

DagNats components emit these metrics automatically:

| Metric | Type | Component | Description |
|--------|------|-----------|-------------|
| `step.duration_ms` | Histogram | Worker | Time to execute a task handler |
| `step.retries` | Counter | Worker | Number of handler errors that triggered retry |
| `worker.tasks.active` | Gauge | Worker | Number of tasks currently being processed |
| `bridge.requests` | Counter | Bridge | Total HTTP bridge requests |
| `bridge.request.duration_ms` | Histogram | Bridge | HTTP bridge request latency |
| `bridge.ackmap.size` | Gauge | Bridge | Number of unresolved tasks in the ack map |
| `telemetry.spans.dropped` | Counter | TraceCollector | Spans dropped due to full buffer |

## Usage

Instruments are created once at construction time and reused:

```go
duration := tel.Metrics.Histogram("step.duration_ms", nil)
retries := tel.Metrics.Counter("step.retries", nil)
active := tel.Metrics.Gauge("worker.tasks.active", nil)

// In the hot path
active.Inc()
start := time.Now()
err := handler(ctx)
duration.Observe(float64(time.Since(start).Milliseconds()))
active.Dec()
if err != nil {
    retries.Inc()
}
```

Tags allow partitioning metrics by dimension:

```go
counter := tel.Metrics.Counter("requests", map[string]string{
    "task_type": "send-email",
    "status":    "success",
})
counter.Inc()
```

## Adapter Pattern

The noop implementation discards all observations. Use it for tests and environments where metrics are not needed:

```go
metrics := observe.NewNoopMetrics()
```

The internal `simple.MetricsCollector` publishes `MetricPoint` records as JSON to the NATS TELEMETRY stream at `telemetry.metrics.{service}.{name}`. For production, implement the `Metrics` interface with your preferred backend (Prometheus, OTel, StatsD):

```go
type PrometheusMetrics struct { /* ... */ }

func (p *PrometheusMetrics) Counter(
    name string, tags map[string]string,
) observe.Counter {
    // Return a Prometheus counter vec with the given labels
}
```

## Related

- [Structured Logging](/docs/observability/structured-logging) -- leveled structured logs
- [Distributed Tracing](/docs/observability/distributed-tracing) -- span-based tracing
- [Telemetry Stream](/docs/observability/telemetry-stream) -- NATS-native telemetry transport
