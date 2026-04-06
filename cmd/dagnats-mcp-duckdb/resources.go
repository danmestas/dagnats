package main

// Schema resource content for MCP clients to understand the data model.

const tracesSchema = `# Traces Schema (Parquet)

Columns:
- TraceId (VARCHAR) — Unique trace identifier
- SpanId (VARCHAR) — Unique span identifier
- ParentSpanId (VARCHAR) — Parent span ID (empty for root spans)
- SpanName (VARCHAR) — Operation name
- ServiceName (VARCHAR) — Name of the service
- SpanKind (VARCHAR) — INTERNAL, SERVER, CLIENT, PRODUCER, CONSUMER
- StatusCode (INTEGER) — 0=Unset, 1=Ok, 2=Error
- StatusMessage (VARCHAR) — Error description when StatusCode=2
- Duration (BIGINT) — Span duration in nanoseconds
- Timestamp (TIMESTAMP) — Span start time
- ResourceAttributes (MAP) — Resource-level attributes
- SpanAttributes (MAP) — Span-level attributes
- Events (LIST) — Span events
- Links (LIST) — Span links`

const logsSchema = `# Logs Schema (Parquet)

Columns:
- Timestamp (TIMESTAMP) — Log entry time
- ServiceName (VARCHAR) — Name of the service
- SeverityText (VARCHAR) — DEBUG, INFO, WARN, ERROR, FATAL
- SeverityNumber (INTEGER) — Numeric severity
- Body (VARCHAR) — Log message body
- TraceId (VARCHAR) — Associated trace ID (if any)
- SpanId (VARCHAR) — Associated span ID (if any)
- ResourceAttributes (MAP) — Resource-level attributes
- LogAttributes (MAP) — Log-level attributes`

const metricsSchema = `# Metrics Schema (Parquet)

Three views available: metrics_gauge, metrics_sum, metrics_histogram.

Common Columns:
- Timestamp (TIMESTAMP) — Metric data point time
- MetricName (VARCHAR) — Metric name
- MetricDescription (VARCHAR) — Metric description
- MetricUnit (VARCHAR) — Unit of measurement
- ServiceName (VARCHAR) — Name of the service
- Attributes (MAP) — Metric attributes
- ResourceAttributes (MAP) — Resource-level attributes

Gauge-specific: Value (DOUBLE)
Sum-specific: Value (DOUBLE), IsMonotonic (BOOLEAN)
Histogram-specific: Count (BIGINT), Sum (DOUBLE), Min (DOUBLE),
  Max (DOUBLE), BucketCounts (LIST), ExplicitBounds (LIST)`

const examplesSchema = `# Example SQL Queries

## Find slow spans (>1 second)
SELECT TraceId, SpanName, Duration / 1e9 AS duration_sec
FROM traces WHERE Duration > 1000000000
ORDER BY Duration DESC LIMIT 20

## Error rate by service
SELECT ServiceName,
  count(*) FILTER (WHERE StatusCode = 2) AS errors,
  count(*) AS total,
  round(100.0 * count(*) FILTER (WHERE StatusCode = 2)
    / count(*), 2) AS error_rate_pct
FROM traces GROUP BY ServiceName

## Recent error logs
SELECT Timestamp, ServiceName, Body
FROM logs WHERE SeverityText = 'ERROR'
ORDER BY Timestamp DESC LIMIT 50

## Latency percentiles by operation
SELECT SpanName,
  percentile_disc(0.5) WITHIN GROUP (ORDER BY Duration) AS p50,
  percentile_disc(0.95) WITHIN GROUP (ORDER BY Duration) AS p95,
  percentile_disc(0.99) WITHIN GROUP (ORDER BY Duration) AS p99,
  count(*) AS total
FROM traces GROUP BY SpanName ORDER BY p99 DESC

## Service dependency map
SELECT
  t1.ServiceName AS caller,
  t2.ServiceName AS callee,
  count(*) AS call_count
FROM traces t1
JOIN traces t2 ON t1.SpanId = t2.ParentSpanId
WHERE t1.ServiceName != t2.ServiceName
GROUP BY caller, callee ORDER BY call_count DESC`

// schemaResources maps resource URIs to their content.
var schemaResources = map[string]struct {
	name    string
	desc    string
	content string
}{
	"schema://traces": {
		name:    "Traces Schema",
		desc:    "Column definitions for the traces view",
		content: tracesSchema,
	},
	"schema://logs": {
		name:    "Logs Schema",
		desc:    "Column definitions for the logs view",
		content: logsSchema,
	},
	"schema://metrics": {
		name:    "Metrics Schema",
		desc:    "Column definitions for all metric views",
		content: metricsSchema,
	},
	"schema://examples": {
		name:    "Example Queries",
		desc:    "Example SQL queries for telemetry analysis",
		content: examplesSchema,
	},
}
