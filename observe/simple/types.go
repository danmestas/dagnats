// Package simple provides a minimal, self-contained telemetry
// implementation backed by NATS JetStream. Zero external dependencies
// beyond nats.go and stdlib.
package simple

import "time"

// SpanRecord is the JSON wire format for a completed trace span.
// Published to telemetry.spans.{service}.{run_id}.
type SpanRecord struct {
	TraceID    string         `json:"trace_id"`
	SpanID     string         `json:"span_id"`
	ParentID   string         `json:"parent_id,omitempty"`
	Name       string         `json:"name"`
	Service    string         `json:"service"`
	Kind       string         `json:"kind"`
	StartTime  time.Time      `json:"start_time"`
	EndTime    time.Time      `json:"end_time"`
	DurationMS int64          `json:"duration_ms"`
	Status     string         `json:"status"`
	Attributes map[string]any `json:"attributes,omitempty"`
	Events     []SpanEvent    `json:"events,omitempty"`
	Error      string         `json:"error,omitempty"`
}

// SpanEvent is a timestamped annotation within a span.
type SpanEvent struct {
	Name       string         `json:"name"`
	Time       time.Time      `json:"time"`
	Attributes map[string]any `json:"attributes,omitempty"`
}

// MetricPoint is the JSON wire format for a single metric observation.
// Published to telemetry.metrics.{service}.{metric_name}.
type MetricPoint struct {
	Name      string            `json:"name"`
	Type      string            `json:"type"`
	Value     float64           `json:"value"`
	Tags      map[string]string `json:"tags,omitempty"`
	Service   string            `json:"service"`
	Timestamp time.Time         `json:"timestamp"`
}

// LogRecord is the JSON wire format for a structured log entry.
// Published to telemetry.logs.{service}.{level}.
type LogRecord struct {
	Level     string         `json:"level"`
	Message   string         `json:"message"`
	Service   string         `json:"service"`
	TraceID   string         `json:"trace_id,omitempty"`
	SpanID    string         `json:"span_id,omitempty"`
	Fields    map[string]any `json:"fields,omitempty"`
	Timestamp time.Time      `json:"timestamp"`
	Error     string         `json:"error,omitempty"`
}
