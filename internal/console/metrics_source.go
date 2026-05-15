package console

import (
	"time"
)

// MetricsSource is the read-side surface the console depends on for
// the metrics dashboard. It mirrors the small slice of
// internal/observe/metrics.Aggregator the console actually consults —
// keeping the boundary thin makes the dashboard testable without
// pulling the aggregator's NATS pump into the test fixtures and keeps
// the console package one indirection away from any future swap of
// the aggregator implementation.
//
// All methods are safe to call concurrently; the production
// implementation guards state with a sync.RWMutex.
type MetricsSource interface {
	// MetricNames returns every metric currently held, sorted for
	// stable output. Empty list (not nil) when nothing has been
	// ingested yet so the dashboard renders an explicit empty state.
	MetricNames() []string

	// MetricSnapshot returns the canonical name, type, description,
	// and point history for one metric. Returns nil + false when the
	// name is unknown so callers can render "no data yet" rather than
	// throwing.
	MetricSnapshot(name string) (MetricSeries, bool)

	// SubscribeMetric returns a channel that emits one MetricEvent
	// per ingest matching the filter, and a cancel function the
	// caller invokes on disconnect. Empty filter == all metrics. When
	// the underlying aggregator is closed or out of subscriber slots
	// the returned channel is already closed and cancel is a no-op.
	SubscribeMetric(filter string) (<-chan MetricEvent, func())
}

// MetricSeries is the console-visible snapshot of one metric's
// history. Mirrors the aggregator's Series type but uses console-
// local field names so external implementations can satisfy this
// interface without importing the observe package.
type MetricSeries struct {
	Name        string
	Kind        string // "counter" | "gauge" | "histogram"
	Description string
	Unit        string
	Service     string
	Points      []MetricPoint
}

// MetricPoint is one observation in a MetricSeries. Fields mirror
// metrics.Point.
type MetricPoint struct {
	Timestamp time.Time
	Value     float64
	Count     uint64
	Sum       float64
	Buckets   []MetricBucket
	Labels    map[string]string
}

// MetricBucket mirrors metrics.HistogramBucket.
type MetricBucket struct {
	UpperBound float64
	Count      uint64
}

// MetricEvent is the live-update envelope subscribers receive. Name +
// LabelsKey identify the affected (metric, labels) slot; Point is the
// new observation.
type MetricEvent struct {
	Name      string
	LabelsKey string
	Kind      string
	Point     MetricPoint
}

// Latest returns the most recent point in the series, or the zero
// MetricPoint when empty. Used by tile renderers that only care about
// the current value.
func (s MetricSeries) Latest() MetricPoint {
	if len(s.Points) == 0 {
		return MetricPoint{}
	}
	return s.Points[len(s.Points)-1]
}
