// metric_exporter.go implements sdkmetric.Exporter backed by
// NATS JetStream. Each metric data point is serialized to JSON
// and published so downstream consumers can process metrics.
package natsexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/nats-io/nats.go/jetstream"
)

// MetricExporter implements sdkmetric.Exporter by publishing
// each metric as JSON to NATS JetStream. Subject pattern:
// telemetry.metrics.{serviceName}.{metricName}.
type MetricExporter struct {
	pub *Publisher
	seq atomic.Uint64
}

// NewMetricExporter creates a MetricExporter backed by the
// given JetStream connection. Panics on nil js.
func NewMetricExporter(
	js jetstream.JetStream,
) *MetricExporter {
	if js == nil {
		panic("NewMetricExporter: js must not be nil")
	}
	return &MetricExporter{pub: NewPublisher(js)}
}

// Export serializes metric data to JSON and publishes each
// metric to telemetry.metrics.{service}.{name}. Implements
// sdkmetric.Exporter.
func (e *MetricExporter) Export(
	ctx context.Context,
	rm *metricdata.ResourceMetrics,
) error {
	if rm == nil {
		return nil
	}

	svc := serviceNameFromResource(rm.Resource)

	for i := range rm.ScopeMetrics {
		sm := &rm.ScopeMetrics[i]
		for j := range sm.Metrics {
			err := e.exportMetric(ctx, svc, &sm.Metrics[j])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// Temporality returns delta temporality — each export contains
// only the change since last collection. Implements
// sdkmetric.Exporter.
func (e *MetricExporter) Temporality(
	_ metric.InstrumentKind,
) metricdata.Temporality {
	return metricdata.DeltaTemporality
}

// Aggregation returns the default aggregation for each
// instrument kind. Implements sdkmetric.Exporter.
func (e *MetricExporter) Aggregation(
	ik metric.InstrumentKind,
) metric.Aggregation {
	return metric.DefaultAggregationSelector(ik)
}

// Shutdown is a no-op — the NATS connection is owned by the
// caller. Implements sdkmetric.Exporter.
func (e *MetricExporter) Shutdown(context.Context) error {
	return nil
}

// ForceFlush is a no-op — metrics are published immediately.
// Implements sdkmetric.Exporter.
func (e *MetricExporter) ForceFlush(context.Context) error {
	return nil
}

// metricRecord is the JSON shape published to NATS.
type metricRecord struct {
	Name        string      `json:"name"`
	Description string      `json:"description,omitempty"`
	Unit        string      `json:"unit,omitempty"`
	ServiceName string      `json:"serviceName"`
	Data        interface{} `json:"data"`
	Timestamp   string      `json:"timestamp"`
}

func (e *MetricExporter) exportMetric(
	ctx context.Context,
	svc string,
	m *metricdata.Metrics,
) error {
	if m == nil {
		panic(
			"MetricExporter.exportMetric: metric must not be nil",
		)
	}
	if svc == "" {
		panic(
			"MetricExporter.exportMetric: svc must not be empty",
		)
	}

	rec := metricRecord{
		Name:        m.Name,
		Description: m.Description,
		Unit:        m.Unit,
		ServiceName: svc,
		Data:        m.Data,
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
	}

	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal metric: %w", err)
	}

	subject := fmt.Sprintf(
		"telemetry.metrics.%s.%s", svc, m.Name,
	)

	seq := e.seq.Add(1)
	msgID := fmt.Sprintf(
		"metric.%s.%d.%d", m.Name, time.Now().UnixNano(), seq,
	)

	return e.pub.Publish(ctx, subject, data, msgID)
}
