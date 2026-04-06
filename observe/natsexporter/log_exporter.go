// log_exporter.go implements sdklog.Exporter backed by NATS
// JetStream. Each log record is serialized to JSON and published
// so downstream consumers can process structured logs.
package natsexporter

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"

	"github.com/nats-io/nats.go/jetstream"
)

const logBatchMax = 10_000

// LogExporter implements sdklog.Exporter by publishing each log
// record as JSON to NATS JetStream. Subject pattern:
// telemetry.logs.{serviceName}.{severity}.
type LogExporter struct {
	pub         *Publisher
	serviceName string
	seq         atomic.Uint64
}

// NewLogExporter creates a LogExporter backed by the given
// JetStream connection. serviceName identifies the producing
// service since log records may lack a Resource.
func NewLogExporter(
	js jetstream.JetStream, serviceName string,
) *LogExporter {
	if js == nil {
		panic("NewLogExporter: js must not be nil")
	}
	if serviceName == "" {
		panic(
			"NewLogExporter: serviceName must not be empty",
		)
	}
	return &LogExporter{
		pub:         NewPublisher(js),
		serviceName: serviceName,
	}
}

// Export serializes each log record to JSON and publishes to
// telemetry.logs.{service}.{severity}. Implements
// sdklog.Exporter.
func (e *LogExporter) Export(
	ctx context.Context,
	records []sdklog.Record,
) error {
	if len(records) == 0 {
		return nil
	}
	if len(records) > logBatchMax {
		return fmt.Errorf(
			"log batch size %d exceeds max %d",
			len(records), logBatchMax,
		)
	}

	for i := range records {
		if err := e.exportOne(ctx, &records[i]); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown is a no-op — the NATS connection is owned by the
// caller. Implements sdklog.Exporter.
func (e *LogExporter) Shutdown(context.Context) error {
	return nil
}

// ForceFlush is a no-op — records are published immediately.
// Implements sdklog.Exporter.
func (e *LogExporter) ForceFlush(context.Context) error {
	return nil
}

// logRecord is the JSON shape published to NATS. Kept simple
// and flat — downstream consumers parse this directly.
type logRecord struct {
	Timestamp   string            `json:"timestamp"`
	Severity    string            `json:"severity"`
	Body        string            `json:"body"`
	Attributes  map[string]string `json:"attributes,omitempty"`
	TraceID     string            `json:"traceId,omitempty"`
	SpanID      string            `json:"spanId,omitempty"`
	ServiceName string            `json:"serviceName"`
}

func (e *LogExporter) exportOne(
	ctx context.Context,
	r *sdklog.Record,
) error {
	if r == nil {
		panic("LogExporter.exportOne: record must not be nil")
	}

	severity := normalizeSeverity(r.SeverityText())
	attrs := extractLogAttrs(r)

	rec := logRecord{
		Timestamp: r.Timestamp().UTC().Format(
			time.RFC3339Nano,
		),
		Severity:    severity,
		Body:        r.Body().AsString(),
		Attributes:  attrs,
		ServiceName: e.serviceName,
	}

	tid := r.TraceID()
	if tid.IsValid() {
		rec.TraceID = tid.String()
	}
	sid := r.SpanID()
	if sid.IsValid() {
		rec.SpanID = sid.String()
	}

	data, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal log: %w", err)
	}

	subject := fmt.Sprintf(
		"telemetry.logs.%s.%s", e.serviceName, severity,
	)

	// Dedup ID: timestamp nanos + monotonic sequence to
	// guarantee uniqueness even for same-nanosecond records.
	seq := e.seq.Add(1)
	msgID := fmt.Sprintf(
		"log.%d.%d", r.Timestamp().UnixNano(), seq,
	)

	return e.pub.Publish(ctx, subject, data, msgID)
}

// normalizeSeverity returns a lowercase severity token safe
// for use in NATS subjects. Falls back to "info" when empty.
func normalizeSeverity(text string) string {
	if text == "" {
		return "info"
	}
	return strings.ToLower(text)
}

// extractLogAttrs walks log record attributes into a flat
// string map for JSON serialization.
func extractLogAttrs(
	r *sdklog.Record,
) map[string]string {
	if r.AttributesLen() == 0 {
		return nil
	}
	attrs := make(map[string]string, r.AttributesLen())
	r.WalkAttributes(func(kv log.KeyValue) bool {
		attrs[string(kv.Key)] = kv.Value.AsString()
		return true
	})
	return attrs
}
