// span_exporter.go implements sdktrace.SpanExporter backed by
// NATS JetStream. Each span is serialized to standard OTLP JSON
// and published individually so downstream consumers (SigNoz,
// Jaeger, custom dashboards) can parse without custom types.
package natsexporter

import (
	"context"
	"fmt"
	"math"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/protobuf/encoding/protojson"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/nats-io/nats.go/jetstream"
)

const spanBatchMax = 10_000

// SpanExporter implements sdktrace.SpanExporter by publishing
// each span as OTLP JSON to NATS JetStream. Subject pattern:
// telemetry.spans.{serviceName}.{runID}.
type SpanExporter struct {
	pub *Publisher
}

// NewSpanExporter creates a SpanExporter backed by the given
// JetStream connection. Panics on nil js.
func NewSpanExporter(js jetstream.JetStream) *SpanExporter {
	if js == nil {
		panic("NewSpanExporter: js must not be nil")
	}
	return &SpanExporter{pub: NewPublisher(js)}
}

// ExportSpans serializes each span to OTLP JSON and publishes
// to telemetry.spans.{service}.{runID}. Implements
// sdktrace.SpanExporter.
func (e *SpanExporter) ExportSpans(
	ctx context.Context,
	spans []sdktrace.ReadOnlySpan,
) error {
	if len(spans) == 0 {
		return nil
	}
	if len(spans) > spanBatchMax {
		return fmt.Errorf(
			"span batch size %d exceeds max %d",
			len(spans), spanBatchMax,
		)
	}

	for _, s := range spans {
		if err := e.exportOne(ctx, s); err != nil {
			return err
		}
	}
	return nil
}

// Shutdown is a no-op — the NATS connection is owned by the
// caller. Implements sdktrace.SpanExporter.
func (e *SpanExporter) Shutdown(context.Context) error {
	return nil
}

func (e *SpanExporter) exportOne(
	ctx context.Context,
	s sdktrace.ReadOnlySpan,
) error {
	if s == nil {
		panic("SpanExporter.exportOne: span must not be nil")
	}

	proto := spanToProto(s)
	data, err := protojson.Marshal(proto)
	if err != nil {
		return fmt.Errorf("marshal span: %w", err)
	}

	svc := serviceNameFromResource(s.Resource())
	runID := runIDFromSpan(s)
	subject := fmt.Sprintf(
		"telemetry.spans.%s.%s", svc, runID,
	)

	tid := s.SpanContext().TraceID()
	sid := s.SpanContext().SpanID()
	msgID := fmt.Sprintf("%s.%s", tid, sid)

	return e.pub.Publish(ctx, subject, data, msgID)
}

// serviceNameFromResource extracts the service.name attribute
// from a Resource. Returns "unknown" when absent — avoids
// empty subject tokens that NATS would reject.
func serviceNameFromResource(
	res *resource.Resource,
) string {
	if res == nil {
		return "unknown"
	}
	iter := res.Iter()
	for iter.Next() {
		kv := iter.Attribute()
		if string(kv.Key) == "service.name" {
			return kv.Value.AsString()
		}
	}
	return "unknown"
}

// runIDFromSpan extracts the run ID from a span's attributes.
// Checks both "dagnats.run.id" (canonical) and "run_id" (used by
// engine/worker instrumentation). Returns "no-run" when absent
// so the subject remains valid for spans outside a workflow run.
func runIDFromSpan(s sdktrace.ReadOnlySpan) string {
	if s == nil {
		return "no-run"
	}
	for _, kv := range s.Attributes() {
		k := string(kv.Key)
		if k == "dagnats.run.id" || k == "run_id" {
			return kv.Value.AsString()
		}
	}
	return "no-run"
}

// --- Proto conversion helpers ---
// Replicate the transform logic from the OTel SDK's internal
// tracetransform package because those types are unexported.

func spanToProto(
	s sdktrace.ReadOnlySpan,
) *tracepb.Span {
	if s == nil {
		panic("spanToProto: span must not be nil")
	}

	tid := s.SpanContext().TraceID()
	sid := s.SpanContext().SpanID()

	sp := &tracepb.Span{
		TraceId:           tid[:],
		SpanId:            sid[:],
		TraceState:        s.SpanContext().TraceState().String(),
		Name:              s.Name(),
		Kind:              protoSpanKind(s.SpanKind()),
		StartTimeUnixNano: uint64(max(0, s.StartTime().UnixNano())),
		EndTimeUnixNano:   uint64(max(0, s.EndTime().UnixNano())),
		Attributes:        protoKeyValues(s.Attributes()),
		Events:            protoEvents(s.Events()),
		Status:            protoStatus(s.Status()),
		DroppedAttributesCount: clampUint32(
			s.DroppedAttributes(),
		),
		DroppedEventsCount: clampUint32(s.DroppedEvents()),
		DroppedLinksCount:  clampUint32(s.DroppedLinks()),
	}

	if psid := s.Parent().SpanID(); psid.IsValid() {
		sp.ParentSpanId = psid[:]
	}

	return sp
}

func protoStatus(
	st sdktrace.Status,
) *tracepb.Status {
	var c tracepb.Status_StatusCode
	switch st.Code {
	case codes.Ok:
		c = tracepb.Status_STATUS_CODE_OK
	case codes.Error:
		c = tracepb.Status_STATUS_CODE_ERROR
	default:
		c = tracepb.Status_STATUS_CODE_UNSET
	}
	return &tracepb.Status{
		Code:    c,
		Message: st.Description,
	}
}

func protoSpanKind(
	k trace.SpanKind,
) tracepb.Span_SpanKind {
	switch k {
	case trace.SpanKindInternal:
		return tracepb.Span_SPAN_KIND_INTERNAL
	case trace.SpanKindClient:
		return tracepb.Span_SPAN_KIND_CLIENT
	case trace.SpanKindServer:
		return tracepb.Span_SPAN_KIND_SERVER
	case trace.SpanKindProducer:
		return tracepb.Span_SPAN_KIND_PRODUCER
	case trace.SpanKindConsumer:
		return tracepb.Span_SPAN_KIND_CONSUMER
	default:
		return tracepb.Span_SPAN_KIND_UNSPECIFIED
	}
}

func protoEvents(
	events []sdktrace.Event,
) []*tracepb.Span_Event {
	if len(events) == 0 {
		return nil
	}
	out := make([]*tracepb.Span_Event, len(events))
	for i := range events {
		out[i] = &tracepb.Span_Event{
			Name:         events[i].Name,
			TimeUnixNano: uint64(max(0, events[i].Time.UnixNano())),
			Attributes:   protoKeyValues(events[i].Attributes),
			DroppedAttributesCount: clampUint32(
				events[i].DroppedAttributeCount,
			),
		}
	}
	return out
}

func protoKeyValues(
	attrs []attribute.KeyValue,
) []*commonpb.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]*commonpb.KeyValue, 0, len(attrs))
	for _, kv := range attrs {
		out = append(out, protoKeyValue(kv))
	}
	return out
}

func protoKeyValue(
	kv attribute.KeyValue,
) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   string(kv.Key),
		Value: protoValue(kv.Value),
	}
}

func protoValue(v attribute.Value) *commonpb.AnyValue {
	av := new(commonpb.AnyValue)
	switch v.Type() {
	case attribute.BOOL:
		av.Value = &commonpb.AnyValue_BoolValue{
			BoolValue: v.AsBool(),
		}
	case attribute.INT64:
		av.Value = &commonpb.AnyValue_IntValue{
			IntValue: v.AsInt64(),
		}
	case attribute.FLOAT64:
		av.Value = &commonpb.AnyValue_DoubleValue{
			DoubleValue: v.AsFloat64(),
		}
	case attribute.STRING:
		av.Value = &commonpb.AnyValue_StringValue{
			StringValue: v.AsString(),
		}
	default:
		av.Value = &commonpb.AnyValue_StringValue{
			StringValue: v.String(),
		}
	}
	return av
}

func clampUint32(v int) uint32 {
	if v < 0 {
		return 0
	}
	if int64(v) > math.MaxUint32 {
		return math.MaxUint32
	}
	return uint32(v)
}
