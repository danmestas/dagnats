// Package otlp provides a bridge from the NATS TELEMETRY stream to
// an OTLP/HTTP endpoint. Hand-rolled protobuf encoding avoids any
// proto dependency. Field numbers follow the OTLP v1 specification.
package otlp

import (
	"encoding/binary"
	"math"

	"github.com/danmestas/dagnats/internal/observe/simple"
)

// Protobuf wire types used in OTLP encoding.
const (
	wireVarint  = 0
	wireFixed64 = 1
	wireBytes   = 2
)

// appendTag appends a protobuf field tag (field number + wire type).
func appendTag(buf []byte, fieldNum, wireType uint64) []byte {
	if fieldNum == 0 {
		panic("appendTag: fieldNum must not be zero")
	}
	if wireType > 5 {
		panic("appendTag: wireType out of range")
	}
	return appendVarint(buf, fieldNum<<3|wireType)
}

// appendVarint appends a base-128 varint to buf.
func appendVarint(buf []byte, val uint64) []byte {
	if val < 0x80 {
		return append(buf, byte(val))
	}
	for val >= 0x80 {
		buf = append(buf, byte(val)|0x80)
		val >>= 7
	}
	return append(buf, byte(val))
}

// appendBytes appends length-delimited bytes to buf.
func appendBytes(buf, data []byte) []byte {
	buf = appendVarint(buf, uint64(len(data)))
	return append(buf, data...)
}

// appendString appends a length-delimited string to buf.
func appendString(buf []byte, s string) []byte {
	buf = appendVarint(buf, uint64(len(s)))
	return append(buf, s...)
}

// appendFixed64 appends a 64-bit fixed-width value to buf.
func appendFixed64(buf []byte, val uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], val)
	return append(buf, b[:]...)
}

// appendFixed64Float appends a float64 as a fixed64 protobuf field.
func appendFixed64Float(buf []byte, val float64) []byte {
	return appendFixed64(buf, math.Float64bits(val))
}

// --- Key-Value encoding (shared by all signal types) ---

// marshalKeyValue encodes a single OTLP KeyValue message.
// KeyValue: field 1=key(string), field 2=AnyValue
func marshalKeyValue(kv keyValue) []byte {
	if kv.key == "" {
		panic("marshalKeyValue: key must not be empty")
	}

	var buf []byte
	// field 1: key (string)
	buf = appendTag(buf, 1, wireBytes)
	buf = appendString(buf, kv.key)
	// field 2: value (AnyValue, length-delimited)
	valBytes := marshalAnyValue(kv.value)
	buf = appendTag(buf, 2, wireBytes)
	buf = appendBytes(buf, valBytes)
	return buf
}

// marshalAnyValue encodes an OTLP AnyValue message.
// AnyValue: 1=string, 2=bool, 3=int, 4=double
func marshalAnyValue(v anyValue) []byte {
	var buf []byte
	switch v.kind {
	case valueString:
		buf = appendTag(buf, 1, wireBytes)
		buf = appendString(buf, v.str)
	case valueBool:
		buf = appendTag(buf, 2, wireVarint)
		val := uint64(0)
		if v.boolVal {
			val = 1
		}
		buf = appendVarint(buf, val)
	case valueInt:
		buf = appendTag(buf, 3, wireVarint)
		buf = appendVarint(buf, uint64(v.intVal))
	case valueDouble:
		buf = appendTag(buf, 4, wireFixed64)
		buf = appendFixed64Float(buf, v.doubleVal)
	}
	return buf
}

// marshalKeyValues encodes a slice of KeyValue messages, each
// prefixed with the given field tag.
func marshalKeyValues(
	buf []byte, fieldNum uint64, kvs []keyValue,
) []byte {
	if fieldNum == 0 {
		panic("marshalKeyValues: fieldNum must not be zero")
	}
	const maxKVs = 10000
	if len(kvs) > maxKVs {
		panic("marshalKeyValues: kvs exceeds max bound")
	}
	for _, kv := range kvs {
		encoded := marshalKeyValue(kv)
		buf = appendTag(buf, fieldNum, wireBytes)
		buf = appendBytes(buf, encoded)
	}
	return buf
}

// --- Resource encoding ---

// marshalResource encodes an OTLP Resource with service.name.
func marshalResource(serviceName string) []byte {
	if serviceName == "" {
		panic("marshalResource: serviceName must not be empty")
	}

	nameKV := keyValue{
		key:   "service.name",
		value: anyValue{kind: valueString, str: serviceName},
	}
	var buf []byte
	// field 1: repeated KeyValue attributes
	buf = marshalKeyValues(buf, 1, []keyValue{nameKV})
	return buf
}

// --- Trace export ---

// marshalTraceExport encodes an ExportTraceServiceRequest.
// ExportTraceServiceRequest: field 1 = []ResourceSpans
func marshalTraceExport(
	serviceName string, spans []simple.SpanRecord,
) ([]byte, error) {
	if serviceName == "" {
		panic("marshalTraceExport: serviceName empty")
	}
	const maxSpans = 10000
	if len(spans) > maxSpans {
		panic("marshalTraceExport: spans exceeds max bound")
	}

	rsBytes := marshalResourceSpans(serviceName, spans)
	var buf []byte
	buf = appendTag(buf, 1, wireBytes)
	buf = appendBytes(buf, rsBytes)
	return buf, nil
}

// marshalResourceSpans encodes a ResourceSpans message.
// ResourceSpans: field 1=Resource, field 2=[]ScopeSpans
func marshalResourceSpans(
	serviceName string, spans []simple.SpanRecord,
) []byte {
	if serviceName == "" {
		panic("marshalResourceSpans: serviceName empty")
	}
	const maxSpans = 10000
	if len(spans) > maxSpans {
		panic("marshalResourceSpans: spans exceeds bound")
	}

	var buf []byte
	// field 1: Resource
	resBytes := marshalResource(serviceName)
	buf = appendTag(buf, 1, wireBytes)
	buf = appendBytes(buf, resBytes)

	// field 2: ScopeSpans
	ssBytes := marshalScopeSpans(spans)
	buf = appendTag(buf, 2, wireBytes)
	buf = appendBytes(buf, ssBytes)
	return buf
}

// marshalScopeSpans encodes a ScopeSpans message.
// ScopeSpans: field 1=InstrumentationScope, field 2=[]Span
func marshalScopeSpans(
	spans []simple.SpanRecord,
) []byte {
	const maxSpans = 10000
	if len(spans) > maxSpans {
		panic("marshalScopeSpans: spans exceeds bound")
	}

	var buf []byte
	for _, s := range spans {
		spanBytes := marshalSpan(s)
		buf = appendTag(buf, 2, wireBytes)
		buf = appendBytes(buf, spanBytes)
	}
	return buf
}

// marshalSpan encodes a single OTLP Span message.
func marshalSpan(s simple.SpanRecord) []byte {
	var buf []byte
	buf = marshalSpanIDs(buf, s)
	buf = marshalSpanCore(buf, s)
	buf = marshalSpanAttrsEventsStatus(buf, s)
	return buf
}

// marshalSpanIDs encodes trace_id, span_id, parent_span_id.
func marshalSpanIDs(
	buf []byte, s simple.SpanRecord,
) []byte {
	if s.TraceID == "" {
		panic("marshalSpanIDs: TraceID must not be empty")
	}
	if s.SpanID == "" {
		panic("marshalSpanIDs: SpanID must not be empty")
	}

	// field 1: trace_id (bytes)
	traceID := hexToBytes(s.TraceID)
	buf = appendTag(buf, 1, wireBytes)
	buf = appendBytes(buf, traceID)

	// field 2: span_id (bytes)
	spanID := hexToBytes(s.SpanID)
	buf = appendTag(buf, 2, wireBytes)
	buf = appendBytes(buf, spanID)

	// field 4: parent_span_id (bytes, optional)
	if s.ParentID != "" {
		parentID := hexToBytes(s.ParentID)
		buf = appendTag(buf, 4, wireBytes)
		buf = appendBytes(buf, parentID)
	}
	return buf
}

// marshalSpanCore encodes name, kind, start/end time.
func marshalSpanCore(
	buf []byte, s simple.SpanRecord,
) []byte {
	if s.Name == "" {
		panic("marshalSpanCore: Name must not be empty")
	}

	// field 5: name (string)
	buf = appendTag(buf, 5, wireBytes)
	buf = appendString(buf, s.Name)

	// field 6: kind (varint)
	kind := convertKind(s.Kind)
	if kind > 0 {
		buf = appendTag(buf, 6, wireVarint)
		buf = appendVarint(buf, uint64(kind))
	}

	// field 7: start_time_unix_nano (fixed64)
	buf = appendTag(buf, 7, wireFixed64)
	buf = appendFixed64(
		buf, uint64(s.StartTime.UnixNano()),
	)

	// field 8: end_time_unix_nano (fixed64)
	buf = appendTag(buf, 8, wireFixed64)
	buf = appendFixed64(
		buf, uint64(s.EndTime.UnixNano()),
	)
	return buf
}

// marshalSpanAttrsEventsStatus encodes attributes, events, status.
func marshalSpanAttrsEventsStatus(
	buf []byte, s simple.SpanRecord,
) []byte {
	// field 9: repeated KeyValue attributes
	attrs := convertAttributes(s.Attributes)
	buf = marshalKeyValues(buf, 9, attrs)

	// field 11: repeated Event
	events := convertEvents(s.Events)
	for _, e := range events {
		evBytes := marshalSpanEvent(e)
		buf = appendTag(buf, 11, wireBytes)
		buf = appendBytes(buf, evBytes)
	}

	// field 15: Status
	statusCode, statusMsg := convertStatus(
		s.Status, s.Error,
	)
	if statusCode > 0 {
		stBytes := marshalStatus(statusCode, statusMsg)
		buf = appendTag(buf, 15, wireBytes)
		buf = appendBytes(buf, stBytes)
	}
	return buf
}

// marshalSpanEvent encodes an OTLP Span.Event.
// Event: field 1=time_unix_nano(fixed64), field 2=name(string),
//
//	field 3=[]KeyValue
func marshalSpanEvent(e spanEvent) []byte {
	if e.name == "" {
		panic("marshalSpanEvent: name must not be empty")
	}

	var buf []byte
	// field 1: time_unix_nano
	buf = appendTag(buf, 1, wireFixed64)
	buf = appendFixed64(
		buf, uint64(e.timeUnixNano),
	)

	// field 2: name
	buf = appendTag(buf, 2, wireBytes)
	buf = appendString(buf, e.name)

	// field 3: attributes
	buf = marshalKeyValues(buf, 3, e.attributes)
	return buf
}

// marshalStatus encodes an OTLP Status message.
// Status: field 1=message(string), field 2=code(varint)
func marshalStatus(code int, msg string) []byte {
	if code < 0 || code > 2 {
		panic("marshalStatus: code out of range")
	}

	var buf []byte
	if msg != "" {
		buf = appendTag(buf, 1, wireBytes)
		buf = appendString(buf, msg)
	}
	if code > 0 {
		buf = appendTag(buf, 2, wireVarint)
		buf = appendVarint(buf, uint64(code))
	}
	return buf
}

// --- Log export ---

// marshalLogExport encodes an ExportLogsServiceRequest.
// ExportLogsServiceRequest: field 1 = []ResourceLogs
func marshalLogExport(
	serviceName string, logs []simple.LogRecord,
) ([]byte, error) {
	if serviceName == "" {
		panic("marshalLogExport: serviceName empty")
	}
	const maxLogs = 10000
	if len(logs) > maxLogs {
		panic("marshalLogExport: logs exceeds max bound")
	}

	rlBytes := marshalResourceLogs(serviceName, logs)
	var buf []byte
	buf = appendTag(buf, 1, wireBytes)
	buf = appendBytes(buf, rlBytes)
	return buf, nil
}

// marshalResourceLogs encodes a ResourceLogs message.
// ResourceLogs: field 1=Resource, field 2=[]ScopeLogs
func marshalResourceLogs(
	serviceName string, logs []simple.LogRecord,
) []byte {
	if serviceName == "" {
		panic("marshalResourceLogs: serviceName empty")
	}
	const maxLogs = 10000
	if len(logs) > maxLogs {
		panic("marshalResourceLogs: logs exceeds bound")
	}

	var buf []byte
	resBytes := marshalResource(serviceName)
	buf = appendTag(buf, 1, wireBytes)
	buf = appendBytes(buf, resBytes)

	slBytes := marshalScopeLogs(logs)
	buf = appendTag(buf, 2, wireBytes)
	buf = appendBytes(buf, slBytes)
	return buf
}

// marshalScopeLogs encodes a ScopeLogs message.
// ScopeLogs: field 1=InstrumentationScope, field 2=[]LogRecord
func marshalScopeLogs(logs []simple.LogRecord) []byte {
	const maxLogs = 10000
	if len(logs) > maxLogs {
		panic("marshalScopeLogs: logs exceeds bound")
	}

	var buf []byte
	for _, l := range logs {
		lrBytes := marshalLogRecord(l)
		buf = appendTag(buf, 2, wireBytes)
		buf = appendBytes(buf, lrBytes)
	}
	return buf
}

// marshalLogRecord encodes a single OTLP LogRecord.
// LogRecord: field 1=time_unix_nano(fixed64),
//
//	field 2=severity_number(varint),
//	field 3=severity_text(string),
//	field 5=body(AnyValue), field 6=[]KeyValue,
//	field 9=trace_id(bytes), field 10=span_id(bytes)
func marshalLogRecord(l simple.LogRecord) []byte {
	var buf []byte
	buf = marshalLogRecordTime(buf, l)
	buf = marshalLogRecordBody(buf, l)
	buf = marshalLogRecordContext(buf, l)
	return buf
}

// marshalLogRecordTime encodes timestamp and severity.
func marshalLogRecordTime(
	buf []byte, l simple.LogRecord,
) []byte {
	if l.Level == "" {
		panic("marshalLogRecordTime: Level empty")
	}

	// field 1: time_unix_nano
	buf = appendTag(buf, 1, wireFixed64)
	buf = appendFixed64(
		buf, uint64(l.Timestamp.UnixNano()),
	)

	// field 2: severity_number
	sev := convertLevel(l.Level)
	if sev > 0 {
		buf = appendTag(buf, 2, wireVarint)
		buf = appendVarint(buf, uint64(sev))
	}

	// field 3: severity_text
	buf = appendTag(buf, 3, wireBytes)
	buf = appendString(buf, l.Level)
	return buf
}

// marshalLogRecordBody encodes body and attributes.
func marshalLogRecordBody(
	buf []byte, l simple.LogRecord,
) []byte {
	if l.Message == "" {
		panic("marshalLogRecordBody: Message empty")
	}

	// field 5: body (AnyValue with string)
	bodyVal := marshalAnyValue(anyValue{
		kind: valueString,
		str:  l.Message,
	})
	buf = appendTag(buf, 5, wireBytes)
	buf = appendBytes(buf, bodyVal)

	// field 6: attributes from Fields
	attrs := convertAttributes(l.Fields)
	buf = marshalKeyValues(buf, 6, attrs)
	return buf
}

// marshalLogRecordContext encodes trace/span context.
func marshalLogRecordContext(
	buf []byte, l simple.LogRecord,
) []byte {
	// Not strictly required, but both fields are bounded.
	if len(l.TraceID) > 64 {
		panic("marshalLogRecordContext: TraceID too long")
	}
	if len(l.SpanID) > 32 {
		panic("marshalLogRecordContext: SpanID too long")
	}

	// field 9: trace_id
	if l.TraceID != "" {
		traceID := hexToBytes(l.TraceID)
		buf = appendTag(buf, 9, wireBytes)
		buf = appendBytes(buf, traceID)
	}

	// field 10: span_id
	if l.SpanID != "" {
		spanID := hexToBytes(l.SpanID)
		buf = appendTag(buf, 10, wireBytes)
		buf = appendBytes(buf, spanID)
	}
	return buf
}

// --- Metric export ---

// marshalMetricExport encodes an ExportMetricsServiceRequest.
// ExportMetricsServiceRequest: field 1 = []ResourceMetrics
func marshalMetricExport(
	serviceName string, metrics []simple.MetricPoint,
) ([]byte, error) {
	if serviceName == "" {
		panic("marshalMetricExport: serviceName empty")
	}
	const maxMetrics = 10000
	if len(metrics) > maxMetrics {
		panic("marshalMetricExport: exceeds max bound")
	}

	rmBytes := marshalResourceMetrics(serviceName, metrics)
	var buf []byte
	buf = appendTag(buf, 1, wireBytes)
	buf = appendBytes(buf, rmBytes)
	return buf, nil
}

// marshalResourceMetrics encodes a ResourceMetrics message.
// ResourceMetrics: field 1=Resource, field 2=[]ScopeMetrics
func marshalResourceMetrics(
	serviceName string, metrics []simple.MetricPoint,
) []byte {
	if serviceName == "" {
		panic("marshalResourceMetrics: serviceName empty")
	}
	const maxMetrics = 10000
	if len(metrics) > maxMetrics {
		panic("marshalResourceMetrics: exceeds bound")
	}

	var buf []byte
	resBytes := marshalResource(serviceName)
	buf = appendTag(buf, 1, wireBytes)
	buf = appendBytes(buf, resBytes)

	smBytes := marshalScopeMetrics(metrics)
	buf = appendTag(buf, 2, wireBytes)
	buf = appendBytes(buf, smBytes)
	return buf
}

// marshalScopeMetrics encodes a ScopeMetrics message.
// ScopeMetrics: field 1=InstrumentationScope, field 2=[]Metric
func marshalScopeMetrics(
	metrics []simple.MetricPoint,
) []byte {
	const maxMetrics = 10000
	if len(metrics) > maxMetrics {
		panic("marshalScopeMetrics: exceeds bound")
	}

	var buf []byte
	for _, m := range metrics {
		mBytes := marshalMetric(m)
		buf = appendTag(buf, 2, wireBytes)
		buf = appendBytes(buf, mBytes)
	}
	return buf
}

// marshalMetric encodes a single OTLP Metric message.
// Metric: field 1=name(string), field 5=Gauge, field 7=Sum
func marshalMetric(m simple.MetricPoint) []byte {
	if m.Name == "" {
		panic("marshalMetric: Name must not be empty")
	}

	var buf []byte
	// field 1: name
	buf = appendTag(buf, 1, wireBytes)
	buf = appendString(buf, m.Name)

	dpBytes := marshalNumberDataPoint(m)
	switch m.Type {
	case "counter":
		// field 7: Sum
		sumBytes := marshalSum(dpBytes)
		buf = appendTag(buf, 7, wireBytes)
		buf = appendBytes(buf, sumBytes)
	default:
		// gauge (default for gauge, histogram, etc.)
		// field 5: Gauge { field 1: []NumberDataPoint }
		var gaugeBody []byte
		gaugeBody = appendTag(gaugeBody, 1, wireBytes)
		gaugeBody = appendBytes(gaugeBody, dpBytes)
		buf = appendTag(buf, 5, wireBytes)
		buf = appendBytes(buf, gaugeBody)
	}
	return buf
}

// marshalSum encodes an OTLP Sum message wrapping a data point.
// Sum: field 1=[]NumberDataPoint, field 2=aggregation_temporality,
//
//	field 3=is_monotonic
func marshalSum(dpBytes []byte) []byte {
	if dpBytes == nil {
		panic("marshalSum: dpBytes must not be nil")
	}

	var buf []byte
	// field 1: data_points
	buf = appendTag(buf, 1, wireBytes)
	buf = appendBytes(buf, dpBytes)
	// field 2: aggregation_temporality = CUMULATIVE (2)
	buf = appendTag(buf, 2, wireVarint)
	buf = appendVarint(buf, 2)
	// field 3: is_monotonic = true
	buf = appendTag(buf, 3, wireVarint)
	buf = appendVarint(buf, 1)
	return buf
}

// marshalNumberDataPoint encodes an OTLP NumberDataPoint.
// NumberDataPoint: field 2=[]KeyValue, field 3=start_time(fixed64),
//
//	field 4=time_unix_nano(fixed64), field 7=as_double(fixed64)
func marshalNumberDataPoint(m simple.MetricPoint) []byte {
	var buf []byte

	// field 2: attributes from Tags
	attrs := convertTags(m.Tags)
	buf = marshalKeyValues(buf, 2, attrs)

	// field 3: start_time_unix_nano
	buf = appendTag(buf, 3, wireFixed64)
	buf = appendFixed64(
		buf, uint64(m.Timestamp.UnixNano()),
	)

	// field 4: time_unix_nano
	buf = appendTag(buf, 4, wireFixed64)
	buf = appendFixed64(
		buf, uint64(m.Timestamp.UnixNano()),
	)

	// field 7: as_double (fixed64)
	buf = appendTag(buf, 7, wireFixed64)
	buf = appendFixed64Float(buf, m.Value)
	return buf
}
