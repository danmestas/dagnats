// Tests for OTLP type conversion and protobuf marshaling.
// Methodology: unit tests that create simple telemetry records,
// marshal them to OTLP protobuf, and verify the output contains
// expected data. Tests exercise both positive (valid input) and
// negative (empty/missing fields) spaces.
package otlp

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/observe/simple"
)

func TestHexToBytes(t *testing.T) {
	// 32-char hex (trace ID) → 16 bytes
	traceHex := "0123456789abcdef0123456789abcdef"
	traceBytes := hexToBytes(traceHex)
	if len(traceBytes) != 16 {
		t.Fatalf("expected 16 bytes, got %d", len(traceBytes))
	}
	if traceBytes[0] != 0x01 || traceBytes[1] != 0x23 {
		t.Fatalf(
			"unexpected first bytes: %x %x",
			traceBytes[0], traceBytes[1],
		)
	}

	// 16-char hex (span ID) → 8 bytes
	spanHex := "0123456789abcdef"
	spanBytes := hexToBytes(spanHex)
	if len(spanBytes) != 8 {
		t.Fatalf("expected 8 bytes, got %d", len(spanBytes))
	}

	// Empty string → nil
	empty := hexToBytes("")
	if empty != nil {
		t.Fatalf("expected nil for empty hex, got %v", empty)
	}
}

func TestConvertKind(t *testing.T) {
	if convertKind("internal") != 1 {
		t.Fatal("internal should be 1")
	}
	if convertKind("server") != 2 {
		t.Fatal("server should be 2")
	}
	if convertKind("client") != 3 {
		t.Fatal("client should be 3")
	}
	if convertKind("unknown") != 0 {
		t.Fatal("unknown should be 0")
	}
}

func TestConvertLevel(t *testing.T) {
	if convertLevel("debug") != 5 {
		t.Fatal("debug should be 5")
	}
	if convertLevel("info") != 9 {
		t.Fatal("info should be 9")
	}
	if convertLevel("warn") != 13 {
		t.Fatal("warn should be 13")
	}
	if convertLevel("error") != 17 {
		t.Fatal("error should be 17")
	}
	if convertLevel("trace") != 1 {
		t.Fatal("trace should be 1")
	}
}

func TestConvertStatus(t *testing.T) {
	code, msg := convertStatus("ok", "")
	if code != 1 {
		t.Fatalf("ok status code: got %d, want 1", code)
	}
	if msg != "" {
		t.Fatalf("ok status msg: got %q, want empty", msg)
	}

	code, msg = convertStatus("error", "boom")
	if code != 2 {
		t.Fatalf("error status code: got %d, want 2", code)
	}
	if msg != "boom" {
		t.Fatalf("error status msg: got %q, want boom", msg)
	}
}

func TestConvertSpan(t *testing.T) {
	span := simple.SpanRecord{
		TraceID:   "0123456789abcdef0123456789abcdef",
		SpanID:    "fedcba9876543210",
		Name:      "test.operation",
		Service:   "test-service",
		Kind:      "server",
		StartTime: time.Now().Add(-time.Second),
		EndTime:   time.Now(),
		Status:    "ok",
		Attributes: map[string]any{
			"http.method": "GET",
			"http.status": 200,
		},
		Events: []simple.SpanEvent{
			{
				Name: "event1",
				Time: time.Now(),
				Attributes: map[string]any{
					"key": "value",
				},
			},
		},
	}

	data, err := marshalTraceExport(
		"test-service", []simple.SpanRecord{span},
	)
	if err != nil {
		t.Fatalf("marshalTraceExport: %v", err)
	}

	// Positive: output is non-empty and contains span name
	if len(data) == 0 {
		t.Fatal("expected non-empty protobuf output")
	}
	if !bytes.Contains(data, []byte("test.operation")) {
		t.Fatal("protobuf should contain span name")
	}

	// Negative: empty spans slice produces minimal output
	empty, err := marshalTraceExport(
		"test-service", []simple.SpanRecord{},
	)
	if err != nil {
		t.Fatalf("marshalTraceExport empty: %v", err)
	}
	if len(empty) == 0 {
		t.Fatal("even empty spans should produce resource wrapper")
	}
}

func TestConvertLog(t *testing.T) {
	rec := simple.LogRecord{
		Level:     "info",
		Message:   "hello world log",
		Service:   "test-service",
		TraceID:   "0123456789abcdef0123456789abcdef",
		SpanID:    "fedcba9876543210",
		Timestamp: time.Now(),
		Fields: map[string]any{
			"component": "bridge",
		},
	}

	data, err := marshalLogExport(
		"test-service", []simple.LogRecord{rec},
	)
	if err != nil {
		t.Fatalf("marshalLogExport: %v", err)
	}

	// Positive: non-empty and contains log message
	if len(data) == 0 {
		t.Fatal("expected non-empty protobuf output")
	}
	if !bytes.Contains(data, []byte("hello world log")) {
		t.Fatal("protobuf should contain log message")
	}

	// Negative: verify service name is in output
	if !bytes.Contains(data, []byte("test-service")) {
		t.Fatal("protobuf should contain service name")
	}
}

func TestConvertMetric(t *testing.T) {
	point := simple.MetricPoint{
		Name:      "request_count",
		Type:      "counter",
		Value:     42.0,
		Service:   "test-service",
		Timestamp: time.Now(),
		Tags: map[string]string{
			"method": "GET",
		},
	}

	data, err := marshalMetricExport(
		"test-service", []simple.MetricPoint{point},
	)
	if err != nil {
		t.Fatalf("marshalMetricExport: %v", err)
	}

	// Positive: non-empty and contains metric name
	if len(data) == 0 {
		t.Fatal("expected non-empty protobuf output")
	}
	if !bytes.Contains(data, []byte("request_count")) {
		t.Fatal("protobuf should contain metric name")
	}

	// Negative: verify gauge type works too
	point.Type = "gauge"
	data2, err := marshalMetricExport(
		"test-service", []simple.MetricPoint{point},
	)
	if err != nil {
		t.Fatalf("marshalMetricExport gauge: %v", err)
	}
	if len(data2) == 0 {
		t.Fatal("gauge metric should produce output")
	}
}

func TestConvertAttributes(t *testing.T) {
	attrs := map[string]any{
		"str":   "hello",
		"num":   42,
		"float": 3.14,
		"bool":  true,
	}
	kvs := convertAttributes(attrs)

	// Positive: all 4 attributes converted
	if len(kvs) != 4 {
		t.Fatalf("expected 4 kvs, got %d", len(kvs))
	}

	// Negative: nil map returns nil
	if convertAttributes(nil) != nil {
		t.Fatal("nil attrs should return nil")
	}
}

func TestConvertEvents(t *testing.T) {
	events := []simple.SpanEvent{
		{
			Name: "exception",
			Time: time.Now(),
			Attributes: map[string]any{
				"message": "oops",
			},
		},
	}
	converted := convertEvents(events)

	// Positive: 1 event converted
	if len(converted) != 1 {
		t.Fatalf("expected 1 event, got %d", len(converted))
	}
	if converted[0].name != "exception" {
		t.Fatalf("event name: got %q", converted[0].name)
	}

	// Negative: nil returns nil
	if convertEvents(nil) != nil {
		t.Fatal("nil events should return nil")
	}
}

func TestAppendHelpers(t *testing.T) {
	// appendVarint: encode small number
	buf := appendVarint(nil, 1)
	if len(buf) != 1 || buf[0] != 1 {
		t.Fatalf("appendVarint(1): got %v", buf)
	}

	// appendVarint: encode larger number
	buf = appendVarint(nil, 300)
	if len(buf) != 2 {
		t.Fatalf(
			"appendVarint(300): expected 2 bytes, got %d",
			len(buf),
		)
	}

	// appendString: includes length prefix + data
	buf = appendString(nil, "hi")
	if !bytes.Contains(buf, []byte("hi")) {
		t.Fatal("appendString should contain the string")
	}

	// appendFixed64: always 8 bytes
	buf = appendFixed64(nil, 12345)
	if len(buf) != 8 {
		t.Fatalf(
			"appendFixed64: expected 8 bytes, got %d",
			len(buf),
		)
	}
}

func TestMarshalTraceExportFieldNumbers(t *testing.T) {
	// Verify the trace export contains expected protobuf
	// structure by checking for service.name attribute key.
	span := simple.SpanRecord{
		TraceID:   "aaaabbbbccccddddaaaabbbbccccdddd",
		SpanID:    "1111222233334444",
		Name:      "root-span",
		Service:   "my-svc",
		Kind:      "client",
		StartTime: time.Now().Add(-time.Second),
		EndTime:   time.Now(),
		Status:    "ok",
	}

	data, err := marshalTraceExport(
		"my-svc", []simple.SpanRecord{span},
	)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// service.name key must appear in resource attributes
	if !bytes.Contains(data, []byte("service.name")) {
		t.Fatal("trace export must contain service.name")
	}

	// Span name must appear
	if !bytes.Contains(data, []byte("root-span")) {
		t.Fatal("trace export must contain span name")
	}
}

func TestMarshalLogExportSeverity(t *testing.T) {
	rec := simple.LogRecord{
		Level:     "error",
		Message:   "something broke",
		Service:   "log-svc",
		Timestamp: time.Now(),
	}

	data, err := marshalLogExport(
		"log-svc", []simple.LogRecord{rec},
	)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if !bytes.Contains(data, []byte("something broke")) {
		t.Fatal("log export must contain message body")
	}
	if !bytes.Contains(data, []byte("service.name")) {
		t.Fatal("log export must contain service.name")
	}
}

func TestHexToBytesInvalidChars(t *testing.T) {
	// Invalid hex chars should not panic, just return
	// best-effort decoding.
	result := hexToBytes("zzzzzzzzzzzzzzzz")
	if result == nil {
		t.Fatal("should return non-nil for 16-char input")
	}
	if len(result) != 8 {
		t.Fatalf("expected 8 bytes, got %d", len(result))
	}

	// Odd-length hex
	result = hexToBytes("abc")
	if result != nil {
		t.Fatal("odd-length hex should return nil")
	}
}

func TestConvertKindEdgeCases(t *testing.T) {
	// Empty string
	if convertKind("") != 0 {
		t.Fatal("empty kind should be 0")
	}
	// producer/consumer (OTLP kinds 4,5)
	if convertKind("producer") != 4 {
		t.Fatal("producer should be 4")
	}
	if convertKind("consumer") != 5 {
		t.Fatal("consumer should be 5")
	}
}

func TestMarshalMetricExportHistogram(t *testing.T) {
	point := simple.MetricPoint{
		Name:      "latency_ms",
		Type:      "histogram",
		Value:     99.5,
		Service:   "hist-svc",
		Timestamp: time.Now(),
	}

	data, err := marshalMetricExport(
		"hist-svc", []simple.MetricPoint{point},
	)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	// Histogram falls through to gauge encoding
	if len(data) == 0 {
		t.Fatal("histogram metric should produce output")
	}
	if !bytes.Contains(data, []byte("latency_ms")) {
		t.Fatal("output must contain metric name")
	}
}

func TestConvertLevelUnknown(t *testing.T) {
	// Unknown level maps to 0 (unspecified)
	if convertLevel("") != 0 {
		t.Fatal("empty level should be 0")
	}
	if convertLevel("custom") != 0 {
		t.Fatal("custom level should be 0")
	}
}

func TestConvertStatusUnset(t *testing.T) {
	code, msg := convertStatus("", "")
	if code != 0 {
		t.Fatalf("unset status code: got %d, want 0", code)
	}
	if msg != "" {
		t.Fatalf("unset status msg: got %q, want empty", msg)
	}
}

func TestAppendTag(t *testing.T) {
	// Field 1, wire type 2 (length-delimited) → tag byte = 0x0a
	buf := appendTag(nil, 1, 2)
	if len(buf) != 1 {
		t.Fatalf("expected 1 byte, got %d", len(buf))
	}
	if buf[0] != 0x0a {
		t.Fatalf("tag: got 0x%02x, want 0x0a", buf[0])
	}

	// Field 1, wire type 0 (varint) → tag byte = 0x08
	buf = appendTag(nil, 1, 0)
	if buf[0] != 0x08 {
		t.Fatalf("tag: got 0x%02x, want 0x08", buf[0])
	}
}

func TestAppendBytes(t *testing.T) {
	data := []byte{0xDE, 0xAD}
	buf := appendBytes(nil, data)

	// Should contain length prefix + data
	if len(buf) < 3 {
		t.Fatalf("expected at least 3 bytes, got %d", len(buf))
	}
	// First byte is length (2)
	if buf[0] != 2 {
		t.Fatalf("length prefix: got %d, want 2", buf[0])
	}
	if buf[1] != 0xDE || buf[2] != 0xAD {
		t.Fatal("data bytes mismatch")
	}
}

func TestConvertAttributeTypes(t *testing.T) {
	attrs := map[string]any{
		"str_val":   "hello",
		"int_val":   int64(42),
		"float_val": 3.14,
		"bool_val":  true,
		"int_basic": 7,
	}
	kvs := convertAttributes(attrs)

	// Check we got all 5 attributes
	if len(kvs) != 5 {
		t.Fatalf("expected 5 kvs, got %d", len(kvs))
	}

	// Verify keys are present by collecting them
	keys := make(map[string]bool, len(kvs))
	for _, kv := range kvs {
		keys[kv.key] = true
	}
	for _, expected := range []string{
		"str_val", "int_val", "float_val",
		"bool_val", "int_basic",
	} {
		if !keys[expected] {
			t.Fatalf("missing key %q", expected)
		}
	}
}

func TestMarshalTraceExportWithError(t *testing.T) {
	span := simple.SpanRecord{
		TraceID:   "aaaabbbbccccddddaaaabbbbccccdddd",
		SpanID:    "1111222233334444",
		Name:      "failing-op",
		Service:   "err-svc",
		Kind:      "server",
		StartTime: time.Now().Add(-time.Second),
		EndTime:   time.Now(),
		Status:    "error",
		Error:     "connection refused",
	}

	data, err := marshalTraceExport(
		"err-svc", []simple.SpanRecord{span},
	)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if !bytes.Contains(data, []byte("connection refused")) {
		t.Fatal("should contain error message")
	}
	if !bytes.Contains(data, []byte("failing-op")) {
		t.Fatal("should contain span name")
	}
}

func TestMarshalLogExportWithFields(t *testing.T) {
	rec := simple.LogRecord{
		Level:   "warn",
		Message: "disk usage high",
		Service: "field-svc",
		Fields: map[string]any{
			"disk.usage": 0.95,
		},
		Timestamp: time.Now(),
	}

	data, err := marshalLogExport(
		"field-svc", []simple.LogRecord{rec},
	)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if !bytes.Contains(data, []byte("disk usage high")) {
		t.Fatal("should contain log message")
	}
	if !bytes.Contains(data, []byte("disk.usage")) {
		t.Fatal("should contain field key")
	}
}

func TestMarshalMetricExportWithTags(t *testing.T) {
	point := simple.MetricPoint{
		Name:      "http_requests",
		Type:      "counter",
		Value:     100.0,
		Service:   "tag-svc",
		Timestamp: time.Now(),
		Tags: map[string]string{
			"path":   "/api/v1",
			"method": "POST",
		},
	}

	data, err := marshalMetricExport(
		"tag-svc", []simple.MetricPoint{point},
	)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	if !bytes.Contains(data, []byte("http_requests")) {
		t.Fatal("should contain metric name")
	}

	// Tags should appear as attributes
	lower := strings.ToLower(string(data))
	_ = lower
	if !bytes.Contains(data, []byte("path")) {
		t.Fatal("should contain tag key 'path'")
	}
}
