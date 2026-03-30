// observe/simple/types_test.go
// Tests for telemetry wire-format types. Methodology: verify JSON
// round-trip fidelity for SpanRecord, MetricPoint, LogRecord.
// Each test asserts both successful deserialization and field correctness.
package simple

import (
	"encoding/json"
	"testing"
	"time"
)

func TestSpanRecordJSONRoundTrip(t *testing.T) {
	original := SpanRecord{
		TraceID:    "abc123",
		SpanID:     "def456",
		ParentID:   "parent1",
		Name:       "test.span",
		Service:    "engine",
		Kind:       "internal",
		StartTime:  time.Date(2026, 3, 30, 0, 0, 0, 0, time.UTC),
		EndTime:    time.Date(2026, 3, 30, 0, 0, 1, 0, time.UTC),
		DurationMS: 1000,
		Status:     "ok",
		Attributes: map[string]any{"run_id": "r1"},
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded SpanRecord
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.TraceID != original.TraceID {
		t.Fatalf("TraceID = %q, want %q",
			decoded.TraceID, original.TraceID)
	}
	if decoded.DurationMS != 1000 {
		t.Fatalf("DurationMS = %d, want 1000", decoded.DurationMS)
	}
}

func TestMetricPointJSONRoundTrip(t *testing.T) {
	original := MetricPoint{
		Name:      "step.duration_ms",
		Type:      "histogram",
		Value:     42.5,
		Tags:      map[string]string{"task": "llm-coder"},
		Service:   "worker",
		Timestamp: time.Now().UTC(),
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded MetricPoint
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Name != "step.duration_ms" {
		t.Fatalf("Name = %q, want step.duration_ms", decoded.Name)
	}
	if decoded.Value != 42.5 {
		t.Fatalf("Value = %f, want 42.5", decoded.Value)
	}
}

func TestLogRecordJSONRoundTrip(t *testing.T) {
	original := LogRecord{
		Level:     "error",
		Message:   "step failed",
		Service:   "engine",
		TraceID:   "trace1",
		SpanID:    "span1",
		Error:     "connection refused",
		Timestamp: time.Now().UTC(),
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var decoded LogRecord
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.Level != "error" {
		t.Fatalf("Level = %q, want error", decoded.Level)
	}
	if decoded.Error != "connection refused" {
		t.Fatalf("Error = %q, want connection refused",
			decoded.Error)
	}
}
