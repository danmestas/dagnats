// protocol/run_event_test.go
// Tests for RunEvent: JSON schema (snake_case wire tags), the terminal
// event-type constants, and the CompletedAt/Labels/TraceParent optionality
// consumers of event.run.* depend on.
// Methodology: construct RunEvent values, marshal to JSON, assert on the
// raw wire shape (field names) and on unmarshal round-trip fidelity.
package protocol

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRunEventJSONRoundTrip(t *testing.T) {
	completedAt := time.Now().UTC().Truncate(time.Millisecond)
	original := RunEvent{
		Type:        RunEventCompleted,
		RunID:       "run-123",
		WorkflowID:  "my-workflow",
		Status:      "completed",
		CreatedAt:   completedAt.Add(-time.Minute),
		CompletedAt: &completedAt,
		TraceParent: "00-abc-def-01",
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	var decoded RunEvent
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if decoded.Type != original.Type {
		t.Fatalf("Type = %q, want %q", decoded.Type, original.Type)
	}
	if decoded.RunID != original.RunID {
		t.Fatalf("RunID = %q, want %q", decoded.RunID, original.RunID)
	}
	if decoded.CompletedAt == nil || !decoded.CompletedAt.Equal(*original.CompletedAt) {
		t.Fatalf("CompletedAt = %v, want %v", decoded.CompletedAt, original.CompletedAt)
	}
}

// TestRunEventWireTagsAreSnakeCase pins the exact JSON field names the
// consumer contract (docs/wire-protocol.md) documents. A drift here is a
// breaking wire change for every event.run.* consumer.
func TestRunEventWireTagsAreSnakeCase(t *testing.T) {
	completedAt := time.Now().UTC().Truncate(time.Millisecond)
	evt := RunEvent{
		Type:        RunEventFailed,
		RunID:       "run-1",
		WorkflowID:  "wf-1",
		Status:      "failed",
		CreatedAt:   completedAt,
		CompletedAt: &completedAt,
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("Unmarshal into map failed: %v", err)
	}
	for _, field := range []string{
		"type", "run_id", "workflow_id", "status",
		"created_at", "completed_at",
	} {
		if _, ok := raw[field]; !ok {
			t.Fatalf("expected wire field %q, got keys %v", field, raw)
		}
	}
	if _, ok := raw["labels"]; ok {
		t.Fatalf("labels must be omitempty when nil, got %v", raw)
	}
	if _, ok := raw["trace_parent"]; ok {
		t.Fatalf("trace_parent must be omitempty when empty, got %v", raw)
	}
}

// TestRunEventTypeConstants pins the exact subject/type strings the
// consumer contract documents.
func TestRunEventTypeConstants(t *testing.T) {
	cases := map[RunEventType]string{
		RunEventCompleted: "run.completed",
		RunEventFailed:    "run.failed",
		RunEventCancelled: "run.cancelled",
	}
	for got, want := range cases {
		if string(got) != want {
			t.Fatalf("RunEvent type constant = %q, want %q", got, want)
		}
	}
}
