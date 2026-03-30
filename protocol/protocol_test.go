// engine/events_test.go
// Tests for workflow event types: serialization roundtrips, required fields,
// and event type classification.
// Methodology: construct events, marshal to JSON, unmarshal back, and verify
// all fields survive the roundtrip. Check required field validation.
package protocol

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestEventJSONRoundTrip(t *testing.T) {
	original := Event{
		Type: EventStepCompleted, RunID: "run-123", StepID: "step-a",
		Timestamp: time.Now().UTC().Truncate(time.Millisecond),
		Payload:   json.RawMessage(`{"result":"ok"}`),
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	var decoded Event
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if decoded.Type != original.Type {
		t.Fatalf("Type = %q, want %q", decoded.Type, original.Type)
	}
	if decoded.RunID != original.RunID {
		t.Fatalf("RunID = %q, want %q", decoded.RunID, original.RunID)
	}
	if decoded.StepID != original.StepID {
		t.Fatalf("StepID = %q, want %q", decoded.StepID, original.StepID)
	}
	if !decoded.Timestamp.Equal(original.Timestamp) {
		t.Fatalf("Timestamp = %v, want %v", decoded.Timestamp, original.Timestamp)
	}
}

func TestEventTypeConstants(t *testing.T) {
	types := []EventType{
		EventWorkflowStarted, EventStepQueued, EventStepStarted,
		EventStepCompleted, EventStepFailed, EventStepContinue,
		EventAgentLoopIteration, EventWorkflowCompleted, EventWorkflowFailed,
	}
	seen := make(map[EventType]bool, len(types))
	for _, et := range types {
		if et == "" {
			t.Fatal("EventType must not be empty")
		}
		if seen[et] {
			t.Fatalf("duplicate EventType: %q", et)
		}
		seen[et] = true
	}
}

func TestNewStepCompletedEvent(t *testing.T) {
	evt := NewStepEvent(EventStepCompleted, "run-1", "step-a", []byte(`"output"`))
	if evt.Type != EventStepCompleted {
		t.Fatalf("Type = %q, want %q", evt.Type, EventStepCompleted)
	}
	if evt.RunID != "run-1" {
		t.Fatalf("RunID = %q, want %q", evt.RunID, "run-1")
	}
	if evt.StepID != "step-a" {
		t.Fatalf("StepID = %q, want %q", evt.StepID, "step-a")
	}
	if evt.Timestamp.IsZero() {
		t.Fatal("Timestamp must not be zero")
	}
	if evt.Payload == nil {
		t.Fatal("Payload must not be nil")
	}
}

func TestNATSSubjectForEvent(t *testing.T) {
	evt := Event{RunID: "run-abc", Type: EventStepCompleted}
	subject := evt.NATSSubject()
	expected := "history.run-abc"
	if subject != expected {
		t.Fatalf("NATSSubject() = %q, want %q", subject, expected)
	}
	// Negative: empty RunID should panic
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for empty RunID")
		}
	}()
	empty := Event{RunID: "", Type: EventStepCompleted}
	empty.NATSSubject()
}

func TestNATSMsgID(t *testing.T) {
	evt := Event{RunID: "run-1", StepID: "step-a", Type: EventStepCompleted}
	msgID := evt.NATSMsgID()
	if msgID != "run-1.step-a.step.completed" {
		t.Fatalf("NATSMsgID() = %q, want %q", msgID, "run-1.step-a.step.completed")
	}
}

func TestEventTraceContextOmitEmpty(t *testing.T) {
	// Events without trace context should serialize identically to before
	evt := NewWorkflowEvent(EventWorkflowStarted, "run-1", nil)
	data, err := evt.Marshal()
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	// trace_parent and trace_state should NOT appear in JSON
	if bytes.Contains(data, []byte("trace_parent")) {
		t.Fatal("empty TraceParent should be omitted from JSON")
	}

	// Events WITH trace context should include both fields
	evt.TraceParent = "00-abc-def-01"
	evt.TraceState = "vendor=value"
	data, err = evt.Marshal()
	if err != nil {
		t.Fatalf("Marshal with trace: %v", err)
	}
	if !bytes.Contains(data, []byte(`"trace_parent"`)) {
		t.Fatal("non-empty TraceParent should appear in JSON")
	}
}

func TestNATSMsgIDWorkflowEvent(t *testing.T) {
	// Workflow events have empty StepID; should not produce double dots.
	workflowStarted := Event{RunID: "run-1", StepID: "", Type: EventWorkflowStarted}
	msgID := workflowStarted.NATSMsgID()
	if msgID != "run-1.workflow.started" {
		t.Fatalf("NATSMsgID() for workflow.started = %q, want %q", msgID, "run-1.workflow.started")
	}

	// Verify step events still work correctly with StepID.
	stepEvent := Event{RunID: "run-1", StepID: "step-b", Type: EventStepStarted}
	msgID = stepEvent.NATSMsgID()
	if msgID != "run-1.step-b.step.started" {
		t.Fatalf("NATSMsgID() for step.started = %q, want %q", msgID, "run-1.step-b.step.started")
	}
}
