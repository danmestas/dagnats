// engine/events_test.go
// Tests for workflow event types: serialization roundtrips, required fields,
// and event type classification.
// Methodology: construct events, marshal to JSON, unmarshal back, and verify
// all fields survive the roundtrip. Check required field validation.
package engine

import (
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
