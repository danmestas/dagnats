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

func TestChildWorkflowEventTypes(t *testing.T) {
	// Positive: constants have correct string values
	if EventWorkflowSpawn != "workflow.spawn" {
		t.Fatalf("EventWorkflowSpawn = %q, want workflow.spawn",
			EventWorkflowSpawn)
	}
	if EventWorkflowChildCompleted != "workflow.child.completed" {
		t.Fatalf("EventWorkflowChildCompleted = %q",
			EventWorkflowChildCompleted)
	}
	if EventWorkflowChildFailed != "workflow.child.failed" {
		t.Fatalf("EventWorkflowChildFailed = %q",
			EventWorkflowChildFailed)
	}

	// Positive: can create and serialize events with new types
	evt := NewWorkflowEvent(EventWorkflowSpawn, "run-1",
		[]byte(`{"child":"c-1"}`))
	if evt.Type != EventWorkflowSpawn {
		t.Fatalf("event type = %q, want workflow.spawn", evt.Type)
	}
	if msgID := evt.NATSMsgID(); msgID == "" {
		t.Fatalf("NATSMsgID should not be empty")
	}
}

func TestTaskPayloadIncludesTaskID(t *testing.T) {
	// Positive: TaskPayload includes TaskID field
	p := TaskPayload{
		TaskID: "run-1.step-a",
		RunID:  "run-1",
		StepID: "step-a",
		Input:  []byte(`{"key":"value"}`),
	}
	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if !bytes.Contains(data, []byte("task_id")) {
		t.Fatal("marshaled JSON must contain task_id field")
	}

	// Negative: unmarshal and verify field round-trips
	var decoded TaskPayload
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if decoded.TaskID != p.TaskID {
		t.Fatalf("TaskID = %q, want %q", decoded.TaskID, p.TaskID)
	}
}

func TestTaskResolutionRoundTrip(t *testing.T) {
	// Positive: complete action with output
	res := TaskResolution{
		Action: "complete",
		Output: json.RawMessage(`{"result":"ok"}`),
	}
	data, err := json.Marshal(res)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	var decoded TaskResolution
	err = json.Unmarshal(data, &decoded)
	if err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if decoded.Action != res.Action {
		t.Fatalf("Action = %q, want %q", decoded.Action, res.Action)
	}

	// Negative: pause action with duration_ms
	pauseRes := TaskResolution{
		Action:     "pause",
		DurationMs: 5000,
	}
	data, err = json.Marshal(pauseRes)
	if err != nil {
		t.Fatalf("Marshal pause failed: %v", err)
	}
	if !bytes.Contains(data, []byte("duration_ms")) {
		t.Fatal("pause action must include duration_ms field")
	}
}

func TestStepFailedPayloadRoundTrip(t *testing.T) {
	payload := StepFailedPayload{
		Error:       "resource not found",
		FailureType: FailureTypeNonRetriable,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	var decoded StepFailedPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if decoded.FailureType != FailureTypeNonRetriable {
		t.Fatalf("FailureType = %q, want %q",
			decoded.FailureType, FailureTypeNonRetriable)
	}
	if decoded.Error != "resource not found" {
		t.Fatalf("Error = %q, want %q",
			decoded.Error, "resource not found")
	}

	var empty StepFailedPayload
	if err := json.Unmarshal([]byte(`{}`), &empty); err != nil {
		t.Fatalf("Unmarshal empty failed: %v", err)
	}
	if empty.FailureType != "" {
		t.Fatalf("empty FailureType = %q, want empty",
			empty.FailureType)
	}
}

func TestStepFailedPayloadRetryAfter(t *testing.T) {
	payload := StepFailedPayload{
		Error:        "rate limited",
		FailureType:  FailureTypeRetryAfter,
		RetryAfterMs: 5000,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if !bytes.Contains(data, []byte(`"retry_after_ms":5000`)) {
		t.Fatalf("JSON should contain retry_after_ms: %s", data)
	}

	var decoded StepFailedPayload
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if decoded.RetryAfterMs != 5000 {
		t.Fatalf("RetryAfterMs = %d, want 5000",
			decoded.RetryAfterMs)
	}

	retriable := StepFailedPayload{
		Error:       "transient",
		FailureType: FailureTypeRetriable,
	}
	data, _ = json.Marshal(retriable)
	if bytes.Contains(data, []byte("retry_after_ms")) {
		t.Fatalf(
			"retriable payload should omit retry_after_ms: %s",
			data)
	}
}

func TestEvent_MarshalRoundTrip_PreservesAttemptNumber(t *testing.T) {
	original := Event{
		Type:          EventStepStarted,
		RunID:         "run-attempt",
		StepID:        "step-x",
		Timestamp:     time.Now().UTC().Truncate(time.Millisecond),
		AttemptNumber: 7,
	}
	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	var decoded Event
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal failed: %v", err)
	}
	if decoded.AttemptNumber != 7 {
		t.Fatalf("AttemptNumber = %d, want 7", decoded.AttemptNumber)
	}
	if decoded.Type != original.Type {
		t.Fatalf("Type = %q, want %q", decoded.Type, original.Type)
	}
}

func TestEvent_UnmarshalLegacyMissingAttempt(t *testing.T) {
	// Legacy event JSON written before this field existed must still
	// deserialize successfully with AttemptNumber defaulting to zero.
	legacy := []byte(`{"type":"step.completed","run_id":"r","step_id":"s","timestamp":"2026-01-01T00:00:00Z"}`)
	var decoded Event
	if err := json.Unmarshal(legacy, &decoded); err != nil {
		t.Fatalf("Unmarshal legacy failed: %v", err)
	}
	if decoded.AttemptNumber != 0 {
		t.Fatalf("AttemptNumber = %d, want 0 for legacy event", decoded.AttemptNumber)
	}
	if decoded.Type != EventStepCompleted {
		t.Fatalf("Type = %q, want %q", decoded.Type, EventStepCompleted)
	}
}

func TestEvent_OmitEmpty_AttemptNumberZero(t *testing.T) {
	// AttemptNumber=0 must not appear in marshalled JSON so existing
	// wire format is preserved for events that don't use it.
	evt := Event{
		Type:      EventWorkflowStarted,
		RunID:     "run-omit",
		Timestamp: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC),
	}
	data, err := json.Marshal(evt)
	if err != nil {
		t.Fatalf("Marshal failed: %v", err)
	}
	if bytes.Contains(data, []byte("attempt_number")) {
		t.Fatalf("marshalled JSON must omit attempt_number when zero, got: %s", data)
	}
}

func TestNATSMsgID_NoAttempt(t *testing.T) {
	// AttemptNumber == 0: existing behaviour is preserved exactly.
	evt := Event{Type: EventStepCompleted, RunID: "r1", StepID: "s1"}
	got := evt.NATSMsgID()
	want := "r1.s1.step.completed"
	if got != want {
		t.Fatalf("NATSMsgID() = %q, want %q", got, want)
	}
}

func TestNATSMsgID_WithAttempt(t *testing.T) {
	cases := []struct {
		name    string
		attempt int
		want    string
	}{
		{"attempt_one", 1, "r1.s1.step.started.1"},
		{"attempt_two", 2, "r1.s1.step.started.2"},
		{"attempt_forty_two", 42, "r1.s1.step.started.42"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			evt := Event{
				Type:          EventStepStarted,
				RunID:         "r1",
				StepID:        "s1",
				AttemptNumber: tc.attempt,
			}
			got := evt.NATSMsgID()
			if got != tc.want {
				t.Fatalf("NATSMsgID() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestNATSMsgID_WorkflowEventIgnoresAttempt(t *testing.T) {
	// Workflow events have empty StepID; the attempt suffix must NOT
	// be appended even if AttemptNumber happens to be set.
	evt := Event{
		Type:          EventWorkflowStarted,
		RunID:         "r1",
		AttemptNumber: 5, // deliberately set; should be ignored
	}
	got := evt.NATSMsgID()
	want := "r1.workflow.started"
	if got != want {
		t.Fatalf("NATSMsgID() = %q, want %q (workflow event must not append attempt)", got, want)
	}
}
