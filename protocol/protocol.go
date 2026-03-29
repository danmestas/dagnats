package protocol

import (
	"encoding/json"
	"time"
)

// TaskPayload is the message body published to a task subject when the engine
// dispatches a step for execution. Workers unmarshal this to build a TaskContext.
// Iteration is the agent-loop iteration index (0 for the first execution); workers
// include it in Continue event MsgIds to prevent JetStream deduplication across cycles.
type TaskPayload struct {
	RunID     string          `json:"run_id"`
	StepID    string          `json:"step_id"`
	Iteration int             `json:"iteration,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
}

// EventType identifies the kind of workflow lifecycle event.
// Using string constants makes events self-describing over the wire.
type EventType string

const (
	EventWorkflowStarted    EventType = "workflow.started"
	EventStepQueued         EventType = "step.queued"
	EventStepStarted        EventType = "step.started"
	EventStepCompleted      EventType = "step.completed"
	EventStepFailed         EventType = "step.failed"
	EventStepContinue       EventType = "step.continue"
	EventAgentLoopIteration EventType = "agent.loop.iteration"
	EventWorkflowCompleted  EventType = "workflow.completed"
	EventWorkflowFailed     EventType = "workflow.failed"
)

// Event is the core communication primitive published to the history stream.
// Payload carries event-specific data as raw JSON to keep the type schema-agnostic.
type Event struct {
	Type      EventType       `json:"type"`
	RunID     string          `json:"run_id"`
	StepID    string          `json:"step_id,omitempty"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// NewStepEvent constructs an Event for a step lifecycle transition.
// Panics on empty runID or stepID — these are programmer errors, not runtime errors.
func NewStepEvent(eventType EventType, runID string, stepID string, payload []byte) Event {
	if runID == "" {
		panic("NewStepEvent: runID must not be empty")
	}
	if stepID == "" {
		panic("NewStepEvent: stepID must not be empty")
	}
	return Event{
		Type:      eventType,
		RunID:     runID,
		StepID:    stepID,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}

// NewWorkflowEvent constructs an Event for a workflow lifecycle transition.
// Panics on empty runID — programmer error.
func NewWorkflowEvent(eventType EventType, runID string, payload []byte) Event {
	if runID == "" {
		panic("NewWorkflowEvent: runID must not be empty")
	}
	return Event{
		Type:      eventType,
		RunID:     runID,
		Timestamp: time.Now().UTC(),
		Payload:   payload,
	}
}

// NATSSubject returns the JetStream subject for publishing this event.
// All events for a run share one subject so consumers get ordered history.
// Panics on empty RunID — subjects with empty segments are invalid in NATS.
func (e Event) NATSSubject() string {
	if e.RunID == "" {
		panic("Event.NATSSubject: RunID must not be empty")
	}
	return "history." + e.RunID
}

// NATSMsgID returns the deduplication ID for JetStream idempotent publishing.
// Composed of run, step, and event type so replays are safe.
func (e Event) NATSMsgID() string {
	return e.RunID + "." + e.StepID + "." + string(e.Type)
}

// Marshal serializes the event to JSON for publishing to NATS.
func (e Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}

// UnmarshalEvent deserializes a NATS message body into an Event.
func UnmarshalEvent(data []byte) (Event, error) {
	var evt Event
	err := json.Unmarshal(data, &evt)
	return evt, err
}
