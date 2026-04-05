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
	TaskID    string          `json:"task_id"` // {runID}.{stepID}
	RunID     string          `json:"run_id"`
	StepID    string          `json:"step_id"`
	Iteration int             `json:"iteration,omitempty"`
	Attempt   int             `json:"attempt,omitempty"`
	Input     json.RawMessage `json:"input,omitempty"`
}

// TaskResolution is the wire format for HTTP bridge resolve actions.
// The action field discriminates between complete/fail/pause/checkpoint.
// Workers POST this to /v1/tasks/{id}/resolve to report task outcomes.
type TaskResolution struct {
	Action     string          `json:"action"`
	Output     json.RawMessage `json:"output,omitempty"`
	Error      string          `json:"error,omitempty"`
	Name       string          `json:"name,omitempty"`
	DurationMs int64           `json:"duration_ms,omitempty"`
	Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
}

// FailureType distinguishes how the engine handles a step failure.
type FailureType string

const (
	FailureTypeRetriable    FailureType = "retriable"
	FailureTypeNonRetriable FailureType = "non_retriable"
	FailureTypeRetryAfter   FailureType = "retry_after"
)

// StepFailedPayload is the structured payload for EventStepFailed.
// FailureType defaults to retriable when empty (backward compat).
// RetryAfterMs is only used with FailureTypeRetryAfter.
type StepFailedPayload struct {
	Error        string      `json:"error"`
	FailureType  FailureType `json:"failure_type,omitempty"`
	RetryAfterMs int64       `json:"retry_after_ms,omitempty"`
}

// EventType identifies the kind of workflow lifecycle event.
// Using string constants makes events self-describing over the wire.
type EventType string

const (
	EventWorkflowStarted          EventType = "workflow.started"
	EventStepQueued               EventType = "step.queued"
	EventStepStarted              EventType = "step.started"
	EventStepCompleted            EventType = "step.completed"
	EventStepFailed               EventType = "step.failed"
	EventStepContinue             EventType = "step.continue"
	EventAgentLoopIteration       EventType = "agent.loop.iteration"
	EventStepMapStarted           EventType = "step.map.started"
	EventStepMapCompleted         EventType = "step.map.completed"
	EventStepMapInstanceCompleted EventType = "step.map.instance.completed"
	EventStepSleepStarted         EventType = "step.sleep.started"
	EventStepSleepCompleted       EventType = "step.sleep.completed"
	EventStepWaitStarted          EventType = "step.wait.started"
	EventStepWaitMatched          EventType = "step.wait.matched"
	EventStepWaitTimeout          EventType = "step.wait.timeout"
	EventWorkflowCompleted        EventType = "workflow.completed"
	EventWorkflowFailed           EventType = "workflow.failed"
	EventWorkflowSpawn            EventType = "workflow.spawn"
	EventWorkflowChildCompleted   EventType = "workflow.child.completed"
	EventWorkflowChildFailed      EventType = "workflow.child.failed"
	EventWorkflowCancelled        EventType = "workflow.cancelled"
	EventStepCancelled            EventType = "step.cancelled"
	EventCompensateStarted        EventType = "compensate.started"
	EventCompensateStepCompleted  EventType = "compensate.step.completed"
	EventCompensateFailed         EventType = "compensate.failed"
	EventCompensateCompleted      EventType = "compensate.completed"
	EventApprovalRequested        EventType = "approval.requested"
	EventApprovalGranted          EventType = "approval.granted"
	EventApprovalRejected         EventType = "approval.rejected"
	EventApprovalExpired          EventType = "approval.expired"
	EventPlannerMaterialized      EventType = "planner.materialized"
)

// Event is the core communication primitive published to the history stream.
// Payload carries event-specific data as raw JSON to keep the type schema-agnostic.
type Event struct {
	Type        EventType       `json:"type"`
	RunID       string          `json:"run_id"`
	StepID      string          `json:"step_id,omitempty"`
	Timestamp   time.Time       `json:"timestamp"`
	Payload     json.RawMessage `json:"payload,omitempty"`
	TraceParent string          `json:"trace_parent,omitempty"`
	TraceState  string          `json:"trace_state,omitempty"`
	WorkerID    string          `json:"worker_id,omitempty"`
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
	if eventType == "" {
		panic("NewWorkflowEvent: eventType must not be empty")
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
	if e.Type == "" {
		panic("Event.NATSSubject: Type must not be empty")
	}
	return "history." + e.RunID
}

// NATSMsgID returns the deduplication ID for JetStream idempotent publishing.
// Composed of run, step, and event type so replays are safe.
// For workflow events (empty StepID), omits the step segment to avoid double dots.
func (e Event) NATSMsgID() string {
	if e.StepID == "" {
		return e.RunID + "." + string(e.Type)
	}
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
