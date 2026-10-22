package protocol

import (
	"encoding/json"
	"strconv"
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
	// Metadata is static per-step config copied from StepDef.Metadata,
	// delivered to workers via TaskContext.Metadata(). Nil when the step
	// declared no metadata.
	Metadata map[string]string `json:"metadata,omitempty"`
	// RequiredCapabilities is copied verbatim from StepDef.RequiredCapabilities
	// at enqueue time. The worker reads it to gate capability handles (e.g.
	// the control plane). Nil = no capabilities declared (today's behavior).
	RequiredCapabilities []string `json:"required_capabilities,omitempty"`
	// DispatchNonce is the per-dispatch run-binding token (#380, ADR-021
	// Phase A). The engine stamps the same value on StepState.DispatchNonce
	// and here. The worker carries it on control-plane requests so the
	// server can verify the caller received this exact dispatch. Additive,
	// omitempty: legacy payloads deserialize to "" and the server rejects an
	// empty/mismatched nonce on the control-plane path.
	DispatchNonce string `json:"dispatch_nonce,omitempty"`
	// WorkflowName is the workflow definition's name (#503), stamped by the
	// engine at every publish site so SigNoz can filter/group task spans by
	// workflow instead of every run collapsing into one undifferentiated
	// span name. Additive, omitempty: legacy payloads deserialize to "" and
	// the worker simply omits the workflow_name span attribute in that case.
	WorkflowName string `json:"workflow_name,omitempty"`
	// TraceParent/TraceState carry the W3C trace context the HTTP bridge
	// stamps on each polled task (#534, #537) so a worker's execution
	// span parents onto bridge.dispatch instead of orphaning.
	//
	// The tags are the W3C wire names, deliberately UN-underscored —
	// unlike protocol.Event's persisted trace_parent/trace_state a few
	// types below. Renaming either to match the other silently drops the
	// field on decode: the poll path uses plain json.Decode with no
	// DisallowUnknownFields, so a mismatch is invisible at runtime.
	// Additive and omitempty: pre-#537 responses decode to "".
	//
	// Use observe.TraceContextFromTask to turn these into a parent
	// context rather than parsing them by hand.
	TraceParent string `json:"traceparent,omitempty"`
	TraceState  string `json:"tracestate,omitempty"`
	// State is the agent-loop state the step last committed through a
	// step.continue event (#801). The engine stores it in the run
	// snapshot alongside the iteration it belongs to and hands it to
	// every later dispatch of the step: the next iteration, a retry of
	// the current one, and a dispatch sent again after an engine crash.
	// Opaque bytes; TaskContext.LoadCheckpoint returns them. Additive:
	// legacy payloads deserialize to nil.
	State []byte `json:"state,omitempty"`
	// StateMaxBytes is the engine's cap on State, stamped on every
	// dispatch so a worker can refuse an oversized Continue before
	// publishing it, with the same number the engine would enforce.
	// Zero (a legacy payload) means LoopStateMaxBytesDefault.
	StateMaxBytes int `json:"state_max_bytes,omitempty"`
}

// LoopStateMaxBytesDefault is the default cap on the state a
// step.continue event may carry (#801). The whole run snapshot is one
// KV value bounded by the server's max_payload, so the state a loop
// commits per iteration must stay well inside it.
const LoopStateMaxBytesDefault = 256 << 10

// StepContinuePayload is the structured payload for EventStepContinue
// (#801). Output is what the handler passed to Continue; State is the
// state it checkpointed during the iteration, committed by the engine
// in the same snapshot save that advances the iteration, so a crash
// between checkpoint and continue cannot repeat a turn.
type StepContinuePayload struct {
	Output json.RawMessage `json:"output,omitempty"`
	State  []byte          `json:"state,omitempty"`
	// StateSet marks State as the state to commit even when it is
	// empty: a handler that checkpointed nil clears the saved state,
	// where an absent State (an event without it) leaves it alone.
	StateSet bool `json:"state_set,omitempty"`
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
	Type          EventType       `json:"type"`
	RunID         string          `json:"run_id"`
	StepID        string          `json:"step_id,omitempty"`
	Timestamp     time.Time       `json:"timestamp"`
	Payload       json.RawMessage `json:"payload,omitempty"`
	TraceParent   string          `json:"trace_parent,omitempty"`
	TraceState    string          `json:"trace_state,omitempty"`
	WorkerID      string          `json:"worker_id,omitempty"`
	AttemptNumber int             `json:"attempt_number,omitempty"`
	// Iteration and DispatchNonce, with AttemptNumber, identify the
	// dispatch a step.continue, step.completed or step.failed answers
	// (#801). The engine accepts such an event only while the step is
	// Running on that iteration and attempt, with that nonce. Iteration
	// is a pointer because iteration 0 must be distinguishable from an
	// event an older worker sent without the field: an event without
	// Iteration carries no identity (see HasIdentity), and the engine
	// accepts it only while it is configured to accept legacy events.
	Iteration     *int   `json:"iteration,omitempty"`
	DispatchNonce string `json:"dispatch_nonce,omitempty"`
	// JournalSeq is the AGENT_JOURNAL sequence of the commit record a
	// journal step wrote before this step.continue, step.completed or
	// step.failed (ADR-026). Zero for a step without a journal.
	JournalSeq uint64 `json:"journal_seq,omitempty"`
}

// HasIdentity reports whether the event names the dispatch it answers
// (#801): Iteration is present and AttemptNumber is set. An event from
// a worker predating the identity fields has neither.
func (e Event) HasIdentity() bool {
	return e.Iteration != nil && e.AttemptNumber > 0
}

// SetIdentity stamps the dispatch identity on the event (#801). nonce
// may be empty when the dispatch carried none (a legacy task payload);
// attempt must be the 1-based attempt the event belongs to.
func (e *Event) SetIdentity(iteration, attempt int, nonce string) {
	if e == nil {
		panic("Event.SetIdentity: receiver must not be nil")
	}
	if iteration < 0 || attempt < 1 {
		panic("Event.SetIdentity: iteration must be >= 0 and attempt >= 1")
	}
	e.Iteration = &iteration
	e.AttemptNumber = attempt
	e.DispatchNonce = nonce
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
// An event that carries its dispatch identity (#801) gets the stable
// {run}.{step}.{type}.{attempt}.{iteration}: a redelivery inside the
// dedup window is dropped by JetStream, one outside it by the engine's
// identity check, and no two iterations or attempts share an ID.
func (e Event) NATSMsgID() string {
	if e.StepID == "" {
		return e.RunID + "." + string(e.Type)
	}
	base := e.RunID + "." + e.StepID + "." + string(e.Type)
	if e.AttemptNumber > 0 {
		base += "." + strconv.Itoa(e.AttemptNumber)
	}
	if e.HasIdentity() {
		base += "." + strconv.Itoa(*e.Iteration)
	}
	return base
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

// WorkerStatusSnapshot is the per-worker counter snapshot stored in
// the worker_status KV bucket. Workers update their entry as
// counters change; the CLI reads and aggregates entries to surface
// drain progress (issue #182). Last-write-wins on the WorkerID key.
type WorkerStatusSnapshot struct {
	WorkerID              string `json:"worker_id"`
	CancelledTasksSkipped uint64 `json:"cancelled_tasks_skipped"`
}

// CancelSubjectPrefix is the core NATS subject family the engine
// publishes on when a run is cancelled (#802). Core, not JetStream: it
// is a best-effort nudge to workers holding a handler for the run, and
// the durable truth stays the run state (workers re-check it on
// Heartbeat).
const CancelSubjectPrefix = "dagnats.cancel."

// CancelSubject returns the cancel signal subject for runID.
func CancelSubject(runID string) string {
	if runID == "" {
		panic("CancelSubject: runID must not be empty")
	}
	return CancelSubjectPrefix + runID
}
