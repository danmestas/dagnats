---
title: protocol
weight: 3
---

```
import "github.com/danmestas/dagnats/protocol"
```

Wire types shared between the engine, workers, and API. This package defines the event schema, task payload format, and resolution types that flow through NATS subjects.

## Key Types

| Type | Description |
|------|-------------|
| `Event` | Envelope for all workflow and step lifecycle events |
| `EventType` | String enum for event types (`workflow.started`, `step.completed`, etc.) |
| `TaskPayload` | Message body published to task subjects when the engine dispatches a step |
| `TaskResolution` | Resolution sent by HTTP workers via the bridge resolve endpoint |
| `FailureType` | How the engine treats a failure: `retriable`, `non_retriable` or `retry_after` |
| `StepFailedPayload` | Payload of `step.failed`: `error`, `failure_type`, `retry_after_ms` |

## Event Types

The event types are string constants such as `workflow.started`, `step.started`, `step.completed`, `step.failed`, `step.continue`, `workflow.completed`, `workflow.failed`, `workflow.cancelled`, `approval.granted` and `approval.rejected`. The full set also covers map, sleep, wait, child-workflow, compensation, approval and planner events. See the generated API page for every constant.

## Event Structure

```go
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
}
```

## Constructor Functions

| Function | Description |
|----------|-------------|
| `NewWorkflowEvent(typ, runID, payload)` | Creates a workflow-scoped event |
| `NewStepEvent(typ, runID, stepID, payload)` | Creates a step-scoped event |

Both constructors set the timestamp and provide methods for NATS subject routing (`NATSSubject()`) and deduplication IDs (`NATSMsgID()`).

## TaskPayload Structure

```go
type TaskPayload struct {
    TaskID               string            `json:"task_id"`
    RunID                string            `json:"run_id"`
    StepID               string            `json:"step_id"`
    Iteration            int               `json:"iteration,omitempty"`
    Attempt              int               `json:"attempt,omitempty"`
    Input                json.RawMessage   `json:"input,omitempty"`
    Metadata             map[string]string `json:"metadata,omitempty"`
    RequiredCapabilities []string          `json:"required_capabilities,omitempty"`
    DispatchNonce        string            `json:"dispatch_nonce,omitempty"`
    WorkflowName         string            `json:"workflow_name,omitempty"`
    TraceParent          string            `json:"traceparent,omitempty"`
    TraceState           string            `json:"tracestate,omitempty"`
}
```

## TaskResolution Structure

Used by HTTP workers to resolve tasks via the bridge:

```go
type TaskResolution struct {
    Action     string          `json:"action"`
    Output     json.RawMessage `json:"output,omitempty"`
    Error      string          `json:"error,omitempty"`
    Name       string          `json:"name,omitempty"`
    DurationMs int64           `json:"duration_ms,omitempty"`
    Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
    Data       json.RawMessage `json:"data,omitempty"`
}
```

The struct covers the common actions. The bridge accepts more fields, such as `failure_type`, `retry_after_ms`, `run_id` and `timeout_ms`, and the actions `complete`, `fail`, `continue`, `pause`, `checkpoint`, `heartbeat`, `stream`, `send_signal` and `wait_signal`. See [HTTP Bridge](/docs/workers/http-bridge).
