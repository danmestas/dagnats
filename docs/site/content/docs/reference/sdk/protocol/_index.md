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
| `FailureType` | Categorizes failures: `permanent`, `transient`, `rate_limited` |

## Event Types

| Constant | Value | Scope |
|----------|-------|-------|
| `EventWorkflowStarted` | `workflow.started` | Workflow |
| `EventWorkflowCompleted` | `workflow.completed` | Workflow |
| `EventWorkflowFailed` | `workflow.failed` | Workflow |
| `EventWorkflowCancelled` | `workflow.cancelled` | Workflow |
| `EventStepStarted` | `step.started` | Step |
| `EventStepCompleted` | `step.completed` | Step |
| `EventStepFailed` | `step.failed` | Step |
| `EventStepContinue` | `step.continue` | Step (agent loop) |
| `EventApprovalGranted` | `approval.granted` | Step (approval) |
| `EventApprovalRejected` | `approval.rejected` | Step (approval) |

## Event Structure

```go
type Event struct {
    Type        EventType       `json:"type"`
    RunID       string          `json:"run_id"`
    StepID      string          `json:"step_id,omitempty"`
    Timestamp   time.Time       `json:"timestamp"`
    Payload     json.RawMessage `json:"payload,omitempty"`
    TraceParent string          `json:"trace_parent,omitempty"`
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
    TaskID    string          `json:"task_id"`
    RunID     string          `json:"run_id"`
    StepID    string          `json:"step_id"`
    Iteration int             `json:"iteration,omitempty"`
    Attempt   int             `json:"attempt,omitempty"`
    Input     json.RawMessage `json:"input,omitempty"`
}
```

## TaskResolution Structure

Used by HTTP workers to resolve tasks via the bridge:

```go
type TaskResolution struct {
    Action     string          `json:"action"`
    Output     json.RawMessage `json:"output,omitempty"`
    Error      string          `json:"error,omitempty"`
    DurationMs int             `json:"duration_ms,omitempty"`
    Checkpoint json.RawMessage `json:"checkpoint,omitempty"`
    Data       json.RawMessage `json:"data,omitempty"`
}
```

Actions: `complete`, `fail`, `pause`, `checkpoint`.
