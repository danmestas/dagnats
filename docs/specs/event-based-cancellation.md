# Event-Based Cancellation

**Status:** Design
**Date:** 2026-04-04
**Depends on:** Nothing (uses existing correlator pattern)

## Problem

Running workflows can only be cancelled via explicit API call (`POST /runs/{id}/cancel`)
or workflow timeout. There is no way to say "cancel this workflow if event X arrives" --
for example, cancel a reminder workflow when the user completes the task, or cancel a
deploy pipeline when a rollback event fires.

## Design

### 1. Concept

A workflow definition declares one or more **cancellation events**. While the workflow
is running, the engine watches for matching events. When a match arrives, the workflow
is cancelled as if `POST /runs/{id}/cancel` was called. The cancelling event's data is
available for cleanup logic.

This uses the same event correlation infrastructure as `WaitForEvent` but with a
different action: cancel instead of complete.

### 2. Type Changes

**`dag/types.go`** -- add `CancelOn` to `WorkflowDef`:

```go
type CancelOn struct {
    Event   string        `json:"event"`
    Match   Match         `json:"match"`
    Timeout time.Duration `json:"timeout,omitempty"`
}

type WorkflowDef struct {
    // ... existing fields ...
    CancelOn []CancelOn `json:"cancel_on,omitempty"`
}
```

### 3. How It Works

Reuse the `Correlator` with a new `WaiterAction` discriminator:

```go
type WaiterAction int

const (
    WaiterActionComplete WaiterAction = iota
    WaiterActionCancel
)
```

**Flow:**

1. On `workflow.started`, register one cancellation waiter per `CancelOn` entry.
2. Correlator resolves `Match` using run input (same as wait-for-event).
3. On match: publish `workflow.cancelled` with triggering event as payload.
   Cancellation cause lives in the event log only -- no `CancelledBy` field
   on `WorkflowRun` (information hiding: one cancellation path, not three).
4. All cancel waiters removed on any terminal state.

### 4. Builder API

```go
wb.CancelOn("task.completed",
    dag.MatchField("data.task_id", "input.task_id"))

wb.CancelOnWithTimeout("task.completed",
    dag.MatchField("data.task_id", "input.task_id"),
    24*time.Hour)
```

### 5. Cancel Timeout

If `CancelOn.Timeout > 0`, schedule a timer (`cancel_watch_expire`) that removes
the cancel waiter after timeout. The workflow continues but stops watching for
that particular cancellation event.

### 6. Validation

- `CancelOn[].Event` must not be empty.
- `CancelOn[].Match` must have valid dot-path expressions.
- Max 5 `CancelOn` entries per workflow.
- `CancelOn[].Timeout` >= 0 (0 = watch for lifetime of run).

### 7. Cleanup on Terminal State

`completeWorkflow`, `failWorkflow`, and `handleWorkflowCancelled` all call
`correlator.RemoveWaitersForRun(run.RunID)`. Cancel waiters share the same
waiter index as wait-for-event waiters.

### 8. Bounds

- Max `CancelOn` entries per workflow: 5.
- Max cancel timeout: 365 days.
- Cancel waiters share the 10,000-per-event-type bound with wait-for-event.

### 9. Observability

- Event: `EventWorkflowCancelledByEvent` for distinguishing event-triggered
  cancellation from manual/timeout.
- Metric: `workflow.cancel.event_triggered` counter.
- Log: info-level on match.

### 10. Edge Cases

- **Multiple CancelOn match simultaneously:** First match wins (per-run lock).
- **Match after workflow completed:** Waiter already removed. No-op.
- **Event arrives during step execution:** Cancellation between steps (same as manual).
- **Engine restart:** Cancel waiters rebuilt from KV on correlator startup.
