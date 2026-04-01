# Concurrency Limits, Workflow Cancel, and Retry Policies Design

## Context

DagNats lacks three features that all major workflow engines (Kestra, Hatchet, Temporal)
provide: concurrency limits to prevent resource exhaustion, workflow cancellation for
operational control, and configurable retry policies beyond NATS NakWithDelay. These
three features share touch points in `dag/types.go` and `engine/orchestrator.go`, so
they are designed and implemented together.

## Design Decisions

1. **Concurrency scope: Per-workflow + per-step** — Per-workflow limits cap concurrent
   runs of the same workflow. Per-step limits cap concurrent executions of a task name
   across all runs (critical for LLM API rate limiting).
2. **Cancel style: Graceful** — Workers are notified via context cancellation derived
   from KV watch. Agent loops finish the current iteration, then stop.
3. **Retry policy location: Workflow default + per-step override** — Workflow-level
   `DefaultRetry` reduces boilerplate. Per-step `Retry` overrides for exceptions.
4. **Backwards compatibility: Preserved** — Existing `StepDef.Retries int` field is
   kept. If `Retry` is nil and `Retries > 0`, a fixed-delay policy is synthesized.

---

## Data Model Changes

### New Types in `dag/types.go`

```go
// RetryStrategy selects the backoff algorithm for retries.
type RetryStrategy int

const (
    RetryFixed       RetryStrategy = iota // Same delay every time
    RetryLinear                           // delay * attempt
    RetryExponential                      // delay * multiplier^(attempt-1)
)

// RetryPolicy configures retry behavior. Used on WorkflowDef (default)
// and StepDef (override). MaxAttempts=0 means no retries.
type RetryPolicy struct {
    MaxAttempts  int           `json:"max_attempts"`
    Strategy     RetryStrategy `json:"strategy"`
    InitialDelay time.Duration `json:"initial_delay"`
    MaxDelay     time.Duration `json:"max_delay"`
    Multiplier   float64       `json:"multiplier,omitempty"`
}

// ConcurrencyLimit controls parallel execution at workflow and step level.
type ConcurrencyLimit struct {
    MaxRuns  int `json:"max_runs,omitempty"`  // concurrent runs of this workflow
    MaxSteps int `json:"max_steps,omitempty"` // concurrent executions per task name
}
```

### Modified Types

**WorkflowDef** gets two new fields:
```go
type WorkflowDef struct {
    Name         string             `json:"name"`
    Version      string             `json:"version"`
    Steps        []StepDef          `json:"steps"`
    DefaultRetry *RetryPolicy       `json:"default_retry,omitempty"`
    Concurrency  *ConcurrencyLimit  `json:"concurrency,omitempty"`
}
```

**StepDef** gets one new field:
```go
type StepDef struct {
    // ...existing fields unchanged...
    Retry *RetryPolicy `json:"retry,omitempty"` // overrides DefaultRetry
}
```

**StepStatus** adds `StepStatusCancelled`:
```go
const (
    // ...existing...
    StepStatusCancelled // new — step was cancelled
)
```

### New Event Types in `protocol/protocol.go`

```go
EventWorkflowCancelled EventType = "workflow.cancelled"
EventStepCancelled     EventType = "step.cancelled"
```

### New NATS Resources

| Resource | Name | Purpose |
|----------|------|---------|
| KV Bucket | `concurrency_runs` | Per-workflow run counters |
| KV Bucket | `concurrency_steps` | Per-task-name step counters |
| KV Bucket | `concurrency_queue` | Pending runs waiting for slots |

---

## Concurrency Limits

### Per-Workflow Run Limits

**Acquire on workflow start:**
1. Read counter from `concurrency_runs` KV (key: `workflow.{workflowID}`)
2. If count >= `Concurrency.MaxRuns` → queue the run
   - Store in `concurrency_queue` KV: key `workflow.{workflowID}.{runID}`, value = timestamp
   - Run stays in `Pending` status, steps not enqueued
3. If count < limit → increment counter via KV Put with revision check (optimistic lock)
4. On CAS failure (concurrent modification) → retry from step 1

**Release on workflow complete/fail/cancel:**
1. Decrement counter in `concurrency_runs` KV
2. Check `concurrency_queue` for oldest pending run of this workflow
3. If found → remove from queue, start it (publish `workflow.started`)

**No polling.** The release-and-start-next is synchronous within the orchestrator's
event handler. The orchestrator that completes a run is the one that starts the next.

### Per-Step Concurrency Limits

**Acquire before step dispatch:**
1. Read counter from `concurrency_steps` KV (key: `step.{taskName}`)
2. If count >= `Concurrency.MaxSteps` → don't publish task message
   - Step stays in `Queued` status
   - Orchestrator registers a KV watcher for the counter key
3. If count < limit → increment, publish task message

**Release on step complete/fail/cancel:**
1. Decrement counter in `concurrency_steps` KV
2. KV watcher fires on the orchestrator → re-check queued steps

**Bounded watcher:** Max 100 KV watchers active (prevent resource exhaustion if many
steps are waiting). Beyond 100, fall back to periodic polling every 5 seconds.

---

## Workflow Cancel

### Cancel Flow

1. User calls `POST /runs/{id}/cancel` or CLI `dagnats run cancel <id>`
2. API publishes `workflow.cancelled` event to `history.{runID}`
3. Orchestrator `handleWorkflowCancelled`:
   a. Load run snapshot
   b. Set `run.Status = RunStatusCancelled`
   c. For each step with status Running or Queued:
      - Set `step.Status = StepStatusCancelled`
      - Publish `step.cancelled` event
   d. Save snapshot
   e. Decrement concurrency counters (runs + steps)
   f. Start next pending run if any (concurrency release)
   g. Notify parent if child workflow

### Worker Cancellation

Workers learn about cancellation via their `TaskContext`:

```go
type TaskContext interface {
    // ...existing methods...
    Done() <-chan struct{} // closed when run is cancelled
}
```

**Implementation in `worker/context.go`:**
- Worker creates a KV watcher on `workflow_runs.run.{runID}` at task start
- When the run snapshot shows `Status: cancelled`, close the Done channel
- Worker's handler goroutine checks `Done()` between operations
- On cancellation, worker calls `ctx.Cancel()` (new method) which publishes
  `step.cancelled` event and acks the task message

**Agent loops:** The agent loop checks `Done()` before calling `Continue()`. If
cancelled, it stops iterating. The current LLM call finishes (no mid-call interrupt)
but no further iterations are queued.

**Workers that ignore Done():** If a worker doesn't check `Done()`, the step
eventually times out via NATS AckWait. Cancel is best-effort from the worker's
perspective — the orchestrator marks it cancelled regardless.

---

## Configurable Retry Policies

### Policy Resolution

For any step, the effective retry policy is resolved in order:
1. `StepDef.Retry` (if non-nil) — per-step override
2. `WorkflowDef.DefaultRetry` (if non-nil) — workflow default
3. Synthesized from `StepDef.Retries` (if > 0) — backwards compatibility
4. No retries (default)

```go
// ResolveRetryPolicy returns the effective retry policy for a step.
func ResolveRetryPolicy(
    wfDef WorkflowDef, stepDef StepDef,
) *RetryPolicy {
    if stepDef.Retry != nil {
        return stepDef.Retry
    }
    if wfDef.DefaultRetry != nil {
        return wfDef.DefaultRetry
    }
    if stepDef.Retries > 0 {
        return &RetryPolicy{
            MaxAttempts:  stepDef.Retries,
            Strategy:     RetryFixed,
            InitialDelay: 5 * time.Second,
            MaxDelay:     5 * time.Second,
        }
    }
    return nil
}
```

### Delay Calculation

```go
func CalculateDelay(
    policy RetryPolicy, attempt int,
) time.Duration {
    var delay time.Duration
    switch policy.Strategy {
    case RetryFixed:
        delay = policy.InitialDelay
    case RetryLinear:
        delay = policy.InitialDelay * time.Duration(attempt)
    case RetryExponential:
        d := float64(policy.InitialDelay) *
            math.Pow(policy.Multiplier, float64(attempt-1))
        delay = time.Duration(d)
    }
    if policy.MaxDelay > 0 && delay > policy.MaxDelay {
        delay = policy.MaxDelay
    }
    return delay
}
```

### Orchestrator Integration

In `handleStepFailed`:
1. Resolve effective `RetryPolicy` for the step
2. If policy is nil or `attempt >= policy.MaxAttempts` → permanent failure (existing)
3. Otherwise → calculate delay, nak with delay (NATS redelivers after delay)

Workers remain oblivious to retry policy. The orchestrator owns all retry logic —
matching the existing "deep module" design where workers only call Complete/Fail.

### Backwards Compatibility

The existing `StepDef.Retries int` field is preserved. Code paths:
- If `Retry != nil`: use `Retry` (new behavior)
- If `Retry == nil && Retries > 0`: synthesize fixed-delay policy (compat)
- If both nil/zero: no retries (existing default)

Old workflow definitions with `Retries: 3` continue to work identically.

---

## What Changes in Existing Code

### `dag/types.go`
- Add `RetryStrategy` enum with String/JSON methods
- Add `RetryPolicy` struct
- Add `ConcurrencyLimit` struct
- Add `DefaultRetry` and `Concurrency` to `WorkflowDef`
- Add `Retry` to `StepDef`
- Add `StepStatusCancelled` to `StepStatus` enum

### `dag/retry.go` (new)
- `ResolveRetryPolicy(wfDef, stepDef) *RetryPolicy`
- `CalculateDelay(policy, attempt) time.Duration`

### `protocol/protocol.go`
- Add `EventWorkflowCancelled` and `EventStepCancelled`

### `engine/orchestrator.go`
- Add `handleWorkflowCancelled` handler
- Modify `handleStepFailed` to use resolved retry policy + calculated delay
- Add concurrency acquire/release in `handleWorkflowStarted` / `completeWorkflow`
- Add concurrency check in `publishReadyTasks`
- Update `isHandledEventType` for new events

### `engine/concurrency.go` (new)
- `ConcurrencyManager` — KV-based acquire/release/queue

### `worker/context.go`
- Add `Done() <-chan struct{}` to `TaskContext` interface
- Add `Cancel()` method
- KV watcher for run cancellation

### `natsutil/conn.go`
- Add `concurrency_runs`, `concurrency_steps`, `concurrency_queue` KV buckets

### `api/service.go`
- Add `CancelRun(runID)` method

### `cli/run.go`
- Add `dagnats run cancel <run_id>` command

---

## Testing Strategy

### Unit Tests (`dag/`)
- RetryStrategy String/JSON round-trip
- RetryPolicy JSON round-trip
- ConcurrencyLimit JSON round-trip
- ResolveRetryPolicy resolution order (step → workflow → compat → nil)
- CalculateDelay for all three strategies
- StepStatusCancelled String/JSON

### Integration Tests (`engine/`, real embedded NATS)
- Concurrency: start 3 runs with limit 2, verify third queues, complete one, third starts
- Cancel: start run, cancel, verify all steps cancelled and counters decremented
- Retry: fail a step, verify retry with correct delay, verify exhaustion → permanent fail
- Cancel during agent loop: verify current iteration completes, no further Continue
- Backwards compat: workflow with `Retries: 3` still retries 3 times

### Worker Tests (`worker/`)
- Done() channel closes when run is cancelled
- Worker that checks Done() stops cleanly

No shared NATS servers between tests.
