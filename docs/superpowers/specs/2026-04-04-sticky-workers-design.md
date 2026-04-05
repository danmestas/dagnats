# Sticky Workers

**Status:** Design
**Date:** 2026-04-04
**Depends on:** Worker directory (implemented)

## Problem

WorkerGroup provides static routing — all steps with `WorkerGroup: "gpu"` go to
workers subscribed to the `gpu` group. But there's no way to say "run all steps
of this workflow on the same worker" or "prefer the worker that ran the parent
step." This matters for:

- **Warm caches:** An LLM agent loop where each iteration benefits from the
  same worker's in-memory context/cache
- **Local state:** Steps that write to a local scratch directory and later steps
  read from it
- **GPU affinity:** Pin a multi-step ML pipeline to the same GPU worker

Hatchet offers `StickyStrategy.SOFT` (prefer same worker, fall back if busy) and
`StickyStrategy.HARD` (require same worker, queue if busy).

## Design

### 1. Concept

Sticky workers bind a workflow run to a specific worker after the first step
executes. Subsequent steps in the same run are routed to that worker. Two modes:

- **Soft:** Prefer the sticky worker. If unavailable within a timeout, fall back
  to any worker in the same group. Maximizes throughput.
- **Hard:** Require the sticky worker. If unavailable, the step queues until it
  returns. Guarantees locality but risks head-of-line blocking.

### 2. Type Changes

**`dag/types.go`** — add `StickyStrategy` and a field on `WorkflowDef`:

```go
type StickyStrategy string

const (
    StickyNone StickyStrategy = ""
    StickySoft StickyStrategy = "soft"
    StickyHard StickyStrategy = "hard"
)
```

```go
type WorkflowDef struct {
    // ... existing fields ...
    Sticky StickyStrategy `json:"sticky,omitempty"`
}
```

**`dag/builder.go`** — builder method:

```go
func (b *WorkflowBuilder) WithSticky(s StickyStrategy) *WorkflowBuilder {
    b.sticky = s
    return b
}
```

### 3. How It Works

**KV bucket: `sticky_bindings`** — maps `{runID}` to `{workerID}`.

**Worker subscriptions:**

Workers with a worker ID subscribe to both their normal subjects (`task.{type}.>`)
and a worker-specific subject (`task.{type}.{workerID}.>`). Only the bound worker
sees messages on the worker-specific subject.

**Binding flow:**

1. Workflow starts. No sticky binding yet.
2. First step dispatched to `task.{type}.{runID}` (normal routing).
3. Worker executes the step. On `step.completed`, the worker includes its
   `WorkerID` in the event payload.
4. Engine receives the completion event. If `WorkflowDef.Sticky != ""` and no
   binding exists, the engine writes `sticky_bindings.{runID} -> {workerID}`.
   The engine owns all routing decisions — workers don't need to know about
   sticky at all (information hiding).
5. For subsequent steps, the engine checks `sticky_bindings.{runID}`:
   - If binding exists: route via `publishStickyTask` (see below).
   - If no binding: publish normally.

**`publishStickyTask` (engine-internal helper):**

Encapsulates all sticky routing complexity in one function:

- **Hard:** Publish only to `task.{type}.{workerID}.{runID}`. If the worker
  is gone, AckWait expires and the step enters the retry/failure flow.
- **Soft:** Publish to `task.{type}.{workerID}.{runID}` first. Schedule a
  fallback timer via `SLEEP_TIMERS` (5-second delay). If the timer fires
  (sticky worker didn't claim), re-publish to `task.{type}.{runID}` (normal
  subject, any worker can pick up). All three actions are hidden inside
  `publishStickyTask` — the caller just says "publish this sticky task."

**Binding cleanup:**

Binding deleted when the workflow completes, fails, or is cancelled. TTL on the
KV entry (workflow timeout + buffer) handles crashes.

### 4. Worker Changes

**`worker/worker.go`:**

- Workers with a worker ID subscribe to `task.{type}.{workerID}.>` in addition
  to their normal subscriptions.
- `TaskContext.Complete()` and `Continue()` include `WorkerID` in the event
  payload. This is the only worker-side change — workers don't know about
  sticky bindings or routing decisions.

### 5. Engine Changes

**`engine/orchestrator.go`:**

- `handleStepCompleted`: if sticky workflow and no binding, create binding
  from `WorkerID` in the completion event payload.
- `publishTask`: if sticky and binding exists, delegate to `publishStickyTask`.
- `publishStickyTask` (new helper): encapsulates hard/soft routing logic.
  Single call site — callers don't coordinate multiple actions.

### 6. Validation

- `Sticky` must be one of `""`, `"soft"`, `"hard"`.
- Sticky workflows should have a `Timeout` set (hard sticky without timeout
  risks permanent blocking).
- Sticky is incompatible with per-step `WorkerGroup` overrides. When sticky is
  set, all steps route to the same worker — mixing worker groups would
  contradict that. If you need different worker groups, don't use sticky.

### 7. NATS Resources

| Resource | Type | Purpose |
|----------|------|---------|
| `sticky_bindings` | KV (TTL: workflow timeout + 1h) | Run-to-worker binding |

No new streams. Uses existing `task.>` subjects with worker-ID segments.

### 8. Bounds

- Binding TTL: workflow timeout + 1 hour (auto-cleanup).
- Soft fallback delay: 5 seconds (configurable on WorkflowDef).
- Maximum sticky bindings: bounded by active workflow runs.

### 9. Builder API

```go
wb := dag.NewWorkflow("ml-pipeline").WithSticky(dag.StickySoft)
wb.Task("preprocess", "preprocess")
wb.Task("train", "train").DependsOn("preprocess")
wb.Task("evaluate", "evaluate").DependsOn("train")
def, _ := wb.Build()
```

### 10. Edge Cases

- **Worker dies during sticky run (soft):** Fallback timer fires, step re-
  published to normal subject. Another worker picks it up and becomes the new
  sticky worker for remaining steps.
- **Worker dies during sticky run (hard):** Step fails via AckWait, enters
  retry flow. On retry, binding is stale (worker gone from directory). Engine
  clears binding, retried step goes to any worker.
- **Concurrent steps:** Both go to the same sticky worker. Worker processes
  them sequentially (bounded by MaxAckPending) or in parallel if configured.
