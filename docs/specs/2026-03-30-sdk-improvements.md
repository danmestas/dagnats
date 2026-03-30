# SDK Improvements Spec

Informed by deep analysis of Hatchet's architecture and SDK, filtered through
Ousterhout (minimize complexity, deep modules) and Hipp (small, fast, reliable)
design philosophies. Zero new packages, zero new abstractions — every change
deepens an existing module.

## 1. `StepRef` in `dag/`

**Problem:** `DependsOn("plan")` is a string that silently breaks at validation
time if misspelled. The error exists at runtime when it could be impossible at
compile time.

**Design:** `Task()` and `AgentLoop()` return a `StepRef` value that holds a
back-pointer to the builder. Dependency wiring uses `After(ref)` instead of
`DependsOn(string)`. The old `DependsOn` stays for backward compat.

```go
// dag/stepref.go
type StepRef struct {
    id      string
    index   int
    builder *WorkflowBuilder
}

func (r StepRef) ID() string { return r.id }

func (r StepRef) After(refs ...StepRef) StepRef {
    for _, ref := range refs {
        r.builder.steps[r.index].DependsOn = append(
            r.builder.steps[r.index].DependsOn, ref.id,
        )
    }
    return r
}

func (r StepRef) WithTimeout(d time.Duration) StepRef {
    r.builder.steps[r.index].Timeout = d
    return r
}
```

**Builder changes** in `dag/builder.go`:

```go
func (b *WorkflowBuilder) Task(id, task string) StepRef {
    b.steps = append(b.steps, StepDef{ID: id, Task: task, Type: StepTypeNormal})
    b.current = len(b.steps) - 1
    return StepRef{id: id, index: b.current, builder: b}
}
```

**Usage:**

```go
wf := dag.NewWorkflow("code-review")
plan := wf.Task("plan", "llm-planner")
code := wf.Task("code", "llm-coder").After(plan)
test := wf.Task("test", "test-runner").After(code)
fix  := wf.AgentLoop("fix", "llm-fixer").After(test).
    WithMaxIterations(10).WithMaxDuration(5 * time.Minute)
```

**Files:** `dag/stepref.go` (new), `dag/builder.go` (modify returns),
`dag/builder_test.go` (update for new API). Add `Name()` accessor to builder.

**Tests:** Verify `After()` wires DependsOn correctly. Verify chaining returns
same ref. Verify modifiers (`WithTimeout`, `WithMaxIterations`) target correct
step. Verify old `DependsOn(string)` still works.

---

## 2. `NonRetryableError` in `worker/`

**Problem:** Every error returned from a handler triggers retry via
`NakWithDelay`. Handlers that encounter permanent failures (invalid input,
content policy violation) have no way to signal "don't retry this."

**Design:**

```go
// worker/errors.go
type NonRetryableError struct {
    Err error
}

func (e *NonRetryableError) Error() string { return e.Err.Error() }
func (e *NonRetryableError) Unwrap() error { return e.Err }

func NewNonRetryableError(err error) *NonRetryableError {
    if err == nil {
        panic("NewNonRetryableError: err must not be nil")
    }
    return &NonRetryableError{Err: err}
}
```

**Worker change** in `handleMessage`:

```go
err = handler(ctx)
if err != nil {
    var nonRetryable *NonRetryableError
    if errors.As(err, &nonRetryable) {
        ctx.Fail(nonRetryable.Err)
        msg.Ack()
        return
    }
    msg.NakWithDelay(5 * time.Second)
    return
}
msg.Ack()
```

**Files:** `worker/errors.go` (new), `worker/worker.go` (modify
`handleMessage`).

**Tests:** Verify `NonRetryableError` wraps/unwraps via `errors.As`. Integration
test: handler returns `NonRetryableError`, verify `msg.Ack()` (no retry) and
`step.failed` event published.

---

## 3. `RetryCount()` on `TaskContext`

**Problem:** Handlers can't know which attempt they're on. Useful for "succeed
on retry 2" patterns or degraded-mode behavior on later attempts.

**Design:** Add `Attempt int` to `protocol.TaskPayload`. Orchestrator sets it
from `StepState.Attempts` when publishing. Expose on `TaskContext`:

```go
// worker/worker.go — TaskContext interface
RetryCount() int

// worker/context.go — implementation
func (c *taskContext) RetryCount() int { return c.attempt }
```

**Files:** `protocol/protocol.go` (add `Attempt`), `worker/context.go` (add
field + method), `worker/worker.go` (add to interface),
`engine/orchestrator.go` (set attempt in `publishTask`).

**Tests:** Verify `RetryCount()` returns 0 on first attempt. Verify it matches
`TaskPayload.Attempt` after unmarshaling.

---

## 4. `worker.Typed[I,O]()` Helper

**Problem:** Every handler manually calls `json.Unmarshal(ctx.Input(), &input)`
and `json.Marshal(output)`. Boilerplate that obscures the actual task logic.

**Design:** Generic wrapper in `worker/`:

```go
// worker/typed.go
type TypedHandlerFunc[I, O any] func(ctx TaskContext, input I) (O, error)

func Typed[I, O any](fn TypedHandlerFunc[I, O]) HandlerFunc {
    return func(ctx TaskContext) error {
        var input I
        if ctx.Input() != nil {
            if err := json.Unmarshal(ctx.Input(), &input); err != nil {
                return NewNonRetryableError(
                    fmt.Errorf("unmarshal input: %w", err),
                )
            }
        }
        output, err := fn(ctx, input)
        if err != nil {
            return err
        }
        data, err := json.Marshal(output)
        if err != nil {
            return NewNonRetryableError(
                fmt.Errorf("marshal output: %w", err),
            )
        }
        return ctx.Complete(data)
    }
}
```

Note: JSON marshal/unmarshal failures are wrapped in `NonRetryableError` — bad
serialization won't fix itself on retry.

**Usage:**

```go
w.Handle("llm-planner", worker.Typed(func(ctx worker.TaskContext, in PlanInput) (PlanOutput, error) {
    return PlanOutput{Steps: plan(in.Repo)}, nil
}))
```

**Files:** `worker/typed.go` (new).

**Tests:** Typed handler round-trips struct through JSON correctly. Nil input
produces zero-value struct. Marshal failure returns `NonRetryableError`.

---

## 5. `PutStream()` on `TaskContext`

**Problem:** LLM pipelines need real-time token streaming. Users want to see
output as it's generated, not after the step completes.

**Design:** Core NATS pub/sub (not JetStream — tokens are ephemeral). Requires
adding `nc *nats.Conn` to `taskContext`:

```go
// worker/context.go
func (c *taskContext) PutStream(data []byte) error {
    subject := fmt.Sprintf("stream.%s.%s", c.runID, c.stepID)
    return c.nc.Publish(subject, data)
}
```

Clients subscribe to `stream.{run_id}.{step_id}` for real-time delivery. No
persistence, no delivery guarantees, no JetStream stream needed.

**Files:** `worker/context.go` (add `nc` field, add method), `worker/worker.go`
(pass `nc` when constructing context, add to interface).

**Tests:** Integration test: subscribe to stream subject, call `PutStream`,
verify message received. Verify subject format.

---

## 6. `Retries int` on `StepDef`

**Problem:** Retry count is currently implicit in NATS consumer config. The
workflow definition should declare how many times a step can be retried — it's a
workflow-level decision, not an infrastructure decision.

**Design:** Single field on `StepDef`:

```go
// dag/types.go
type StepDef struct {
    // ... existing fields ...
    Retries int `json:"retries,omitempty"`
}
```

Builder method:

```go
func (r StepRef) WithRetries(n int) StepRef {
    r.builder.steps[r.index].Retries = n
    return r
}
```

The orchestrator tracks attempts in `StepState.Attempts`. When a `step.failed`
event arrives and `Attempts < Retries`, the step stays eligible for redelivery
via `NakWithDelay`. When `Attempts >= Retries`, the orchestrator marks the step
as permanently failed.

Validation: `Retries` must be >= 0. Zero means no retries (fail on first
error).

**Files:** `dag/types.go` (add field), `dag/builder.go` (add to `StepRef`),
`dag/validate.go` (validate non-negative), `engine/orchestrator.go` (check
attempts vs retries in `handleStepFailed`).

**Tests:** Verify validation rejects negative retries. Integration test: step
with `Retries: 2` fails twice then succeeds on third attempt.

---

## 7. `SkipIf` with `ParentCond` on `StepDef`

**Problem:** DagNats has a static DAG — steps either run or don't based on
parent completion. Real workflows need conditional branching: "skip code review
if diff < 10 lines." Putting this in handlers breaks the engine's ability to
skip downstream steps and report accurate status.

**Design:** One condition type only — parent output field comparison. Evaluated
purely in `dag/`, no I/O, no timers, no event watching.

```go
// dag/condition.go

// ParentCond evaluates a simple comparison on a parent step's JSON output.
// Field is a top-level key. Op is one of: ==, !=, <, >, <=, >=.
// Value is the comparison target.
type ParentCond struct {
    StepID string      `json:"step_id"`
    Field  string      `json:"field"`
    Op     string      `json:"op"`
    Value  interface{} `json:"value"`
}

// Evaluate returns true when the condition is satisfied.
func (c *ParentCond) Evaluate(steps map[string]StepState) bool {
    state, ok := steps[c.StepID]
    if !ok || state.Output == nil {
        return false
    }
    var data map[string]interface{}
    if err := json.Unmarshal(state.Output, &data); err != nil {
        return false
    }
    fieldVal, ok := data[c.Field]
    if !ok {
        return false
    }
    return compare(fieldVal, c.Op, c.Value)
}
```

Add to `StepDef`:

```go
SkipIf *ParentCond `json:"skip_if,omitempty"`
```

`ResolveReady` changes: when all deps are satisfied and `SkipIf` evaluates
true, the step is marked `Skipped` instead of being enqueued. `completedSet`
in the orchestrator includes `Skipped` status so downstream steps treat
skipped parents as satisfied.

Builder convenience using `StepRef`:

```go
func SkipIfOutput(parent StepRef, field, op string, value interface{}) *ParentCond {
    return &ParentCond{StepID: parent.ID(), Field: field, Op: op, Value: value}
}

func (r StepRef) SkipIf(cond *ParentCond) StepRef {
    r.builder.steps[r.index].SkipIf = cond
    return r
}
```

**Usage:**

```go
review := wf.Task("review", "llm-reviewer").After(test).
    SkipIf(dag.SkipIfOutput(test, "line_count", "<", 10))
```

Validation in `dag/validate.go`: `SkipIf.StepID` must be in the step's
`DependsOn` list (can only condition on a parent's output). `Op` must be one of
the 6 comparison operators. Reject at definition time.

**Files:** `dag/condition.go` (new — `ParentCond`, `Evaluate`, `compare`),
`dag/types.go` (add `SkipIf` to `StepDef`), `dag/resolve.go` (condition-aware
resolution), `dag/validate.go` (validate `SkipIf`), `dag/builder.go` (add
`SkipIf` to `StepRef`), `engine/orchestrator.go` (include `Skipped` in
`completedSet`, handle skip in `enqueueReady`).

**Tests:** Pure unit tests for `ParentCond.Evaluate` — all 6 operators, missing
field, nil output, type mismatches. `ResolveReady` with `SkipIf` — verify step
marked skipped, verify downstream still resolves. Validation rejects `SkipIf`
referencing non-parent step.

---

## 8. `LoopDelay` on `AgentLoopConfig`

**Problem:** LLM agent loops often need a delay between iterations (API rate
limits, cooling off). Without this, every handler adds its own `time.Sleep`,
duplicating the same concern.

**Design:** One field on `AgentLoopConfig`:

```go
type AgentLoopConfig struct {
    MaxIterations int           `json:"max_iterations"`
    MaxDuration   time.Duration `json:"max_duration,omitempty"`
    LoopDelay     time.Duration `json:"loop_delay,omitempty"`
}
```

Builder method:

```go
func (r StepRef) WithLoopDelay(d time.Duration) StepRef {
    if r.builder.steps[r.index].Loop == nil {
        panic("WithLoopDelay called on non-AgentLoop step")
    }
    r.builder.steps[r.index].Loop.LoopDelay = d
    return r
}
```

The orchestrator applies the delay when re-enqueuing after a `step.continue`
event. Implementation: `time.AfterFunc(loopDelay, publishIterationTask)`. If
`LoopDelay` is zero, re-enqueue immediately (current behavior).

**Files:** `dag/types.go` (add field), `dag/builder.go` (add to `StepRef`),
`engine/orchestrator.go` (delay in `handleStepContinue`).

**Tests:** Integration test: agent loop with `LoopDelay: 100ms`, verify
iterations are spaced by at least 100ms. Verify zero delay preserves current
immediate re-enqueue behavior.

---

## Implementation Order

Each step is independently shippable and testable. TDD: red, green, refactor.

1. **`StepRef`** — foundation; other items use `StepRef` in their builder API
2. **`NonRetryableError`** — standalone; `Typed[I,O]` depends on it
3. **`RetryCount()`** — standalone
4. **`worker.Typed[I,O]()`** — depends on `NonRetryableError`
5. **`PutStream()`** — standalone
6. **`Retries`** — standalone
7. **`SkipIf`** — depends on `StepRef` for builder API
8. **`LoopDelay`** — depends on `StepRef` for builder API

## What's Deferred

These features are explicitly NOT in scope. Build them when a production
workflow proves the need:

- `sdk/` convenience package (shallow wrapper)
- `StandaloneTask` shorthand (saves 2 lines)
- Full condition system (`WaitFor`, `SleepCond`, `UserEventCond`, `Or`/`And`)
- `SleepFor` in task context (complex state machine)
- `BackoffConfig` (single `RetryDelay` field if needed later)
- Concurrency keys and framework
- Rate limiting
- Worker labels and routing
- Sticky worker affinity
- Dashboard / web UI
