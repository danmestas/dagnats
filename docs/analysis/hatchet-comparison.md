# Hatchet vs DagNats: Deep Comparative Analysis

## Executive Summary

Hatchet is a production-grade, Postgres-backed workflow orchestrator with a polished
SDK, multi-tenancy, and a rich feature set built over years. DagNats is a
NATS-native, minimalist DAG engine optimized for LLM pipelines. This analysis
identifies what Hatchet gets right that we should learn from, and where our
architectural bets give us advantages we should protect.

---

## 1. What Hatchet Gets Right

### 1.1 Type-Safe, Closure-Based Workflow Definition

**Hatchet's approach (v1 SDK):**
```go
workflow := client.NewWorkflow("dag-workflow")
step1 := workflow.NewTask("step-1", func(ctx hatchet.Context, input Input) (Output, error) {
    return Output{Result: input.Value * 2}, nil
})
step2 := workflow.NewTask("step-2", func(ctx hatchet.Context, input Input) (Output, error) {
    var s1 StepOutput
    ctx.ParentOutput(step1, &s1)  // type-safe reference to parent
    return Output{Result: s1.Result + 10}, nil
}, hatchet.WithParents(step1))
```

**DagNats's approach:**
```go
wf := dag.NewWorkflow("code-review").
    Task("plan", "llm-planner").
    Task("code", "llm-coder").DependsOn("plan").
    Task("test", "test-runner").DependsOn("code")
```

**What Hatchet does better:**
- **Handlers are co-located with the DAG definition.** You see the graph AND the
  logic in one place. DagNats separates definition (builder) from execution
  (worker handler registration), which means the graph shape and the code that
  runs are in different files.
- **Parent references are object references, not strings.** `WithParents(step1)`
  is a compile-time guarantee. `DependsOn("plan")` is a runtime string match
  that can silently break.
- **Typed input/output.** Hatchet uses Go generics so `ctx.ParentOutput(step1, &s1)`
  knows the parent step's output type. DagNats passes `[]byte` everywhere.

**Lesson:** Consider a hybrid builder that accepts handler closures alongside
step definitions, and uses step references instead of string IDs for
dependencies. This eliminates an entire class of wiring bugs.

### 1.2 Standalone Tasks (Zero-DAG Escape Hatch)

Hatchet has `NewStandaloneTask` for one-off tasks that don't need a DAG:
```go
task := client.NewStandaloneTask("process-message", func(ctx hatchet.Context, input I) (O, error) {
    return O{Result: "done"}, nil
})
result, _ := task.Run(ctx, Input{Message: "hello"})
```

DagNats forces everything through a workflow definition, even trivial
single-step operations. For LLM pipelines, many operations are single-step
(call an LLM, run a tool). The DAG ceremony adds friction.

**Lesson:** Support standalone tasks as a first-class concept. They should
compile down to a single-step workflow internally (same execution model) but
with a simpler API surface.

### 1.3 Concurrency Control with CEL Expressions

Hatchet supports expression-based concurrency keys:
```go
hatchet.WithWorkflowConcurrency(types.Concurrency{
    Expression:    "input.userId",
    MaxRuns:       &maxRuns,
    LimitStrategy: &strategy,  // GROUP_ROUND_ROBIN, CANCEL_IN_PROGRESS, CANCEL_NEWEST
})
```

This lets you say "only 1 concurrent run per user" or "only 5 concurrent runs
per tier" without any custom infrastructure. DagNats has no concurrency control
beyond what NATS `MaxAckPending` provides at the consumer level.

**Lesson:** Expression-based concurrency keys are powerful for multi-tenant
LLM pipelines (rate limit per repo, per user, per org). This is worth building.
The `CANCEL_IN_PROGRESS` strategy is particularly useful for LLM agents where
a newer request supersedes an older one.

### 1.4 Rate Limiting as a First-Class Primitive

Hatchet has both static and dynamic rate limits:
```go
// Static: 10 requests per second globally
hatchet.WithRateLimits(&types.RateLimit{Key: "api-limit", Units: &units})

// Dynamic: per-user rate limit derived from input
hatchet.WithRateLimits(&types.RateLimit{
    Key:            "input.userId",
    LimitValueExpr: &userLimit,
    Duration:       &duration,
})
```

This is essential for LLM pipelines where you're calling external APIs with
rate limits. DagNats has nothing here — you'd have to build it into each worker.

**Lesson:** Rate limiting belongs in the orchestrator, not the worker. Workers
should declare their rate limit requirements; the scheduler should enforce them.
NATS KV could back a distributed rate limiter elegantly.

### 1.5 Conditional Execution and Skip Logic

Hatchet supports runtime conditions on steps:
```go
hatchet.WithSkipIf(hatchet.ParentCondition(step1, "output.random_number > 50"))
hatchet.WithWaitFor(hatchet.SleepCondition(10 * time.Second))
hatchet.WithWaitFor(hatchet.UserEventCondition("approval:granted", ""))
hatchet.WithWaitFor(hatchet.OrCondition(sleep, event))
```

DagNats has a static DAG — steps either run or don't based on parent
completion. There's no skip logic, no conditional branching, no sleep-until,
no wait-for-event at the step level.

**Lesson:** Conditional execution is essential for real-world workflows.
The `WaitFor` + `SkipIf` pattern with composable conditions (Or, And, Sleep,
Event) is elegant. We should design this into the `StepDef`, not bolt it on
later. For LLM pipelines: "skip code review if diff is < 10 lines" or "wait
for human approval before deploying."

### 1.6 Durable Tasks with `SleepFor`

Hatchet distinguishes "durable" tasks that can survive worker restarts:
```go
task := client.NewStandaloneDurableTask("long-running", func(ctx hatchet.DurableContext, input I) (O, error) {
    ctx.SleepFor(30 * time.Second)  // releases the worker slot, resumes later
    return O{Message: "done"}, nil
})
```

When `SleepFor` is called, the worker releases its slot and the scheduler
re-dispatches the task after the sleep completes. This is critical for LLM
agents that might wait minutes between iterations.

DagNats agent loops re-enqueue via `Continue()`, which is similar in spirit
but coarser — you can't sleep mid-step, only between iterations.

**Lesson:** Consider `SleepFor` semantics in the agent loop context. An LLM
agent that wants to wait for a rate limit reset or a human review shouldn't
hold a worker slot. The orchestrator should handle this as a native primitive.

### 1.7 Streaming Support

Hatchet supports mid-task streaming:
```go
func(ctx hatchet.DurableContext, input I) (O, error) {
    for _, token := range tokens {
        ctx.PutStream(token)  // stream tokens to client in real-time
    }
    return O{Done: true}, nil
}
```

For LLM pipelines, this is table stakes. Users want to see tokens as they're
generated, not wait for the full response.

**Lesson:** DagNats needs a streaming primitive. NATS subjects are natural
for this — publish tokens to `stream.{run_id}.{step_id}` and let clients
subscribe. This is trivial to implement on NATS; we just need the TaskContext
API.

### 1.8 Non-Retryable Errors

```go
return nil, worker.NewNonRetryableError(errors.New("invalid input"))
```

Hatchet distinguishes "this failed and should be retried" from "this failed
permanently." DagNats treats all failures the same — the retry policy always
applies.

**Lesson:** Add `NonRetryable` error type. LLM pipelines frequently encounter
permanent failures (invalid prompt, content policy violation, missing context)
that should not be retried.

### 1.9 Worker Labels and Affinity

```go
worker, _ := client.NewWorker("gpu-worker", hatchet.WithLabels(map[string]any{
    "gpu": "a100",
    "region": "us-east-1",
}))
// Task requests specific worker capabilities
hatchet.WithDesiredWorkerLabels(map[string]*DesiredWorkerLabel{
    "gpu": {Value: "a100", Required: true},
})
```

For LLM pipelines with heterogeneous infrastructure (GPU vs CPU, different
model providers, regional constraints), this is valuable.

**Lesson:** Worker labels map naturally to NATS subject hierarchies. A task
requiring `gpu=a100` could be published to `task.llm-coder.gpu.a100` with
workers subscribing to matching subjects. This could be simpler than Hatchet's
label-matching scheduler.

---

## 2. Where DagNats Has the Advantage

### 2.1 NATS vs Postgres for Queuing

Hatchet uses **Postgres as a message queue** with `LISTEN/NOTIFY`. This is
pragmatic (one fewer dependency) but has known scaling limitations:
- Polling with 1-second intervals for missed notifications
- Table bloat from queue rows requiring autovacuum monitoring
- `hatchet.db.bloat.dead_tuple_percent` is a metric they track — that tells
  you this is a real operational concern

DagNats uses NATS JetStream, which is purpose-built for message queuing:
- Sub-millisecond delivery, no polling
- Built-in consumer groups, ack/nak, redelivery
- No table bloat, no vacuum
- Horizontal scaling without partition rebalancing

**Protect this.** NATS-native is our core differentiator. Don't add Postgres.

### 2.2 Operational Simplicity

Hatchet production deployment requires:
- PostgreSQL 15.6+
- 4 separate services: migrate, admin, engine, api
- Optional: RabbitMQ, Prometheus, Grafana
- Partition management with heartbeats and rebalancing
- Database bloat monitoring and autovacuum tuning

DagNats requires:
- NATS server (single binary, ~20MB)

This is a massive advantage for the LLM pipeline use case where teams want to
ship fast, not operate infrastructure.

### 2.3 Pure DAG Logic Separation

DagNats's `dag/` package is zero-dependency pure logic. `Advance()` is a pure
function. This is fundamentally better for testing and reasoning than Hatchet's
approach where scheduling logic is entangled with Postgres queries and partition
management.

**Protect this.** Keep `dag/Advance` pure. Never let I/O leak in.

### 2.4 Event Sourcing

DagNats's `WORKFLOW_HISTORY` stream is an immutable event log. Hatchet uses
mutable row state in Postgres with OLAP batch updates. Event sourcing gives us:
- Perfect audit trail
- Replay/recovery from any point
- Temporal queries ("what was the state at time T?")

### 2.5 Agent Loop as a Native Primitive

DagNats designed for LLM agent loops from day one. Hatchet's "durable tasks"
are a more general abstraction, but agent loops are bolted on top. DagNats's
`Continue()` + `MaxIterations` + `MaxDuration` is cleaner for the specific
use case of iterative LLM agents.

---

## 3. Concrete Recommendations

### Priority 1: SDK Ergonomics (High Impact, Moderate Effort)

| Change | Rationale |
|--------|-----------|
| **Typed step references** instead of string IDs | Compile-time dependency safety |
| **Co-located handlers** in workflow builder | See graph + logic together |
| **Typed input/output** via generics | Eliminate `[]byte` casting everywhere |
| **Standalone task shorthand** | Single-step workflows without DAG ceremony |

Proposed API sketch:
```go
wf := dagnats.NewWorkflow("code-review")
plan := wf.Task("plan", planHandler)
code := wf.Task("code", codeHandler).DependsOn(plan)
test := wf.Task("test", testHandler).DependsOn(code)
fix  := wf.AgentLoop("fix", fixHandler).DependsOn(test).
    WithMaxIterations(10).WithMaxDuration(5 * time.Minute)
```

### Priority 2: Conditional Execution (High Impact, Moderate Effort)

Add to `StepDef`:
```go
type StepDef struct {
    // ... existing fields ...
    SkipIf  Condition `json:"skip_if,omitempty"`
    WaitFor Condition `json:"wait_for,omitempty"`
}
```

Conditions are evaluated by `dag.Advance` (still pure — conditions are
data, not closures). Support:
- `ParentOutputCondition(stepRef, "output.lineCount < 10")`
- `SleepCondition(duration)` (orchestrator manages the timer)
- `OrCondition`, `AndCondition` for composition

### Priority 3: Runtime Controls (Medium Impact, Low Effort)

| Feature | Implementation |
|---------|---------------|
| **Non-retryable errors** | New error type in worker package, orchestrator skips retry |
| **Retry count in context** | Add `ctx.RetryCount() int` to TaskContext |
| **Retry backoff config** | Add `Backoff` field to StepDef, map to `NakWithDelay` |
| **Streaming** | `ctx.PutStream(data)` publishes to `stream.{run_id}.{step_id}` |

### Priority 4: Concurrency & Rate Limiting (Medium Impact, Higher Effort)

- **Concurrency keys:** Use NATS KV to track active runs per key. Orchestrator
  checks before enqueuing. `MaxConcurrent` + `LimitStrategy` on StepDef.
- **Rate limiting:** NATS KV-backed token bucket. Workers declare rate limit
  keys; orchestrator enforces before dispatch.

### Priority 5: Worker Routing (Lower Priority)

- **Labels via NATS subjects:** `task.{worker_type}.{label1}.{label2}` with
  wildcard subscriptions. Simpler than Hatchet's label-matching scheduler.
- **Sticky workers:** Use NATS KV to store `run_id -> worker_id` affinity
  mapping. Re-route subsequent steps in the same run to the same worker.

---

## 4. What NOT to Copy from Hatchet

| Hatchet Feature | Why Not |
|-----------------|---------|
| **Postgres-backed queuing** | NATS JetStream is strictly better for this |
| **Multi-service deployment** | Keep it as few binaries as possible |
| **Partition-based distribution** | NATS consumer groups handle this natively |
| **gRPC worker protocol** | NATS pub/sub is simpler and sufficient |
| **Multi-tenant partitioning** | Premature for our scale; NATS accounts handle isolation |
| **Dashboard/web UI** | CLI-first per design doc; dashboard is a distraction now |
| **RabbitMQ alternative** | One messaging system, not two |
| **CEL expression engine** | Start with simple Go predicates; add expressions if needed |

---

## 5. Summary

Hatchet's biggest wins over DagNats are in **SDK ergonomics** (typed references,
co-located handlers, standalone tasks) and **runtime control primitives**
(concurrency keys, rate limiting, conditional execution, streaming). These are
features, not architecture — they can be added to DagNats without compromising
the NATS-native, event-sourced, operationally-simple core.

DagNats's architectural bets (NATS-native, pure DAG logic, event sourcing,
single-binary deployment, agent loops as primitives) are sound and should be
protected. The goal is to match Hatchet's developer experience while keeping
DagNats's operational simplicity.

**TL;DR:** Steal the SDK ergonomics and runtime primitives. Keep the architecture.
