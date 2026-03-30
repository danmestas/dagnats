# Deferred Features

Features identified in the Hatchet comparison that were deliberately deferred
after Ousterhout/Hipp review. Each has a clear trigger for when to build it.

## High Priority (gate production adoption)

### Concurrency Control
Expression-based concurrency keys with strategies.
```go
hatchet.WithWorkflowConcurrency(types.Concurrency{
    Expression:    "input.userId",
    MaxRuns:       &maxRuns,
    LimitStrategy: &strategy, // WAIT, CANCEL_IN_PROGRESS
})
```
**NATS approach:** KV-backed tracker. Key `concurrency.{expression_value}` holds
active run IDs. Orchestrator checks before enqueuing. CAS via KV revision.
**Build when:** Multiple users trigger workflows concurrently.

### Rate Limiting
Distributed token bucket for external API calls.
```go
hatchet.WithRateLimits(&types.RateLimit{
    Key: "openai-api", Units: &units, Duration: &duration,
})
```
**NATS approach:** KV-backed token bucket with lazy refill on Acquire. No
background goroutine. Steps declare rate limit keys on StepDef.
**Build when:** Workers hit external API rate limits (OpenAI, Anthropic).

### WaitFor Conditions (sleep, user event, composition)
Pause step execution until a condition is met.
```go
hatchet.WithWaitFor(hatchet.SleepCondition(10 * time.Second))
hatchet.WithWaitFor(hatchet.UserEventCondition("approval:granted", ""))
hatchet.WithWaitFor(hatchet.OrCondition(sleep, event))
```
**Design tension:** Sleep and event conditions are I/O concerns. They belong in
the orchestrator, not `dag/`. `SkipIf` (parent output conditions) is already in
`dag/` as pure evaluation. `WaitFor` adds `StepStatusWaiting` and requires the
orchestrator to manage timers and event subscriptions.
**NATS approach:** Sleep via `time.AfterFunc` publishing `step.timer_fired` to
history stream. Events via watching `event.{run_id}.{event_key}` subjects.
**Build when:** First human-in-the-loop workflow requirement.

## Medium Priority (DX improvement)

### Co-located Handlers
Handlers defined inline with the DAG, not registered separately.
```go
// Hatchet style — graph + logic in one place
step1 := workflow.NewTask("step-1",
    func(ctx hatchet.Context, input I) (O, error) { ... })
```
**Design tension:** `dag/` must stay pure (no NATS deps). Co-location requires a
higher-level package that imports both `dag/` and `worker/`. The Ousterhout
review recommended against a new `sdk/` package (shallow wrapper). Alternative:
enrich the builder to accept handler references without importing NATS — store
handlers as `any` in `StepDef`, cast in `worker/` at registration time.
**Build when:** User feedback confirms the two-file pattern is a pain point.

### Standalone Tasks
Single-step workflows without DAG ceremony.
```go
task := sdk.StandaloneTask[I, O]("process", handler)
result, _ := task.Run(ctx, input)
```
**Current workaround:** 3 lines with the builder (NewWorkflow + Task + Build).
**Build when:** The 3-line pattern appears in >5 places in the codebase.

## Low Priority (premature at current scale)

### Worker Labels and Affinity
Route tasks to workers with specific capabilities.
```go
hatchet.WithLabels(map[string]any{"gpu": "a100"})
hatchet.WithDesiredWorkerLabels(map[string]*DesiredWorkerLabel{
    "gpu": {Value: "a100", Required: true},
})
```
**NATS approach:** Subject hierarchies. `task.llm-coder.a100.{runID}` with
workers subscribing via wildcards. Sticky affinity via KV.
**Build when:** Heterogeneous worker fleet (GPU vs CPU, regional routing).

### BackoffConfig
Configurable retry backoff (factor + max delay).
```go
hatchet.WithRetryBackoff(2, 10) // factor=2, max=10s
```
**Current state:** Orchestrator re-publishes immediately on retry. A single
`RetryDelay time.Duration` field on `StepDef` could be added as the minimal
version if immediate re-publish causes thundering herd.
**Build when:** Retry storms observed in production.

### Durable SleepFor
Mid-step sleep that releases the worker slot and resumes later.
```go
ctx.SleepFor(30 * time.Second) // releases slot, resumes after
```
**Current workaround:** `LoopDelay` on `AgentLoopConfig` provides delay between
agent loop iterations. Covers the rate-limit spacing case but not arbitrary
mid-step pauses.
**Design tension:** Requires handler state machine (save state before sleep,
resume from state after). Complex for users. The `Continue()` + `LoopDelay`
pattern is simpler for the agent loop use case.
**Build when:** A real use case requires mid-step pause that `LoopDelay` can't
cover.
