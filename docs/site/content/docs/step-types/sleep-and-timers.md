---
title: Sleep and Timers
weight: 5
---

A **sleep step** introduces a durable delay into a workflow -- the engine handles the timer entirely, with no worker involved.

## Overview

Sleep steps pause workflow execution for a specified duration. Unlike a `time.Sleep()` in application code, a DagNats sleep is durable: it survives engine restarts, NATS reconnections, and server reboots. The delay is stored as a `WakeAt` timestamp in the step state, and the engine uses the `SLEEP_TIMERS` JetStream stream with `NakWithDelay` to schedule the wake-up.

No worker participates in a sleep step. The engine publishes a timer message to `SLEEP_TIMERS`, the NATS consumer NAKs it with the requested delay, and when NATS redelivers the message after the delay expires, the engine publishes a `step.sleep.completed` event to advance the workflow.

This is distinct from the worker-level `Pause()` method, which checkpoints mid-task state and uses `NakWithDelay` to resume the same worker handler. Sleep steps are DAG-level delays between steps; `Pause()` is a within-step delay that keeps the step in `Running` status.

## How It Works

```mermaid
sequenceDiagram
    participant Engine
    participant SLEEP as SLEEP_TIMERS
    participant History as WORKFLOW_HISTORY

    Engine->>Engine: step ready, type = sleep
    Engine->>SLEEP: publish timer (action: sleep_complete)
    Engine->>History: step.sleep.started
    Note over SLEEP: NakWithDelay(duration)
    SLEEP-->>Engine: redeliver after delay
    Engine->>History: step.sleep.completed
    Note over Engine: downstream steps now ready
```

The `SLEEP_TIMERS` stream is shared across multiple timer use cases via an **action discriminator** in the message payload. Sleep steps use the `sleep_complete` action. The same stream handles wait-for-event timeouts (`wait_timeout`), rate-limit retries (`rate_retry`), and other scheduled operations.

The step's `WakeAt` field in `StepState` records the expected completion time. This is purely informational -- the actual wake-up is driven by NATS redelivery, not by polling the timestamp.

## Usage

```go
wf := dag.NewWorkflow("rate-limited-pipeline")

call := wf.Task("call-api", "api-call").
    WithTimeout(10 * time.Second)

cooldown := wf.Sleep("cooldown", 30*time.Second).
    After(call)

next := wf.Task("next-call", "api-call").
    After(cooldown)

def, err := wf.Build()
```

## Configuration

Sleep configuration is stored in `StepDef.Config` as `SleepConfig`:

| Field | Type | Purpose |
|-------|------|---------|
| `duration` | `time.Duration` | How long to sleep. Must be positive. Fixed when the def is registered. |
| `cron` | `string` | Sleep until the next occurrence of this 5-field cron expression, strictly after dispatch. |
| `until_input_path` | `string` | Dot-path into the **run** input naming the deadline. Resolved at dispatch. |

Exactly one of the three must be set; a config with zero or more than one is rejected when the workflow def is registered, as is a malformed `cron` expression.

`duration` fixes the delay at registration time. `cron` and `until_input_path` defer it to dispatch, which is what makes calendar waits and payload-carried deadlines expressible as sleep steps rather than as a worker that blocks on a task lease.

**`cron` form:**

Uses the same 5-field grammar as cron triggers (`*`, `*/N`, `N-M`, comma lists; numeric day-of-week, no symbolic names). All five fields are ANDed, so day-of-month and day-of-week intersect rather than union.

```go
// Next Monday at 09:00.
Config: dag.MarshalConfig(&dag.SleepConfig{Cron: "0 9 * * 1"})
```

{{< callout type="warning" >}}
**Sleep cron has no timezone field.** Unlike cron *triggers*, which carry an explicit `Timezone`, a sleep step's cron expression is evaluated in the **orchestrator process's local zone**. `"0 9 * * 1"` means 09:00 wherever the engine runs, not 09:00 UTC. If you need a specific zone, use `until_input_path` with an RFC3339 instant, which carries its own offset.
{{< /callout >}}

**`until_input_path` form:**

The path resolves against the run input -- not the step's resolved input, which for a mid-DAG step is upstream output and the wrong source for a run-scoped deadline. The target value is either an RFC3339 instant or a number of milliseconds.

```go
// Run input: {"deadline": "2026-08-01T12:00:00Z"}
Config: dag.MarshalConfig(&dag.SleepConfig{UntilInputPath: "deadline"})
```

An RFC3339 instant already in the past clamps to a zero-length sleep that completes normally. A missing path, an unparseable value, or a cron expression with no next occurrence fails the step at dispatch.

**Bounds:**

- Maximum: **365 days**
- A warning is logged for durations exceeding 30 days
- The engine sets `StepState.WakeAt` to `now + duration` when the sleep starts

**Sleep vs Pause:**

| | Sleep Step | Worker Pause |
|---|-----------|-------------|
| Scope | DAG-level, between steps | Within a step handler |
| Worker | None | Same worker resumes |
| Step status | Transitions through started/completed | Stays `Running` |
| Builder | `wf.Sleep(id, duration)` | `ctx.Pause(name, duration)` |
| Mechanism | `SLEEP_TIMERS` stream | Checkpoint + `NakWithDelay` |

## Related

- [Wait for Event](/docs/step-types/wait-for-event) -- pause until an external event arrives
- [Normal Steps](/docs/step-types/normal-steps) -- standard task execution
