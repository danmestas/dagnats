---
title: Workflow Schema
weight: 4
---

Reference for the JSON format used by `dagnats workflow register` and `dagnats workflow validate`.

{{< callout type="info" >}}
**IDE support:** Point your editor at [`workflow-schema.json`](https://github.com/danmestas/dagnats/blob/main/docs/workflow-schema.json) for autocomplete and validation. Add `"$schema": "./path/to/workflow-schema.json"` to your workflow files.
{{< /callout >}}

Durations are Go duration strings (`"30s"`, `"5m"`, `"1h30m"`) or integer nanoseconds (`30000000000`). Strings work in every duration field, including those inside a step's `config`. Definitions dagnats writes back, such as `dagnats workflow show --json`, use nanoseconds. Go duration strings have no day unit; write `"24h"`, not `"1d"`.

A workflow file may also carry a top-level `$schema` key (ignored) and, for `dagnats workflow register`, a `triggers` array that registers triggers with the workflow. Other unknown top-level keys are rejected by both `register` and `validate`.

---

## WorkflowDef

Top-level workflow definition object.

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| Name | `name` | string | Yes | -- | Unique workflow identifier |
| Version | `version` | string | No | "" | Free-form version label for your own bookkeeping; not validated |
| Steps | `steps` | []StepDef | Yes | -- | At least one step required |
| DefaultRetry | `default_retry` | RetryPolicy | No | nil | Fallback retry for all steps |
| Concurrency | `concurrency` | ConcurrencyLimit | No | nil | Workflow-level parallelism limits (`max_steps` 0 to 1000) |
| Timeout | `timeout` | duration | No | 0 | Overall workflow deadline |
| InputSchema | `input_schema` | JSON | No | nil | JSON Schema for workflow input validation |
| OutputSchema | `output_schema` | JSON | No | nil | JSON Schema for workflow output validation |
| AuxSteps | `aux_steps` | map[string]bool | No | nil | Steps whose outcome does not decide the run's status (e.g. `on_failure` handlers) |
| IdempotencyKey | `idempotency_key` | string | No | "" | Input path whose value dedups run starts |
| Sticky | `sticky` | string | No | "" | Worker affinity: `soft` or `hard` (`hard` requires a workflow `timeout`) |
| Priority | `priority` | PriorityConfig | No | nil | Input-driven dispatch priority (`key`, `rules`, `default_offset`) |
| CancelOn | `cancel_on` | []CancelOn | No | nil | Events that cancel the run (`event`, `match`, `timeout`); max 5 |
| Singleton | `singleton` | SingletonConfig | No | nil | One run at a time: `mode` (`skip` or `cancel`), optional `key` |

**Minimal example:**

```json
{
  "name": "hello",
  "version": "1.0",
  "steps": [
    {"id": "greet", "task": "greet", "timeout": "30s", "type": "normal"}
  ]
}
```

---

## StepDef

Individual step within a workflow.

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| ID | `id` | string | Yes | -- | Unique within the workflow |
| Task | `task` | string | Yes | -- | Task name workers subscribe to: 1 to 128 bytes of `A-Za-z0-9_.-`, no leading or trailing `.`, no empty token |
| DependsOn | `depends_on` | []string | No | [] | Step IDs that must complete first |
| Retries | `retries` | int | No | 0 | Legacy retry count (prefer `retry`): a fixed 5s delay between attempts. 0 to 100000 |
| Timeout | `timeout` | duration | No | 0 | Per-attempt deadline, enforced by the engine. `0` arms no timeout. It runs from when the current attempt first started: a redelivered or resumed task re-arms it only for the time left, so time spent paused counts. A retry gets the full timeout |
| Type | `type` | StepType | No | `normal` | Execution semantics (see below). An unknown value is an error |
| Config | `config` | object | Depends on type | nil | Type-specific settings, e.g. AgentLoopConfig for `agent_loop`, sleep settings for `sleep` |
| SkipIf | `skip_if` | ParentCond | No | nil | Conditional skip based on parent output |
| Metadata | `metadata` | map[string]string | No | nil | Static key-value pairs, delivered to the worker in the task payload |
| Retry | `retry` | RetryPolicy | No | nil | Structured retry policy (overrides `retries`) |
| WorkerGroup | `worker_group` | string | No | "" | Routes to a specific worker pool. Same character rule as `task`, but no dots |
| OnFailure | `on_failure` | string | No | "" | Step ID to run on failure |
| Compensate | `compensate` | string | No | "" | Step ID for saga compensation |
| RateLimit | `rate_limit` | RateLimit | No | nil | `limit` dispatches per `period` for this step's task |
| KeyedRateLimit | `keyed_rate_limit` | KeyedRateLimit | No | nil | Rate limit per input key (`key`, `limit`, `period`, `units`) |
| MaxTaskConcurrency | `max_task_concurrency` | int | No | 0 | Max in-flight tasks of this step's task type; 0 = unlimited, max 1000 |
| Singleton | `singleton` | bool | No | false | Only one instance of this step runs at a time |
| RequiredCapabilities | `required_capabilities` | []string | No | nil | Capabilities the workflow must be granted, e.g. `control-plane` |

**Example with dependencies and retry:**

```json
{
  "id": "lint",
  "task": "lint.run",
  "timeout": "5m",
  "type": "normal",
  "depends_on": ["fetch-diff"],
  "worker_group": "lint-workers",
  "retry": {
    "max_attempts": 3,
    "strategy": "exponential",
    "initial_delay": "2s",
    "max_delay": "30s",
    "multiplier": 2.0
  }
}
```

---

## StepType Values

| Value | Description |
|-------|-------------|
| `normal` | Runs once, completes or fails |
| `agent_loop` | Iterates until termination signal; requires an AgentLoopConfig in `config` |
| `sub_workflow` | Delegates to a nested workflow DAG |
| `agent` | Single autonomous agent execution |
| `map` | Fans out one task per item of an input array |
| `sleep` | Durable delay: `duration`, `until_input_path`, or `cron` (with optional `cron_timezone` / `cron_timezone_input_path`) |
| `wait_for_event` | Waits for a matching external event |
| `approval` | Waits for a human approval (`timeout`, `subject`, optional `description`) |
| `planner` | Generates and runs steps at runtime |
| `respond` | Sends the HTTP response for an HTTP trigger (`status`, `headers`, `body_from`, `content_type`) |

---

## Step config by type

A step's `config` holds the settings for its `type`. A typed step whose config does not parse is rejected at registration.

### sleep

Exactly one of `duration`, `cron` or `until_input_path`.

| Field | Type | Description |
|-------|------|-------------|
| `duration` | duration | Fixed delay. Must be positive |
| `cron` | string | Wake at the next occurrence of a cron expression, strictly after dispatch |
| `until_input_path` | string | Input path holding a deadline: an RFC3339 instant or integer milliseconds |
| `cron_timezone` | string | IANA zone the cron expression is evaluated in, e.g. `America/Denver` |
| `cron_timezone_input_path` | string | Input path that supplies the zone at run time |

`cron_timezone` is validated when the workflow is registered. `cron_timezone_input_path` is read at dispatch; a missing or unknown zone there falls back to UTC with a logged warning. Both apply only with `cron`, and at most one may be set. With neither, the cron expression is evaluated in UTC, not the host's local zone.

```json
{"id": "morning", "task": "sleep", "type": "sleep", "depends_on": ["prep"],
 "config": {"cron": "0 9 * * 1-5", "cron_timezone": "America/Denver"}}
```

### map

| Field | Type | Description |
|-------|------|-------------|
| `max_items` | int | Cap on the input array, 1 to 10000 |

A map step must have exactly one dependency, and that dependency may not itself be a map step.

### approval

| Field | Type | Description |
|-------|------|-------------|
| `timeout` | duration | How long to wait. Positive, at most 168h |
| `subject` | string | Required. Names the approval request |
| `description` | string | Optional text shown to the approver |
| `metadata` | map[string]string | Optional extra context |

### wait_for_event

| Field | Type | Description |
|-------|------|-------------|
| `event` | string | Event name to wait for |
| `match` | object | `left` (a path into the event data), `op` (`eq`), `right` (`input.<path>` or `step.<id>.<path>`, resolved when the wait starts) |
| `timeout` | duration | Required, positive |

### sub_workflow

| Field | Type | Description |
|-------|------|-------------|
| `workflow` | string | Name of the child workflow |
| `detach` | bool | When true, the parent step completes as soon as the child is spawned |

### planner

| Field | Type | Description |
|-------|------|-------------|
| `max_steps` | int | Cap on generated steps, 1 to 100 |
| `max_depth` | int | Nesting cap, 0 to 10 |
| `allowed_tasks` | []string | Task types the planner may use |

### respond

`status` (default 200), `headers`, `body_from`, `content_type` (default `application/json`). See the HTTP trigger guide.

### agent_loop

See [AgentLoopConfig](#agentloopconfig) below.

---

## RetryPolicy

Resolution order: step `retry` > workflow `default_retry` > legacy `retries` > none.

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| MaxAttempts | `max_attempts` | int | Yes | -- | Total retry attempts; 0 means no retries |
| Strategy | `strategy` | RetryStrategy | Yes | -- | Backoff algorithm |
| InitialDelay | `initial_delay` | duration | Yes | -- | Base delay between attempts |
| MaxDelay | `max_delay` | duration | Yes | -- | Delay cap (0 = uncapped) |
| Multiplier | `multiplier` | float64 | No | 0 | Used by `exponential` strategy only. 0 or omitted means 2 |

### RetryStrategy Values

| Value | Delay Formula | Example (initial=2s) |
|-------|---------------|---------------------|
| `fixed` | `initial_delay` every attempt | 2s, 2s, 2s |
| `linear` | `initial_delay * attempt` | 2s, 4s, 6s |
| `exponential` | `initial_delay * multiplier^(attempt-1)` | 2s, 4s, 8s (multiplier=2) |

**Example:**

```json
{
  "max_attempts": 5,
  "strategy": "exponential",
  "initial_delay": "1s",
  "max_delay": "30s",
  "multiplier": 2.0
}
```

---

## AgentLoopConfig

Set in the step's `config` when its type is `agent_loop`.

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| MaxIterations | `max_iterations` | int | Yes | -- | Upper bound on loop cycles; must be > 0 |
| MaxDuration | `max_duration` | duration | No | 0 | Wall-clock limit; whichever fires first wins |
| LoopDelay | `loop_delay` | duration | No | 0 | Pause between iterations |
| BudgetWarnAt | `budget_warn_at` | int | No | 0 | Advisory: iteration at which workers should warn the model |
| BudgetForceAt | `budget_force_at` | int | No | 0 | Advisory: iteration at which workers should force a wrap-up |

**Example:**

```json
{
  "id": "review-loop",
  "task": "agent.code-review",
  "timeout": "15m",
  "type": "agent_loop",
  "config": {
    "max_iterations": 10,
    "max_duration": "10m",
    "loop_delay": "2s"
  }
}
```

---

## ConcurrencyLimit

Controls parallelism at the workflow level.

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| MaxRuns | `max_runs` | int | No | 0 | Max parallel runs of this workflow |
| MaxSteps | `max_steps` | int | No | 0 | Max parallel steps within a run |

**Example:**

```json
{
  "concurrency": {
    "max_runs": 3,
    "max_steps": 2
  }
}
```

---

## ParentCond (skip_if)

Evaluates a comparison against a parent step's JSON output. The parent step must be listed in the step's `depends_on`.

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| StepID | `step_id` | string | Yes | -- | Parent step to check (must be in `depends_on`) |
| Field | `field` | string | Yes | -- | Top-level key in parent's output JSON |
| Op | `op` | string | Yes | -- | Comparison operator |
| Value | `value` | any | Yes | -- | Value to compare against |

**Valid operators:** `==`, `!=`, `<`, `>`, `<=`, `>=`

Types are compared as float64 (numbers), string, or bool (equality only).

**Example:**

```json
{
  "id": "post-results",
  "task": "github.post-comment",
  "timeout": "1m",
  "type": "normal",
  "depends_on": ["lint"],
  "skip_if": {
    "step_id": "lint",
    "field": "issues_found",
    "op": "==",
    "value": 0
  }
}
```

This step is skipped when the `lint` step's output has `issues_found` equal to 0.

---

## Validation Rules

The following rules are enforced by `dag.Validate()`:

| # | Rule |
|---|------|
| 1 | Workflow must have a non-empty `name` |
| 2 | Workflow must contain at least one step |
| 3 | Every step must have a unique `id` |
| 4 | Every step must have a non-empty `task` |
| 5 | `depends_on` entries must reference existing step IDs |
| 6 | `on_failure` must reference an existing step ID (if set) |
| 7 | `compensate` must reference an existing step ID (if set) |
| 8 | `retries` must be 0 to 100000, and `retry` / `default_retry` `max_attempts` at most 100000; an `exponential` `multiplier` must be between 1 and 100 |
| 9 | `agent_loop` steps must have a `config` with `max_iterations > 0` |
| 10 | Each typed step's `config` must parse for its type (sleep, map, approval, respond, ...) |
| 11 | `skip_if.step_id` must be in the step's `depends_on` list |
| 12 | `skip_if.op` must be a valid operator |
| 13 | The step dependency graph must be acyclic (Kahn's algorithm) |
| 14 | `task` and `worker_group` follow the character rules above |
| 15 | `on_failure` and `compensate` targets must not depend on other steps or reference themselves |
| 16 | `idempotency_key` must not start or end with `.` or contain an empty segment |
| 17 | `sticky` must be `soft` or `hard`; `hard` requires a workflow `timeout`, and `sticky` cannot be combined with a per-step `worker_group` |
| 18 | `priority` needs a `key` and 1 to 20 `rules`, with offsets in [-600, 600]; `cancel_on` has at most 5 entries, each with an `event`; `singleton.mode` must be `skip` or `cancel` |

---

## Complete Example

```json
{
  "name": "code-review-pipeline",
  "version": "1.0.0",
  "timeout": "30m",
  "default_retry": {
    "max_attempts": 2,
    "strategy": "fixed",
    "initial_delay": "5s",
    "max_delay": "5s"
  },
  "concurrency": {
    "max_runs": 3,
    "max_steps": 2
  },
  "steps": [
    {
      "id": "fetch-diff",
      "task": "git.fetch-diff",
      "timeout": "2m",
      "type": "normal"
    },
    {
      "id": "lint",
      "task": "lint.run",
      "timeout": "5m",
      "type": "normal",
      "depends_on": ["fetch-diff"],
      "worker_group": "lint-workers"
    },
    {
      "id": "review-loop",
      "task": "agent.code-review",
      "timeout": "15m",
      "type": "agent_loop",
      "depends_on": ["fetch-diff"],
      "config": {
        "max_iterations": 10,
        "max_duration": "10m",
        "loop_delay": "2s"
      },
      "retry": {
        "max_attempts": 3,
        "strategy": "exponential",
        "initial_delay": "2s",
        "max_delay": "30s",
        "multiplier": 2.0
      }
    },
    {
      "id": "post-results",
      "task": "github.post-comment",
      "timeout": "1m",
      "type": "normal",
      "depends_on": ["lint", "review-loop"],
      "skip_if": {
        "step_id": "lint",
        "field": "issues_found",
        "op": "==",
        "value": 0
      },
      "on_failure": "notify-failure",
      "metadata": {
        "channel": "pull-requests"
      }
    },
    {
      "id": "notify-failure",
      "task": "notify.slack",
      "timeout": "30s",
      "type": "normal"
    }
  ]
}
```
