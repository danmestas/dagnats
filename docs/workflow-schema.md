# Workflow Definition Schema

Reference for the JSON format used by `dagnats workflow register` and `dagnats workflow validate`.

> **IDE support:** Point your editor at [`workflow-schema.json`](workflow-schema.json) for autocomplete and validation. Add `"$schema": "./path/to/workflow-schema.json"` to your workflow files.

Durations are Go `time.Duration` strings: `"30s"`, `"5m"`, `"1h30m"`.

---

## WorkflowDef

Top-level workflow definition object.

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| Name | `name` | string | Yes | -- | Unique workflow identifier |
| Version | `version` | string | Yes | -- | Schema version for evolution |
| Steps | `steps` | []StepDef | Yes | -- | At least one step required |
| DefaultRetry | `default_retry` | RetryPolicy | No | nil | Fallback retry for all steps |
| Concurrency | `concurrency` | ConcurrencyLimit | No | nil | Workflow-level parallelism limits |
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
| Task | `task` | string | Yes | -- | Task name workers subscribe to |
| DependsOn | `depends_on` | []string | No | [] | Step IDs that must complete first |
| Retries | `retries` | int | No | 0 | Legacy retry count (prefer `retry`) |
| Timeout | `timeout` | duration | No | 0 | Per-attempt deadline, enforced by the engine. `0` arms no timeout |
| Type | `type` | StepType | No | `normal` | Execution semantics (see below) |
| Config | `config` | object | Depends on type | nil | Type-specific settings, e.g. AgentLoopConfig for `agent_loop`, sleep settings for `sleep` |
| SkipIf | `skip_if` | ParentCond | No | nil | Conditional skip based on parent output |
| Metadata | `metadata` | map | No | nil | Arbitrary key-value pairs |
| Retry | `retry` | RetryPolicy | No | nil | Structured retry policy (overrides `retries`) |
| WorkerGroup | `worker_group` | string | No | "" | Routes to a specific worker pool |
| OnFailure | `on_failure` | string | No | "" | Step ID to run on failure |
| Compensate | `compensate` | string | No | "" | Step ID for saga compensation |
| RateLimit | `rate_limit` | RateLimit | No | nil | `limit` dispatches per `period` for this step's task |
| KeyedRateLimit | `keyed_rate_limit` | KeyedRateLimit | No | nil | Rate limit per input key (`key`, `limit`, `period`, `units`) |
| MaxTaskConcurrency | `max_task_concurrency` | int | No | 0 | Max in-flight tasks of this step's task type; 0 = unlimited |
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

## RetryPolicy

Resolution order: step `retry` > workflow `default_retry` > legacy `retries` > none.

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| MaxAttempts | `max_attempts` | int | Yes | -- | Total retry attempts; 0 means no retries |
| Strategy | `strategy` | RetryStrategy | Yes | -- | Backoff algorithm |
| InitialDelay | `initial_delay` | duration | Yes | -- | Base delay between attempts |
| MaxDelay | `max_delay` | duration | Yes | -- | Delay cap (0 = uncapped) |
| Multiplier | `multiplier` | float64 | No | 0 | Used by `exponential` strategy only |

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
| 8 | `retries` must not be negative |
| 9 | `agent_loop` steps must have a `config` with `max_iterations > 0` |
| 10 | Each typed step's `config` must parse for its type (sleep, map, approval, respond, ...) |
| 11 | `skip_if.step_id` must be in the step's `depends_on` list |
| 12 | `skip_if.op` must be a valid operator |
| 13 | The step dependency graph must be acyclic (Kahn's algorithm) |
| 14 | `priority`, `cancel_on` (max 5), `sticky` and `singleton` are checked when set |

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
      "type": "normal",
      "depends_on": ["fetch-diff"]
    }
  ]
}
```
