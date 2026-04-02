# Workflow Definition Schema

Reference for the JSON format used by `dagnats workflow register` and
`dagnats workflow validate`.

Durations are Go `time.Duration` strings: `"30s"`, `"5m"`, `"1h30m"`.

## WorkflowDef

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| Name | `name` | string | yes | — | Unique workflow identifier |
| Version | `version` | string | yes | — | Schema version for evolution |
| Steps | `steps` | []StepDef | yes | — | At least one step required |
| DefaultRetry | `default_retry` | RetryPolicy | no | nil | Fallback retry for all steps |
| Concurrency | `concurrency` | ConcurrencyLimit | no | nil | Workflow-level parallelism limits |
| Timeout | `timeout` | duration | no | 0 | Overall workflow deadline |
| InputSchema | `input_schema` | JSON | no | nil | JSON Schema for workflow input validation |
| OutputSchema | `output_schema` | JSON | no | nil | JSON Schema for workflow output validation |

## StepDef

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| ID | `id` | string | yes | — | Unique within the workflow |
| Task | `task` | string | yes | — | Task name workers subscribe to |
| DependsOn | `depends_on` | []string | no | [] | Step IDs that must complete first |
| Retries | `retries` | int | no | 0 | Legacy retry count (prefer `retry`) |
| Timeout | `timeout` | duration | yes | — | Per-step execution deadline |
| Type | `type` | StepType | yes | — | Execution semantics (see below) |
| Loop | `loop` | AgentLoopConfig | no | nil | Required when type is `agent_loop` |
| SkipIf | `skip_if` | ParentCond | no | nil | Conditional skip based on parent output |
| Metadata | `metadata` | map[string]string | no | nil | Arbitrary key-value pairs |
| Retry | `retry` | RetryPolicy | no | nil | Structured retry policy (overrides `retries`) |
| WorkerGroup | `worker_group` | string | no | "" | Routes to a specific worker pool |
| OnFailure | `on_failure` | string | no | "" | Step ID to run on failure |
| Compensate | `compensate` | string | no | "" | Step ID for saga compensation |

## RetryPolicy

Resolution order: step `retry` > workflow `default_retry` > legacy `retries` > none.

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| MaxAttempts | `max_attempts` | int | yes | — | Total retry attempts; 0 means no retries |
| Strategy | `strategy` | RetryStrategy | yes | — | Backoff algorithm |
| InitialDelay | `initial_delay` | duration | yes | — | Base delay between attempts |
| MaxDelay | `max_delay` | duration | yes | — | Delay cap (0 = uncapped) |
| Multiplier | `multiplier` | float64 | no | 0 | Used by `exponential` strategy only |

### RetryStrategy Values

| Value | Delay Formula |
|-------|---------------|
| `fixed` | `initial_delay` every attempt |
| `linear` | `initial_delay * attempt` |
| `exponential` | `initial_delay * multiplier^(attempt-1)` |

## AgentLoopConfig

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| MaxIterations | `max_iterations` | int | yes | — | Upper bound on loop cycles; must be > 0 |
| MaxDuration | `max_duration` | duration | no | 0 | Wall-clock limit; whichever fires first wins |
| LoopDelay | `loop_delay` | duration | no | 0 | Pause between iterations |

## ConcurrencyLimit

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| MaxRuns | `max_runs` | int | no | 0 | Max parallel runs of this workflow |
| MaxSteps | `max_steps` | int | no | 0 | Max parallel steps within a run |

## ParentCond (skip_if)

Evaluates a comparison against a parent step's JSON output.

| Field | JSON Key | Type | Required | Default | Description |
|-------|----------|------|----------|---------|-------------|
| StepID | `step_id` | string | yes | — | Parent step to check (must be in `depends_on`) |
| Field | `field` | string | yes | — | Top-level key in parent's output JSON |
| Op | `op` | string | yes | — | Comparison operator |
| Value | `value` | any | yes | — | Value to compare against |

Valid operators: `==`, `!=`, `<`, `>`, `<=`, `>=`.
Types compared as float64 (numbers), string, or bool (equality only).

## StepType Values

| Value | Description |
|-------|-------------|
| `normal` | Runs once, completes or fails |
| `agent_loop` | Iterates until termination signal; requires `loop` config |
| `sub_workflow` | Delegates to a nested workflow DAG |
| `agent` | Single autonomous agent execution |

## Validation Rules

1. Workflow must have a non-empty `name`.
2. Workflow must contain at least one step.
3. Every step must have a unique `id`.
4. Every step must have a non-empty `task`.
5. `depends_on` entries must reference existing step IDs.
6. `on_failure` must reference an existing step ID (if set).
7. `compensate` must reference an existing step ID (if set).
8. `retries` must not be negative.
9. `agent_loop` steps must have a `loop` config with `max_iterations > 0`.
10. Non-`agent_loop` steps must not have a `loop` config.
11. `skip_if.step_id` must be in the step's `depends_on` list.
12. `skip_if.op` must be a valid operator (`==`, `!=`, `<`, `>`, `<=`, `>=`).
13. The step dependency graph must be acyclic (Kahn's algorithm).

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
      "loop": {
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
