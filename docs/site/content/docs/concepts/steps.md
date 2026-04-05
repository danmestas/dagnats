---
title: Steps
weight: 2
---

A **step** is the smallest unit of work in a DagNats workflow -- one task dispatched to one worker (or handled by the engine itself for infrastructure steps).

## Step Types

DagNats supports 9 step types. Each has distinct execution semantics:

| Type | Purpose | When to use |
|------|---------|-------------|
| `normal` | Execute a task once, complete or fail | Standard compute, API calls, data transforms |
| `agent_loop` | Iterative execution with `Continue()`, bounded by max iterations/duration | LLM agent loops, polling, convergence tasks |
| `agent` | Routed to agent SDK via metadata | Claude Agent SDK integration |
| `sub_workflow` | Spawn a child workflow, wait for completion | Reusable workflow composition, nested pipelines |
| `map` | Fan-out over an array (one task per item), fan-in on completion | Parallel batch processing, list transforms |
| `sleep` | Durable delay handled by the engine | Rate limiting, scheduled delays, cooldowns |
| `wait_for_event` | Block until an external event matches a condition | Webhooks, human triggers, cross-system coordination |
| `approval` | Human approval gate with cryptographic token | Deploy approvals, sensitive operations |
| `planner` | Generate DAG fragments at runtime | Dynamic pipelines, AI-planned workflows |

Steps that require a **worker** to execute: `normal`, `agent_loop`, `agent`, `map`, `planner`. Steps handled entirely by the **engine**: `sleep`, `wait_for_event`, `approval`, `sub_workflow`.

## StepDef Fields

Every step is declared as a `StepDef` in the workflow definition:

| Field | Type | Purpose |
|-------|------|---------|
| `ID` | `string` | Unique identifier within the workflow |
| `Task` | `string` | Task type that workers register for |
| `DependsOn` | `[]string` | Step IDs that must complete first |
| `Type` | `StepType` | One of the 9 types above |
| `Timeout` | `time.Duration` | Per-attempt timeout |
| `Retries` | `int` | Max retry attempts (0 = no retries) |
| `Retry` | `*RetryPolicy` | Fine-grained retry config (backoff, etc.) |
| `Config` | `json.RawMessage` | Type-specific configuration (AgentLoopConfig, MapConfig, etc.) |
| `SkipIf` | `*ParentCond` | Conditional skip based on parent output |
| `Metadata` | `map[string]string` | Opaque key-value pairs (used by agent steps) |
| `WorkerGroup` | `string` | Route to specific worker group |
| `OnFailure` | `string` | Step ID to run on permanent failure |
| `Compensate` | `string` | Step ID for saga compensation |
| `MaxTaskConcurrency` | `int` | Global per-task-type concurrency limit |

## Dependencies

Dependencies are declared via `DependsOn` -- a list of step IDs that must reach a terminal state before this step is queued. The engine resolves dependencies iteratively: when a step completes, it checks which downstream steps have all their dependencies satisfied and enqueues them.

**Skipped steps count as satisfied.** If a step is skipped via `SkipIf`, downstream steps that depend on it proceed normally. This lets you build conditional branches without blocking the rest of the graph.

The builder API provides two ways to wire dependencies:

```go
// String-based (backward compat)
wf.Task("process", "process")
wf.DependsOn("fetch")

// StepRef-based (compile-time safe, preferred)
fetch := wf.Task("fetch", "fetch")
process := wf.Task("process", "process").After(fetch)
```

`After()` panics immediately if you pass a `StepRef` from a different builder, catching miswiring at construction time rather than at validation time.

## Conditional Execution

Steps can be conditionally skipped based on a parent step's output using `SkipIf`. The condition references a parent step (which must be in `DependsOn`) and evaluates an operator against a field in that step's output.

```go
fetch := wf.Task("fetch", "fetch")
process := wf.Task("process", "process").
    After(fetch).
    SkipIf(dag.ParentOutput("fetch", "skip", dag.OpEquals, true))
```

## Related pages

- [Workflows and DAGs](/docs/concepts/workflows-and-dags) -- how steps compose into workflows
- [Workers](/docs/concepts/workers) -- how steps are executed
- [Step Types](/docs/step-types/) -- detailed reference for each step type
