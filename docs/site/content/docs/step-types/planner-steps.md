---
title: Planner Steps
weight: 8
---

A **planner step** generates a DAG fragment at runtime, enabling workflows to create steps dynamically based on data or LLM output.

## Overview

Static DAGs require you to know every step at definition time. Planner steps remove that constraint. A `StepTypePlanner` step is a normal worker task whose output is not data -- it is a JSON DAG fragment containing steps and edges. The engine validates the fragment, namespaces the generated step IDs, and materializes them into the running workflow as `DynamicSteps`. From that point, the generated steps execute exactly like static ones.

This is the primitive that makes AI-planned workflows possible. An LLM can analyze a task, decide what subtasks are needed, emit a plan as structured JSON, and the engine executes it. The planner does not need to know the full workflow graph -- it only produces its fragment, and the engine merges it with the static definition.

Planner steps are bounded to prevent runaway generation: a single planner can emit at most **100 steps** (and no more than its `max_steps`), a single run can accumulate at most **500 dynamic steps** total, and the dependency chain depth within a fragment is capped at **10**. Generated step IDs are namespaced as `{plannerStepID}.{generatedID}` to prevent collisions with static steps or other planner fragments.

## How It Works

```mermaid
sequenceDiagram
    participant Worker as Planner Worker
    participant Engine
    participant Run as WorkflowRun

    Engine->>Worker: dispatch planner task
    Worker->>Engine: step.completed (JSON fragment)
    Engine->>Engine: validate fragment (bounds, cycles, IDs)
    Engine->>Run: append to DynamicSteps
    Note over Run: EffectiveSteps() merges static + dynamic
    Engine->>Engine: enqueue ready generated steps
```

The engine performs several validation checks on the fragment before materialization:

1. **Bounds check**: step count within [1, `MaxSteps`], and the longest chain no deeper than `MaxDepth` when `MaxDepth` is set
2. **Cycle detection**: the generated fragment must be a DAG
3. **ID collision check**: namespaced IDs must not collide with existing steps
4. **Task allowlist**: if `AllowedTasks` is configured, generated steps may only reference listed task types

After validation, the engine appends the fragment to `WorkflowRun.DynamicSteps` and publishes an `EventPlannerMaterialized` event for observability. The `EffectiveSteps()` method on the workflow run merges static steps from the definition with all dynamic steps, producing the complete step graph that the DAG resolver operates on.

**Outputs**: the planner step is marked completed as soon as the worker returns, and its output is the fragment JSON the worker returned. The engine does not collect the generated steps' outputs back into it. Each generated step keeps its own output under its namespaced ID, and static steps that depend on the planner step become ready when the planner step completes. If the fragment fails validation, or would take the run past 500 dynamic steps, the planner step fails.

## Usage

```go
wf := dag.NewWorkflow("ai-pipeline")

analyze := wf.Task("analyze", "analyze-task").
    WithTimeout(30 * time.Second)

plan := wf.Planner("plan", "generate-plan", dag.PlannerConfig{
    MaxSteps:     20,
    MaxDepth:     5,
    AllowedTasks: []string{"code-edit", "test-run", "lint"},
}).After(analyze)

report := wf.Task("report", "summarize").
    After(plan)

def, err := wf.Build()
```

The planner worker returns a JSON object with a `steps` array. Each generated step needs an `id` and a `task` (a valid task type), and `depends_on` may name only other steps in the same fragment. Generated steps always run as normal steps: whatever `type` the fragment names, the engine sets it to `normal`. A fragment with no steps is allowed and adds nothing. Steps with no `depends_on` start after the planner step.

```go
w.Handle("generate-plan", func(ctx worker.TaskContext) error {
    plan := map[string]any{
        "steps": []map[string]any{
            {"id": "edit", "task": "code-edit"},
            {"id": "test", "task": "test-run",
                "depends_on": []string{"edit"}},
            {"id": "lint", "task": "lint",
                "depends_on": []string{"edit"}},
        },
    }
    output, _ := json.Marshal(plan)
    return ctx.Complete(output)
})
```

## Configuration

Planner configuration is stored in `StepDef.Config` as `PlannerConfig`:

| Field | Type | Default | Purpose |
|-------|------|---------|---------|
| `max_steps` | `int` | (required) | Maximum steps in the fragment. Range: 1-100. |
| `max_depth` | `int` | 0 (no depth check) | Maximum length of the longest dependency chain in the fragment, counted in steps (a single step has depth 1). Range: 0-10. |
| `allowed_tasks` | `[]string` | (all allowed) | Restrict which task types may appear in the fragment |

**Global bounds:**

| Limit | Value |
|-------|-------|
| Steps per planner | 100 |
| Total dynamic steps per run | 500 |
| Max fragment depth | 10 |
| ID namespace | `{plannerStepID}.{generatedID}` |

## Related

- [Sub-Workflows](/docs/step-types/sub-workflows) -- static workflow composition
- [Map Steps](/docs/step-types/map-steps) -- parallel execution over arrays
- [Normal Steps](/docs/step-types/normal-steps) -- the step type that generated steps become
