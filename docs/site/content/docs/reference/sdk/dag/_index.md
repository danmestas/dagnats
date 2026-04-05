---
title: dag
weight: 1
---

```
import "github.com/danmestas/dagnats/dag"
```

Pure DAG logic with zero NATS dependencies. This package defines the workflow data model, validation rules, and state machine for advancing runs through their steps.

## Key Types

| Type | Description |
|------|-------------|
| `WorkflowDef` | Complete workflow definition: name, version, steps, retry policy, concurrency limits, timeout, input/output schemas |
| `StepDef` | Individual step within a workflow: task type, dependencies, retry, timeout, conditional skip |
| `WorkflowRun` | Runtime snapshot of an executing workflow: status, per-step state, timestamps |
| `StepState` | Per-step execution state: status, attempts, iterations, output, error |
| `RetryPolicy` | Structured retry configuration: strategy, delays, max attempts |
| `AgentLoopConfig` | Configuration for agent-loop steps: max iterations, duration, delay |
| `ConcurrencyLimit` | Workflow-level parallelism: max parallel runs and steps |
| `ParentCond` | Conditional skip predicate evaluated against parent step output |

## Key Functions

| Function | Description |
|----------|-------------|
| `Validate(def)` | Validates a workflow definition: unique IDs, valid references, acyclic graph (Kahn's algorithm) |
| `Advance(run, event)` | State machine: applies an event to a run snapshot and returns the next set of ready steps |
| `ResolveRetryPolicy(def, step)` | Resolves the effective retry policy for a step (step > workflow default > legacy) |
| `ValidateSchema(schema, data)` | Validates JSON data against a JSON Schema |
| `ExtractDotPath(path, data)` | Extracts a value from JSON data using dot-notation path |

## Step Types

The `StepType` enum controls execution semantics:

| Type | Constant | Description |
|------|----------|-------------|
| `normal` | `StepTypeNormal` | Runs once, completes or fails |
| `agent_loop` | `StepTypeAgentLoop` | Iterates until termination signal |
| `sub_workflow` | `StepTypeSubWorkflow` | Delegates to a nested workflow |
| `agent` | `StepTypeAgent` | Single autonomous agent execution |
| `map` | `StepTypeMap` | Fan-out over input collection |
| `sleep` | `StepTypeSleep` | Pause for a duration |
| `wait_for_event` | `StepTypeWaitForEvent` | Wait for an external signal |
| `approval` | `StepTypeApproval` | Human-in-the-loop gate |
| `planner` | `StepTypePlanner` | Dynamic step generation |

## Run Status

| Status | Terminal | Description |
|--------|----------|-------------|
| `pending` | No | Created, not yet started |
| `running` | No | At least one step is executing |
| `completed` | Yes | All steps completed successfully |
| `failed` | Yes | One or more steps failed |
| `cancelled` | Yes | Cancelled by user or system |
| `compensate_failed` | Yes | Saga compensation failed |

## Usage

```go
def := dag.WorkflowDef{
    Name:    "pipeline",
    Version: "1.0",
    Steps: []dag.StepDef{
        {ID: "fetch", Task: "fetch", Timeout: 2 * time.Minute, Type: dag.StepTypeNormal},
        {ID: "process", Task: "process", Timeout: 5 * time.Minute, Type: dag.StepTypeNormal, DependsOn: []string{"fetch"}},
    },
}
if err := dag.Validate(def); err != nil {
    log.Fatal(err)
}
```
