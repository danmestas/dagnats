---
title: dag
weight: 1
---

```
import "github.com/danmestas/dagnats/dag"
```

Pure DAG logic with zero NATS dependencies. This package defines the workflow data model, the builder, and the validation rules. The engine that advances runs lives in an internal package.

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
| `ResolveReady(def, completed, queued)` | Returns the steps whose dependencies are all complete |
| `ResolveRetryPolicy(def, step)` | Resolves the effective retry policy for a step (step > workflow default > legacy) |
| `ValidateSchema(schema, data)` | Validates JSON data against a JSON Schema subset: `type`, `required`, `properties` |
| `WithSchemas[I, O](def)` | Fills `InputSchema` and `OutputSchema` from Go types |
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
| `wait_for_event` | `StepTypeWaitForEvent` | Wait for a matching event |
| `approval` | `StepTypeApproval` | Human-in-the-loop gate |
| `planner` | `StepTypePlanner` | Dynamic step generation |
| `respond` | `StepTypeRespond` | Publish the HTTP response for an HTTP trigger |

An unknown `type` string is an error (`unknown step type`). In JSON, durations accept strings such as `"30s"` or integer nanoseconds, and a top-level `$schema` key is accepted and ignored.

## Run Status

| Status | Terminal | Description |
|--------|----------|-------------|
| `pending` | No | Created, not yet started |
| `running` | No | At least one step is executing |
| `completed` | Yes | All steps completed successfully |
| `failed` | Yes | One or more steps failed |
| `cancelled` | Yes | Cancelled by user or system |
| `compensated` | Yes | Failed, and saga compensation finished |
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
