---
title: Sticky Assignment
weight: 2
---

Sticky assignment routes all steps of a workflow run to the same worker, enabling cache affinity, GPU pinning, and session-local state.

## StickyStrategy

The `StickyStrategy` type is a string enum on `WorkflowDef` that controls worker affinity behavior:

| Strategy | Value | Behavior |
|----------|-------|----------|
| **None** | `""` | Default. Tasks are distributed across all workers via the TASK_QUEUES stream. |
| **Soft** | `"soft"` | The engine tries the bound worker first. If that worker has not claimed the task after 5 seconds, the engine republishes it to the normal queue. |
| **Hard** | `"hard"` | The engine only routes to the bound worker. If that worker is down, the task waits for it. |

```go
wf := dag.NewWorkflow("gpu-pipeline").WithSticky(dag.StickySoft)
```

In workflow JSON it is `"sticky": "soft"` on the workflow. `Build()` and `workflow validate` reject two combinations: `hard` without a workflow `timeout` (the wait has no other bound), and any sticky workflow with a step that sets `worker_group`.

## How It Works

A sticky workflow's first step goes through the normal queue, so any worker can take it. When that step completes, the engine records the worker that ran it in the **sticky_bindings** KV bucket (key: `{runID}`, value: `{workerID}`). Subsequent steps are published to the `STICKY_TASKS` stream on subject `sticky.{taskType}.{workerID}.{runID}`, where only that worker is listening. The engine deletes the binding when the run completes, fails or is cancelled.

A worker subscribes to its sticky subject for each task type it handles:

```
sticky.{taskType}.{workerID}.>
```

A worker built with `WithGroups` or `WithPartitions` does not subscribe to sticky subjects, so it can start a sticky run but never receives the later steps. Use sticky workflows with plain workers.

The `STICKY_TASKS` stream uses **memory storage** with a 30-minute MaxAge, so a sticky task that nobody claims within 30 minutes is dropped from the stream. The `sticky_bindings` KV bucket has a 25-hour TTL as a backstop for bindings the engine did not delete.

## Use Cases

**Cache affinity** -- An LLM embedding pipeline loads a large model into memory. Sticky soft routing keeps all steps on the same worker to reuse the loaded model, but allows failover if the worker crashes.

**GPU pinning** -- A video processing pipeline needs a specific GPU. Sticky hard routing guarantees all steps run on the same machine, even if it means waiting for the worker to recover.

**Session state** -- A multi-step conversation agent accumulates context in memory. Sticky routing avoids serializing and deserializing the full context on every step.

## Soft vs. Hard

Choose **soft** for performance optimization where correctness does not depend on worker identity. The engine tries the sticky worker first, and after 5 seconds without a claim it republishes the task to the normal TASK_QUEUES stream.

Choose **hard** only when correctness requires the same worker -- for example, when local hardware resources are involved. Hard sticky tasks never fall back to another worker, which is why the workflow must set a `timeout` to bound the wait.

## Infrastructure Requirements

Sticky routing uses two NATS resources: the **STICKY_TASKS** stream and the **sticky_bindings** KV bucket. `dagnats serve` creates both at startup. If the STICKY_TASKS stream does not exist, a worker silently skips the sticky subscription, and if the bucket is missing the engine records no bindings.

## Related

- [Worker Configuration](/docs/workers/worker-configuration) -- setting up workers
- [Flow Control](/docs/flow-control/rate-limiting) -- controlling task throughput
