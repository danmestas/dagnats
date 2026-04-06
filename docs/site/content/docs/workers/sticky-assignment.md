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
| **Soft** | `"soft"` | The engine attempts to route to the same worker. Falls back to any worker if the bound worker is unavailable. |
| **Hard** | `"hard"` | The engine always routes to the bound worker. If that worker is down, the task waits until it returns. |

```go
def := dag.WorkflowDef{
    Name:   "gpu-pipeline",
    Sticky: dag.StickySoft,
    Steps:  []dag.StepDef{...},
}
```

## How It Works

When a sticky workflow starts, the engine records the first worker that completes a step in the **sticky_bindings** KV bucket (key: `{runID}`, value: `{workerID}`). Subsequent steps are published to the `STICKY_TASKS` stream on subject `sticky.{taskType}.{workerID}.{runID}`, where only that specific worker is listening.

Workers automatically subscribe to their sticky subject on startup:

```
sticky.{taskType}.{workerID}.>
```

The `STICKY_TASKS` stream uses **memory storage** with a 30-minute MaxAge. The `sticky_bindings` KV bucket has a 25-hour TTL, ensuring bindings expire after the workflow completes.

## Use Cases

**Cache affinity** -- An LLM embedding pipeline loads a large model into memory. Sticky soft routing keeps all steps on the same worker to reuse the loaded model, but allows failover if the worker crashes.

**GPU pinning** -- A video processing pipeline needs a specific GPU. Sticky hard routing guarantees all steps run on the same machine, even if it means waiting for the worker to recover.

**Session state** -- A multi-step conversation agent accumulates context in memory. Sticky routing avoids serializing and deserializing the full context on every step.

## Soft vs. Hard

Choose **soft** for performance optimization where correctness does not depend on worker identity. The engine tries the sticky worker first but falls back to the normal TASK_QUEUES stream if delivery fails.

Choose **hard** only when correctness requires the same worker -- for example, when local hardware resources are involved. Hard sticky tasks wait indefinitely for the bound worker to return, so pair with a workflow-level timeout to bound the wait.

## Infrastructure Requirements

Sticky routing requires two NATS resources beyond the defaults:

1. **STICKY_TASKS stream** -- provisioned by `natsutil.SetupStickyStream()`
2. **sticky_bindings KV bucket** -- provisioned by `natsutil.SetupKVBuckets()`

Both are created automatically by `natsutil.SetupAll()`. If the STICKY_TASKS stream does not exist, the worker silently skips sticky subscription -- sticky features degrade gracefully.

## Related

- [Worker Configuration](/docs/workers/worker-configuration) -- setting up workers
- [Flow Control](/docs/flow-control/rate-limiting) -- controlling task throughput
