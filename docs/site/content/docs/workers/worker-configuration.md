---
title: Worker Configuration
weight: 1
---

`worker.NewWorker()` is the single entry point for creating a task processor that subscribes to NATS JetStream and dispatches messages to registered handlers.

## Constructor

```go
w := worker.NewWorker(nc, tel,
    worker.WithGroups("gpu", "cpu"),
    worker.WithPartitions(8),
)
w.Handle("resize-image", resizeHandler)
w.HandleSingleton("billing-sync", billingHandler)
w.Start()
defer w.Stop()
```

The constructor panics if `nc` is nil or JetStream cannot be initialized -- both are startup-time programmer errors. When `tel` is nil, a **noop telemetry** bundle is used so callers are not forced to import the `observe` package for simple use cases.

## Options

| Option | Description | Default |
|--------|-------------|---------|
| `WithGroups(groups...)` | Subscribe only to specific **worker groups**. The worker listens on `task.{type}.{group}.>` instead of `task.{type}.>`. | All groups |
| `WithPartitions(n)` | Enable **elastic consumer groups** with `n` partitions (1--256). Each partition gets its own JetStream consumer for parallel processing. | 0 (legacy single consumer) |

### WithGroups

Groups route tasks to a subset of workers. When a step definition sets `WorkerGroup: "gpu"`, only workers created with `WithGroups("gpu")` receive those tasks.

```go
w := worker.NewWorker(nc, tel, worker.WithGroups("gpu"))
w.Handle("inference", inferenceHandler)
w.Start()
```

Panics if called with zero groups or any empty group name -- both are programmer errors that should fail at startup.

### WithPartitions

Partitions enable the [pcgroups](https://github.com/synadia-io/orbit.go) elastic consumer group pattern. Workers automatically join and leave the group; partitions are rebalanced across all active members.

```go
w := worker.NewWorker(nc, tel, worker.WithPartitions(16))
w.Handle("process", handler)
w.Start()
```

The partition count is bounded to 256 maximum. Higher values panic at construction time.

## Singleton Handlers

`HandleSingleton` registers a handler that runs as a **single-partition** elastic consumer group. Only one worker instance processes messages for that task type at any given time, across the entire cluster.

```go
w.HandleSingleton("cron-cleanup", cleanupHandler)
```

Internally, `HandleSingleton` sets `partitions = 1` for the task type and implicitly enables partitioned mode if `WithPartitions` was not called.

## Handler Registration

`Handle` maps a task type string to a `HandlerFunc`. The handler receives a [TaskContext](/docs/workers/heartbeats) with methods for input access, completion, streaming, checkpointing, and signals.

```go
w.Handle("send-email", func(ctx worker.TaskContext) error {
    input := ctx.Input()
    // ... process
    return ctx.Complete([]byte(`{"sent": true}`))
})
```

Call exactly one of `Complete`, `Fail`, `FailPermanent`, `FailRetryAfter`, or `Continue` per execution. Returning a non-nil error from the handler triggers a retry via `NakWithDelay(5s)`.

## Lifecycle

1. **NewWorker** -- allocates the worker, applies options, creates metric instruments
2. **Handle / HandleSingleton** -- registers task handlers (must be called before Start)
3. **Start** -- creates JetStream consumers, binds optional KV buckets, registers in the worker directory
4. **Stop** -- unsubscribes all consumers, deregisters from the directory

Start panics if no handlers are registered. Stop is safe to call after Start and cleans up all resources including the directory heartbeat goroutine.

## Optional KV Buckets

Start binds two optional KV buckets if they exist:

- **checkpoints** -- enables `Checkpoint()` and `LoadCheckpoint()` on TaskContext
- **signals** -- enables `WaitForSignal()` and `SendSignal()` on TaskContext

If either bucket is missing, the corresponding methods return an error. Provision them via `natsutil.SetupAll()` or `natsutil.SetupKVBuckets()`.

## Related

- [Heartbeats](/docs/workers/heartbeats) -- extending AckWait for long-running tasks
- [Sticky Assignment](/docs/workers/sticky-assignment) -- worker affinity routing
- [Embedded Workers](/docs/workers/embedded-workers) -- running workers in-process
- [Rate Limiting](/docs/flow-control/rate-limiting) -- per-key and global rate limits
