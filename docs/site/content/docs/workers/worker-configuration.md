---
title: Worker Configuration
weight: 1
---

`worker.NewWorker()` is the single entry point for creating a task processor that subscribes to NATS JetStream and dispatches messages to registered handlers.

## Constructor

```go
nc, err := nats.Connect(nats.DefaultURL)
if err != nil {
    log.Fatal(err)
}
defer nc.Close()

w := worker.NewWorker(nc,
    worker.WithGroups("gpu", "cpu"),
    worker.WithPartitions(8),
)
w.Handle("resize-image", resizeHandler)
w.HandleSingleton("billing-sync", billingHandler)
w.Start()
defer w.Stop()
```

The constructor panics if `nc` is nil or JetStream cannot be initialized -- both are startup-time programmer errors. It takes no telemetry argument. Tracing and metrics use the global OpenTelemetry providers, which are no-ops until you install real ones (see [observe](/docs/reference/sdk/observe)). The constructor installs a W3C trace-context propagator only if the global one is still the default.

## Options

| Option | Description | Default |
|--------|-------------|---------|
| `WithGroups(groups...)` | Subscribe only to specific **worker groups**. The worker takes only the tasks routed to those groups, and none of the ungrouped ones. | All ungrouped tasks |
| `WithPartitions(n)` | Enable **elastic consumer groups** with `n` partitions (1--256). Each partition gets its own JetStream consumer for parallel processing. | 0 (legacy single consumer) |
| `WithControlPlane(cp)` | Grant the worker a control-plane handle for steps that declare the `control-plane` capability. Without it, `ctx.ControlPlane()` is always nil. Build one with `worker.NewControlPlane(nc)`. | None |

### WithGroups

Groups route tasks to a subset of workers. When a step definition sets `WorkerGroup: "gpu"`, only workers created with `WithGroups("gpu")` receive those tasks. A group name follows the task-type rules (letters, digits, `_` and `-`) except that dots are not allowed.

```go
w := worker.NewWorker(nc, worker.WithGroups("gpu"))
w.Handle("inference", inferenceHandler)
w.Start()
```

Panics if called with zero groups or any empty group name -- both are programmer errors that should fail at startup.

### WithPartitions

Partitions enable the [pcgroups](https://github.com/synadia-io/orbit.go) elastic consumer group pattern. Workers automatically join and leave the group; partitions are rebalanced across all active members.

```go
w := worker.NewWorker(nc, worker.WithPartitions(16))
w.Handle("process", handler)
w.Start()
```

The partition count must be between 0 and 256. A larger value panics at construction time.

## Singleton Handlers

`HandleSingleton` registers a handler that runs as a **single-partition** elastic consumer group. Only one worker instance processes messages for that task type at any given time, across the entire cluster.

```go
w.HandleSingleton("cron-cleanup", cleanupHandler)
```

Internally, `HandleSingleton` marks the task type as a singleton and enables partitioned mode with one partition if `WithPartitions` was not called. It takes no handler options.

## Handler Registration

`Handle` maps a task type string to a `HandlerFunc`. The handler receives a `TaskContext` with methods for input access, completion, streaming, checkpointing, and signals. See [Heartbeats](/docs/workers/heartbeats) for the long-running ones.

```go
w.Handle("send-email", func(ctx worker.TaskContext) error {
    input := ctx.Input()
    // ... process
    _ = input
    return ctx.Complete([]byte(`{"sent": true}`))
})
```

`Handle` takes optional handler options after the function. The only one today is `worker.WithAckWait(d)`, which sets the JetStream AckWait for that task type.

For handlers with typed input and output, `worker.HandleTyped` decodes the input into `I` and encodes the returned `O` as the step output:

```go
type resizeInput struct {
    URL   string `json:"url"`
    Width int    `json:"width"`
}

type resizeOutput struct {
    Path string `json:"path"`
}

worker.HandleTyped(w, "resize-image",
    func(ctx worker.TaskContext, in resizeInput) (resizeOutput, error) {
        return resizeOutput{Path: "/tmp/out.png"}, nil
    },
)
```

A decode or encode failure is a non-retryable failure. `worker.UnwrapTrigger()` is an option for handlers that run behind a trigger; see [HTTP Trigger](/docs/triggers/http).

### TaskContext methods

| Method | Purpose |
|--------|---------|
| `Input()`, `RunID()`, `StepID()`, `RetryCount()`, `Metadata()`, `Context()` | Read the task. `RetryCount()` is 0 on the first dispatch and 2, 3, ... on engine retries. |
| `Complete(out)` | Finish the step with output |
| `Fail(err)` | Fail the attempt; the retry policy decides what happens next |
| `FailPermanent(err)` | Fail without retrying |
| `FailRetryAfter(err, d)` | Fail and retry after `d` |
| `Continue(out)` | Request another iteration of an [agent loop](/docs/step-types/agent-loops) |
| `Heartbeat()` | Extend the claim; see [Heartbeats](/docs/workers/heartbeats) |
| `Pause(name, d)` | Hold the task and run it again after `d` |
| `Checkpoint(b)`, `LoadCheckpoint()` | Save and read state across retries and pauses |
| `PutStream(b)` | Publish a live token to `stream.{run_id}.{step_id}` |
| `LogOut()`, `LogErr()` | `io.Writer`s that stream stdout and stderr chunks into the run's logs |
| `WaitForSignal(name, timeout)`, `SendSignal(runID, name, data)` | Coordinate between steps and runs |
| `ControlPlane()` | The control-plane handle, or nil |

Call exactly one of `Complete`, `Fail`, `FailPermanent`, `FailRetryAfter`, or `Continue` per execution. Returning a non-nil error from the handler reports a retriable failure, so the step's retry policy decides whether and when to retry. Wrap the error with `worker.NewNonRetryableError(err)` to fail permanently, or with `worker.NewRateLimitError(err, d)` to retry after `d`.

## Lifecycle

1. **NewWorker** -- allocates the worker, applies options, creates metric instruments
2. **Handle / HandleSingleton** -- registers task handlers (must be called before Start)
3. **Start** -- creates JetStream consumers, binds optional KV buckets, registers in the worker directory
4. **Stop** -- stops the consumers, deregisters from the directory, then waits up to 30 seconds for handlers that are still running

Start panics if no handlers are registered. Stop is safe to call after Start and cleans up all resources including the directory heartbeat goroutine. A handler that outlives the 30-second wait is logged and abandoned, so Stop always returns.

## Optional KV Buckets

Start binds optional KV buckets if they exist:

- **checkpoints** -- enables `Checkpoint()`, `LoadCheckpoint()` and `Pause()` on TaskContext
- **signals** -- enables `WaitForSignal()` and `SendSignal()` on TaskContext
- **workflow_runs** -- lets the worker skip tasks whose run was already cancelled
- **workers** -- the worker directory shown by `dagnats workers`

If the checkpoints or signals bucket is missing, the corresponding methods return an error and the worker logs a warning. `dagnats serve` creates all of them at startup, so a worker pointed at a running server has them.

## Related

- [Heartbeats](/docs/workers/heartbeats) -- extending AckWait for long-running tasks
- [Sticky Assignment](/docs/workers/sticky-assignment) -- worker affinity routing
- [Embedded Workers](/docs/workers/embedded-workers) -- running workers in-process
- [Rate Limiting](/docs/flow-control/rate-limiting) -- per-key and global rate limits
