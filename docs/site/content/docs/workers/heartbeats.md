---
title: Heartbeats
weight: 3
---

A Go worker heartbeats automatically: while a handler runs, it extends the NATS AckWait timer on the task message so the task is not redelivered and run a second time. `Heartbeat()` sends one more extension by hand. Neither extends the step's timeout.

## The Problem

JetStream delivers each task message with an **AckWait** deadline. If the worker does not acknowledge the message before the deadline expires, NATS assumes the worker failed and redelivers to another consumer. For tasks that take longer than AckWait (default 5 minutes; override per handler with `worker.WithAckWait`), this would cause duplicate execution.

## Automatic Heartbeat

While any handler runs, the worker calls `msg.InProgress()` every half AckWait, using the AckWait of the consumer that delivered the task (including a `WithAckWait` override). The ticker stops when the handler returns, before the worker acks or NAKs the message, and `Pause` stops it before its delayed NAK, so the pause keeps its full delay.

Because the worker keeps the message alive for as long as the handler runs, AckWait no longer bounds a hung handler. The step's timeout does: the engine fails the attempt when it passes (see [Timeouts](/docs/reliability/timeouts)). Give a step that may hang a `Timeout`.

If a redelivery of a task arrives while the same task is still running in this worker (a `WithConcurrency` lane can receive one), the worker skips it without acking or NAKing it. The running handler's result settles the message.

## Manual Heartbeat

`Heartbeat()` still works and is never needed to prevent redelivery. It returns `worker.ErrRunCancelled` once the step's run is cancelled, so a long loop that calls it learns about a cancel promptly:

```go
w.Handle("long-task", func(ctx worker.TaskContext) error {
    for i := 0; i < steps; i++ {
        doWork(i)
        if err := ctx.Heartbeat(); errors.Is(err, worker.ErrRunCancelled) {
            return err
        }
    }
    return ctx.Complete(result)
})
```

## What Happens When Heartbeats Stop

If a worker stops sending heartbeats (crash, network partition), the sequence is:

1. **AckWait expires** -- NATS marks the message as unacknowledged
2. **Redelivery** -- NATS delivers the message to another available consumer
3. **Same attempt** -- a redelivery is not an engine retry, so `ctx.RetryCount()` does not change. It is 0 on a step's first dispatch and 2, 3, ... on retries the engine schedules.
4. **Step timeout still applies** -- redelivery does not restart the step's timeout, which runs from when the attempt first started. If the step is still unfinished when it passes, the engine fails the attempt and the [retry policy](/docs/reliability/retry-policies) decides what happens next (see [Timeouts](/docs/reliability/timeouts))

The worker that crashed does not need to do anything. JetStream's redelivery mechanism handles the failover automatically.

## Overriding AckWait

`worker.WithAckWait` is a handler option that sets AckWait for one task type. It must be positive, and it applies to the consumer for that task type:

```go
w.Handle("render-video", renderHandler, worker.WithAckWait(30*time.Minute))
```

Use a short AckWait so a crashed worker's task comes back quickly; the automatic heartbeat keeps long tasks alive whatever the AckWait. `HandleTyped` takes no handler options, so register a handler that needs one with `Handle(taskType, worker.Typed(fn), opts...)`.

## Pause

`ctx.Pause(name, duration)` is the other way to hold a task for a long time. It saves a checkpoint, then NAKs the message with a delay, so the handler runs again after `duration` and finds the pause marker with `LoadCheckpoint()`. It needs the `checkpoints` bucket. The step stays `running`, and paused time counts toward the step's timeout, so a step that pauses repeatedly needs a `Timeout` that covers its whole run.

## Panics

`Heartbeat()` panics if the underlying message is nil, which means the TaskContext was not built by a worker. It returns the error from the NATS call, which a handler may ignore.

## Related

- [Worker Configuration](/docs/workers/worker-configuration) -- setting up workers
- [Checkpoints](/docs/coordination/checkpoints) -- saving state across retries
- [Retry Policies](/docs/reliability/retry-policies) -- controlling retry behavior
