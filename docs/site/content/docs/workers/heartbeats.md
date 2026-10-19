---
title: Heartbeats
weight: 3
---

`Heartbeat()` extends the NATS AckWait timer on a task message to prevent redelivery while long-running work is in progress. It does not extend the step's timeout.

## The Problem

JetStream delivers each task message with an **AckWait** deadline. If the worker does not acknowledge the message before the deadline expires, NATS assumes the worker failed and redelivers to another consumer. For tasks that take longer than AckWait (default 5 minutes; override per handler with `worker.WithAckWait`), this causes duplicate execution.

## The Solution

Calling `Heartbeat()` on the `TaskContext` signals to NATS that the worker is still alive and processing. Internally, it calls `msg.InProgress()` on the underlying JetStream message, which resets the AckWait timer.

```go
w.Handle("long-task", func(ctx worker.TaskContext) error {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for i := 0; i < steps; i++ {
        doWork(i)
        select {
        case <-ticker.C:
            ctx.Heartbeat()
        default:
        }
    }
    return ctx.Complete(result)
})
```

## Recommended Interval

Send heartbeats at **one-third of the AckWait duration**. With the default 5-minute AckWait, heartbeat every 100 seconds or so. This provides two missed heartbeats as buffer before redelivery.

| AckWait | Heartbeat Interval | Missed Heartbeats Before Redeliver |
|---------|-------------------|------------------------------------|
| 30s | 10s | 2 |
| 60s | 20s | 2 |
| 5m (default) | 100s | 2 |

## What Happens When Heartbeats Stop

If a worker stops sending heartbeats (crash, network partition, infinite loop without heartbeat calls), the sequence is:

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

Use a short AckWait for sub-second tasks, so a crashed worker's task comes back quickly, and a longer one for tasks that cannot heartbeat. `HandleTyped` takes no handler options, so register a handler that needs one with `Handle(taskType, worker.Typed(fn), opts...)`.

## Pause

`ctx.Pause(name, duration)` is the other way to hold a task for a long time. It saves a checkpoint, then NAKs the message with a delay, so the handler runs again after `duration` and finds the pause marker with `LoadCheckpoint()`. It needs the `checkpoints` bucket. The step stays `running`, and paused time counts toward the step's timeout, so a step that pauses repeatedly needs a `Timeout` that covers its whole run.

## Streaming with Heartbeats

When using [PutStream](/docs/coordination/streaming) for real-time output, interleave heartbeats with stream publishes:

```go
w.Handle("generate", func(ctx worker.TaskContext) error {
    for i, chunk := range generate(ctx.Input()) {
        ctx.PutStream(chunk)
        if i%50 == 0 {
            ctx.Heartbeat()
        }
    }
    return ctx.Complete(assembleResult())
})
```

## Panics

`Heartbeat()` panics if the underlying message is nil, which means the TaskContext was not built by a worker. It returns the error from the NATS call, which a handler may ignore.

## Related

- [Worker Configuration](/docs/workers/worker-configuration) -- setting up workers
- [Streaming](/docs/coordination/streaming) -- real-time output with heartbeats
- [Checkpoints](/docs/coordination/checkpoints) -- saving state across retries
- [Retry Policies](/docs/reliability/retry-policies) -- controlling retry behavior
