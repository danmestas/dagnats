---
title: Heartbeats
weight: 3
---

`Heartbeat()` extends the NATS AckWait timer on a task message to prevent redelivery while long-running work is in progress.

## The Problem

JetStream delivers each task message with an **AckWait** deadline. If the worker does not acknowledge the message before the deadline expires, NATS assumes the worker failed and redelivers to another consumer. For tasks that take longer than AckWait (default 30 seconds), this causes duplicate execution.

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

Send heartbeats at **one-third of the AckWait duration**. With the default 30-second AckWait, heartbeat every 10 seconds. This provides two missed heartbeats as buffer before redelivery.

| AckWait | Heartbeat Interval | Missed Heartbeats Before Redeliver |
|---------|-------------------|------------------------------------|
| 30s | 10s | 2 |
| 60s | 20s | 2 |
| 120s | 40s | 2 |

## What Happens When Heartbeats Stop

If a worker stops sending heartbeats (crash, network partition, infinite loop without heartbeat calls), the sequence is:

1. **AckWait expires** -- NATS marks the message as unacknowledged
2. **Redelivery** -- NATS delivers the message to another available consumer
3. **RetryCount increments** -- the new worker sees `ctx.RetryCount()` incremented by 1
4. **MaxDeliver limit** -- after the configured maximum deliveries, the message is discarded or sent to the dead letter stream

The worker that crashed does not need to do anything. JetStream's redelivery mechanism handles the failover automatically.

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

`Heartbeat()` panics if the underlying message is nil (already consumed or not initialized). This is a programmer error -- it means the handler called a completion method before calling Heartbeat, or the TaskContext was used outside its handler scope.

## Related

- [Worker Configuration](/docs/workers/worker-configuration) -- setting up workers
- [Streaming](/docs/coordination/streaming) -- real-time output with heartbeats
- [Checkpoints](/docs/coordination/checkpoints) -- saving state across retries
- [Retry Policies](/docs/reliability/retry-policies) -- controlling retry behavior
