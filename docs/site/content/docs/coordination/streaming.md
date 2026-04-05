---
title: Streaming
weight: 3
---

Streaming publishes real-time data from a running step to any subscribed client using core NATS pub/sub.

## Overview

Some workflows produce output incrementally -- LLM token generation, log lines, progress updates. Waiting for `Complete()` to see any output is not acceptable for these use cases. **PutStream** lets a handler publish data mid-execution on a subject that clients can subscribe to for live delivery.

Streaming uses core NATS publish (not JetStream). Messages are ephemeral, fire-and-forget. If no subscriber is listening, the data is lost. This is intentional: streaming is for real-time observation, not durable state. For durable output, use `Complete()` or [Checkpoints](/docs/coordination/checkpoints).

The subject format is `stream.{runID}.{stepID}`. Any NATS client can subscribe to this subject to receive live data from a specific step, or use a wildcard like `stream.{runID}.>` to receive all streaming output from a run.

## API

### PutStream

Publishes data to the step's streaming subject.

```go
w.Handle("generate-text", func(ctx worker.TaskContext) error {
    for i, chunk := range generateChunks(ctx.Input()) {
        ctx.PutStream(chunk)
        if i%10 == 0 {
            ctx.Heartbeat()
        }
    }
    return ctx.Complete(assembleResult())
})
```

`PutStream` publishes to `stream.{runID}.{stepID}` via `nc.Publish` -- a plain NATS core publish. There is no ack, no persistence, no backpressure. If the publish buffer is full, it returns an error.

### Subscribing to Streams

Clients subscribe using a standard NATS subscription:

```go
sub, _ := nc.Subscribe(
    fmt.Sprintf("stream.%s.%s", runID, stepID),
    func(msg *nats.Msg) {
        fmt.Print(string(msg.Data))
    },
)
defer sub.Unsubscribe()
```

Wildcard subscription for all steps in a run:

```go
sub, _ := nc.Subscribe(
    fmt.Sprintf("stream.%s.>", runID),
    func(msg *nats.Msg) {
        // msg.Subject contains the full stream.{runID}.{stepID}
        fmt.Printf("[%s] %s\n", msg.Subject, msg.Data)
    },
)
```

### CLI: Tailing Logs

The `dagnats logs --tail` command subscribes to the streaming subject for a run and prints output as it arrives:

```bash
dagnats logs --tail <run-id>
dagnats logs --tail <run-id> --step <step-id>
```

## Streaming vs. Durable Output

| Aspect | PutStream | Complete |
|--------|-----------|----------|
| Delivery | Fire-and-forget | Durable (JetStream) |
| Persistence | None | Stored in run history |
| Backpressure | None | Ack-based |
| Use case | Real-time observation | Final result |
| Subscriber required | Yes, or data is lost | No |

Use both together: stream tokens as they arrive for live UX, then call `Complete()` with the assembled final output for durable storage.

## Heartbeat During Streaming

Long-running steps that stream data should call `Heartbeat()` periodically to prevent NATS message redelivery. The AckWait timer on the task message resets with each heartbeat:

```go
w.Handle("long-stream", func(ctx worker.TaskContext) error {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for chunk := range processLargeInput(ctx.Input()) {
        ctx.PutStream(chunk)
        select {
        case <-ticker.C:
            ctx.Heartbeat()
        default:
        }
    }
    return ctx.Complete([]byte("done"))
})
```

## LLM Pattern: Streaming Token Output to Clients

An LLM handler streams tokens as they arrive from the model API, giving the end user immediate feedback:

```go
w.Handle("llm-generate", func(ctx worker.TaskContext) error {
    var fullResponse strings.Builder
    stream, err := openLLMStream(ctx.Input())
    if err != nil {
        return ctx.Fail(err)
    }
    count := 0
    for token := range stream.Tokens() {
        ctx.PutStream([]byte(token))
        fullResponse.WriteString(token)
        count++
        if count%50 == 0 {
            ctx.Heartbeat()
        }
    }
    return ctx.Complete([]byte(fullResponse.String()))
})
```

A frontend subscribes to `stream.{runID}.{stepID}` and renders tokens as they arrive. The final assembled response is stored durably via `Complete()`.

## Related

- [Checkpoints](/docs/coordination/checkpoints) -- durable state persistence
- [Signals](/docs/coordination/signals) -- cross-step coordination
- [Agent Loops](/docs/step-types/agent-loops) -- iterative steps that benefit from streaming
