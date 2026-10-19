---
title: Workers
weight: 3
---

A **worker** is a process that subscribes to NATS task subjects, executes handler functions, and publishes completion events back to the engine.

## Lifecycle

Workers follow a strict lifecycle: create, register handlers, start, consume tasks, stop.

```mermaid
stateDiagram-v2
    [*] --> Created: NewWorker()
    Created --> Registered: Handle() / HandleTyped()
    Registered --> Running: Start()
    Running --> Running: consume tasks
    Running --> Stopped: Stop()
    Stopped --> [*]
```

**Create** allocates the worker and binds it to a NATS connection. **Register** maps task type strings to handler functions. **Start** creates JetStream pull consumers for each registered task type and begins consuming. **Stop** unsubscribes all consumers and deregisters from the worker directory.

## Creating a Worker

`worker.NewWorker()` takes a NATS connection and variadic options. Tracing and metrics use the global OpenTelemetry providers, which are no-ops unless you install your own.

```go
w := worker.NewWorker(nc)
```

Options include:

| Option | Purpose |
|--------|---------|
| `WithGroups(groups...)` | Subscribe only to specific worker groups |
| `WithPartitions(n)` | Enable elastic consumer groups with n partitions |
| `WithControlPlane(cp)` | Grant steps that declare the `control-plane` capability a handle for starting and registering workflows at run time |

## Registering Handlers

Two registration patterns:

### Handle (raw bytes)

`Handle()` registers a `HandlerFunc` that receives a `TaskContext` with raw `[]byte` input. The handler calls exactly one terminal method per execution.

```go
w.Handle("fetch", func(ctx worker.TaskContext) error {
    data := fetchFromAPI(ctx.Input())
    return ctx.Complete(data)
})
```

### HandleTyped (generics)

`HandleTyped[I, O]()` wraps a typed handler function. JSON marshal/unmarshal is handled automatically. Serialization failures are non-retriable.

```go
worker.HandleTyped(w, "transform", func(
    ctx worker.TaskContext, input TransformInput,
) (TransformOutput, error) {
    result := transform(input)
    return result, nil
})
```

The typed handler calls `Complete()` automatically on success -- you return the output value and error, not call `Complete()` yourself.

### HandleSingleton

`HandleSingleton()` registers a handler that runs as a single-partition elastic consumer group. Only one consumer processes messages at a time across all worker instances. Useful for ordered processing or exclusive resource access.

## TaskContext Interface

Every handler receives a `TaskContext` -- the deep interface that hides all NATS mechanics behind clean method calls.

### Identity and input

| Method | Returns | Purpose |
|--------|---------|---------|
| `Input()` | `[]byte` | Raw task input payload |
| `RunID()` | `string` | Workflow run identifier |
| `StepID()` | `string` | Step identifier within the run |
| `RetryCount()` | `int` | Retry attempt number the engine stamped on the task; 0 on the first dispatch |
| `Metadata()` | `map[string]string` | The step's static `metadata` from the workflow definition; `nil` if none |
| `Context()` | `context.Context` | The task's context, carrying its trace span |

### Step completion

Call exactly one of these per execution:

| Method | Purpose |
|--------|---------|
| `Complete(output)` | Mark step as succeeded with output |
| `Fail(err)` | Mark as retriable failure (retries apply) |
| `FailPermanent(err)` | Mark as non-retriable failure (skip all retries) |
| `FailRetryAfter(err, d)` | Fail with explicit retry delay, bypassing backoff |
| `Continue(output)` | Agent loop: signal another iteration |

### Streaming and heartbeat

| Method | Purpose |
|--------|---------|
| `PutStream(data)` | Publish data to `stream.{runID}.{stepID}` via core NATS (ephemeral, fire-and-forget) |
| `Heartbeat()` | Extend the AckWait claim to prevent redelivery during long work (does not extend the step timeout) |

### Logs

| Method | Purpose |
|--------|---------|
| `LogOut()` | `io.Writer` that captures stdout-tagged output to the `BUILD_LOGS` hot lane. Hand it to `cmd.Stdout` |
| `LogErr()` | The same for stderr |

Every terminal method flushes buffered log bytes and writes an attempt-ending marker before it publishes the step result. Read the output with `GET /runs/{id}/logs`.

### Checkpointing

| Method | Purpose |
|--------|---------|
| `Checkpoint(state)` | Save arbitrary state to KV at `{runID}.{stepID}` |
| `LoadCheckpoint()` | Retrieve saved state (returns `nil, nil` if none exists) |
| `Pause(name, d)` | Checkpoint + NakWithDelay for mid-task durable delay |

### Signals

| Method | Purpose |
|--------|---------|
| `WaitForSignal(name, timeout)` | Block until KV key `{runID}.{name}` is written (max 1 hour) |
| `SendSignal(runID, name, data)` | Write to KV to wake a waiting step |

### Control plane

`ControlPlane()` returns a handle for starting runs and registering workflow definitions from inside a step, or `nil`. It is `nil` unless the step declares the `control-plane` capability in `required_capabilities` and the worker was built with `WithControlPlane`. Always nil-check it.

## Graceful Shutdown

`Stop()` unsubscribes all JetStream consumers, stops the heartbeat goroutine, and deregisters from the worker directory KV bucket. In-flight tasks that have not yet called a terminal method are redelivered by JetStream after `AckWait` expires. A redelivery continues the attempt's step timeout; it does not restart it.

```go
w.Start()
defer w.Stop()

// Block until shutdown signal
sig := make(chan os.Signal, 1)
signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
<-sig
```

## Related pages

- [Steps](/docs/concepts/steps) -- what workers execute
- [Runs](/docs/concepts/runs) -- the execution context workers operate within
- [Events and Event Sourcing](/docs/concepts/events-and-event-sourcing) -- what happens after a worker completes
