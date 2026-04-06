---
title: worker
weight: 2
---

```
import "github.com/danmestas/dagnats/worker"
```

Task execution framework for building DagNats workers. Handles NATS subscription management, message acknowledgment, retry/NAK logic, heartbeat registration, and the task context API that handlers use.

## Key Types

| Type | Description |
|------|-------------|
| `Worker` | Core worker runtime: manages NATS subscriptions, dispatches tasks to handlers, handles lifecycle |
| `TaskContext` | Interface passed to handlers: provides input, completion, failure, checkpointing, and signal methods |
| `WorkerRegistration` | Metadata registered in the `workers` KV bucket for discovery |

## Worker Lifecycle

1. Create a worker with `NewWorker(nc, tel)`
2. Register handlers with `Handle(taskType, fn)` or `HandleTyped(taskType, fn)`
3. Call `Start()` to begin consuming tasks (blocks until shutdown)

## Handle vs HandleTyped

| Method | Signature | Input Access |
|--------|-----------|-------------|
| `Handle` | `func(ctx TaskContext) error` | `ctx.Input()` returns raw `[]byte` |
| `HandleTyped[I, O]` | `func(ctx TaskContext, input I) (O, error)` | Automatic JSON unmarshal into `I`, marshal `O` on return |

`HandleTyped` is a generic convenience wrapper. It unmarshals the input, calls your function, and auto-completes with the marshaled output on nil error.

## TaskContext Methods

| Method | Description |
|--------|-------------|
| `Input() []byte` | Raw input JSON |
| `RunID() string` | Workflow run ID |
| `StepID() string` | Step ID |
| `Attempt() int` | Current retry attempt (1-based) |
| `Complete(output []byte) error` | Mark task as successfully completed |
| `Fail(err error) error` | Mark task as failed |
| `Continue(output []byte) error` | Request next agent-loop iteration |
| `Checkpoint(data []byte) error` | Save incremental state without pausing |
| `LoadCheckpoint() ([]byte, error)` | Load previously saved checkpoint |
| `Pause(duration, checkpoint) error` | Suspend and redeliver after duration |
| `WaitForSignal(name) ([]byte, error)` | Block until a named signal arrives |

## Error Helpers

| Function | Description |
|----------|-------------|
| `Retryable(err)` | Wraps an error to indicate it should be retried (NAK with delay) |
| `IsRetryable(err) bool` | Checks if an error was marked retryable |
| `Fatal(err)` | Wraps an error to indicate no retry (ACK + fail immediately) |
| `IsFatal(err) bool` | Checks if an error was marked fatal |

## Usage

```go
nc, _ := nats.Connect("nats://localhost:4222")
tel := observe.NewNoopTelemetry()
w := worker.NewWorker(nc, tel)

w.Handle("process", func(ctx worker.TaskContext) error {
    input := ctx.Input()
    result := doWork(input)
    return ctx.Complete(result)
})

w.Start() // blocks
```
