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

1. Create a worker with `NewWorker(nc, opts...)`. Options are `WithGroups`, `WithPartitions` and `WithControlPlane`.
2. Register handlers with `Handle(taskType, fn, opts...)`, `HandleSingleton(taskType, fn)` or the generic `HandleTyped(w, taskType, fn)`.
3. Call `Start()` to create the consumers. It returns once they are running, so keep the process alive yourself.
4. Call `Stop()` to stop consuming and wait up to 30 seconds for running handlers.

## Handle vs HandleTyped

| Function | Signature | Input Access |
|----------|-----------|-------------|
| `(*Worker).Handle` | `func(ctx TaskContext) error` | `ctx.Input()` returns raw `[]byte` |
| `HandleTyped[I, O]` | `func(ctx TaskContext, input I) (O, error)` | Automatic JSON unmarshal into `I`, marshal `O` on return |

`HandleTyped` is a generic convenience wrapper. It unmarshals the input, calls your function, and completes with the marshaled output on a nil error. Pass `worker.UnwrapTrigger()` to receive the `data` of a trigger envelope instead of the envelope.

## TaskContext Methods

| Method | Description |
|--------|-------------|
| `Input() []byte` | Raw input JSON |
| `RunID() string`, `StepID() string` | Run and step IDs |
| `RetryCount() int` | 0 on the first dispatch, then the attempt number (2, 3, ...) on engine retries |
| `Metadata() map[string]string` | The step's static metadata |
| `Context() context.Context` | Context of the task |
| `Complete(output []byte) error` | Finish the step |
| `Fail(err error) error` | Fail the attempt; the retry policy applies |
| `FailPermanent(err error) error` | Fail without retrying |
| `FailRetryAfter(err error, d time.Duration) error` | Fail and retry after `d` |
| `Continue(output []byte) error` | Request next agent-loop iteration |
| `Heartbeat() error` | Extend the claim on the task |
| `PutStream(data []byte) error` | Publish a live token to `stream.{run_id}.{step_id}` |
| `LogOut() io.Writer`, `LogErr() io.Writer` | Stream stdout and stderr chunks into the run's logs |
| `Checkpoint(state []byte) error` | Save state to the checkpoints KV |
| `LoadCheckpoint() ([]byte, error)` | Load previously saved state |
| `Pause(name string, d time.Duration) error` | Save a pause marker and run the handler again after `d` |
| `WaitForSignal(name string, timeout time.Duration) ([]byte, error)` | Block until a named signal arrives |
| `SendSignal(runID, name string, data []byte) error` | Send a signal |
| `ControlPlane() ControlPlane` | Control-plane handle, or nil |

## Error Helpers

| Function | Description |
|----------|-------------|
| `NewNonRetryableError(err)` | Return it from a handler to fail permanently with no retry |
| `NewRateLimitError(err, d)` | Return it from a handler to retry after `d` |

Both are detected with `errors.As`, so they can be wrapped.

## Usage

```go
nc, _ := nats.Connect("nats://localhost:4222")
w := worker.NewWorker(nc)

w.Handle("process", func(ctx worker.TaskContext) error {
    input := ctx.Input()
    result := doWork(input)
    return ctx.Complete(result)
})

w.Start() // returns once consumers are running
defer w.Stop()
```
