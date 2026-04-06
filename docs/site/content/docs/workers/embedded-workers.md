---
title: Embedded Workers
weight: 5
---

Embedded workers run in the same process as the DagNats engine, eliminating network hops between the orchestrator and task handlers.

## When to Use

**Development** -- Run the entire system in a single binary for local iteration. No separate worker processes to manage, no port conflicts, instant feedback.

**Simple deployments** -- When the workflow engine and workers share the same machine and scaling them independently is not needed. A single `dagnats serve` process handles orchestration and task execution.

**Testing** -- Integration and end-to-end tests embed workers alongside the engine in the test process. Each test gets its own NATS server, engine, and workers with zero external dependencies.

## How It Works

An embedded worker uses the same `worker.NewWorker()` constructor and `Handle()` registration as a standalone worker. The only difference is lifecycle management -- the worker starts and stops alongside the engine in the same process.

```go
nc, _ := nats.Connect(nats.DefaultURL)
natsutil.SetupAll(nc)

// Create engine
eng := engine.New(nc, tel)

// Create embedded worker
w := worker.NewWorker(nc, tel)
w.Handle("send-email", emailHandler)
w.Handle("resize-image", imageHandler)
w.Start()
defer w.Stop()

// Engine and worker share the same NATS connection
eng.Start()
defer eng.Stop()
```

Because both the engine and worker connect to the same NATS server, task dispatch and completion events flow through JetStream exactly as they would with separate processes. There is no special "embedded mode" -- the worker subscribes to the same TASK_QUEUES stream and publishes the same events.

## When Not to Use

**Independent scaling** -- If workers need to scale horizontally (more CPU for image processing) while the engine stays at one instance, run workers as separate processes.

**Language diversity** -- If some task handlers are written in Python or TypeScript, those must use the [HTTP Bridge](/docs/workers/http-bridge) regardless.

**Fault isolation** -- A panic in an embedded worker takes down the engine. Separate processes isolate failures.

## Test Pattern

The `dagnatstest` package provides a one-call setup that creates an embedded NATS server, engine, and worker for testing:

```go
func TestWorkflow(t *testing.T) {
    env := dagnatstest.Setup(t)
    env.Worker.Handle("my-task", func(ctx worker.TaskContext) error {
        return ctx.Complete([]byte(`{"ok": true}`))
    })
    env.Start()
    // Submit workflow and assert results...
}
```

Each test gets an isolated NATS server, so tests run in parallel without interference.

## Related

- [Worker Configuration](/docs/workers/worker-configuration) -- full configuration reference
- [HTTP Bridge](/docs/workers/http-bridge) -- non-Go worker support
