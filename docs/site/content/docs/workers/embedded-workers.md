---
title: Embedded Workers
weight: 5
---

Embedded workers run in the same process as the DagNats engine, eliminating network hops between the orchestrator and task handlers.

## When to Use

**Development** -- Run the entire system in a single binary for local iteration. No separate worker processes to manage, no port conflicts, instant feedback.

**Simple deployments** -- When the workflow engine and workers share the same machine and scaling them independently is not needed. A single binary built on `server.Run()` handles orchestration and runs your Go handlers. (`dagnats serve` on its own has no Go handlers; it can only run workers declared in its config as external commands or HTTP endpoints.)

**Testing** -- Integration and end-to-end tests embed workers alongside the engine in the test process. Each test gets its own NATS server, engine, and workers with zero external dependencies.

## How It Works

`server.EmbeddedWorker(srv)` returns a registration shim for a server you have built but not yet started. Register handlers on it, then call `srv.Run()`. When the server starts its components, it creates a regular `worker.Worker` on the server's own NATS connection from your registrations, starts it, and stops it on shutdown.

```go
cfg := server.ConfigFromEnv()
srv := server.New(cfg)

w := server.EmbeddedWorker(srv)
w.Handle("send-email", emailHandler)
w.Handle("resize-image", imageHandler)
w.HandleSingleton("billing-sync", billingHandler)

// Starts NATS, the engine, the API and your worker, then blocks until
// SIGINT or SIGTERM.
if err := srv.Run(); err != nil {
    log.Fatal(err)
}
```

The shim has `Handle`, `HandleLoop`, `HandleStream`, `HandleSignal`, `HandleSingleton` and `WithGroups`. All of them panic if called after `Run()`. It takes no handler options such as `WithAckWait`, and it does not offer `HandleTyped`; use a plain `Handle` handler and decode the input yourself. A server accepts up to 50 shims.

Because the worker uses the same NATS server as the engine, task dispatch and completion events flow through JetStream exactly as they would with separate processes. There is no special "embedded mode" -- the worker subscribes to the same TASK_QUEUES stream and publishes the same events.

## When Not to Use

**Independent scaling** -- If workers need to scale horizontally (more CPU for image processing) while the engine stays at one instance, run workers as separate processes.

**Language diversity** -- If some task handlers are written in Python or TypeScript, those must use the [HTTP Bridge](/docs/workers/http-bridge) regardless.

**Fault isolation** -- A panic in an embedded worker takes down the engine. Separate processes isolate failures.

## Test Pattern

The `dagnatstest` package builds the pieces for an integration test: an embedded NATS server, an orchestrator, an API service and an unstarted worker. Register handlers, start the worker, then run a workflow to a terminal state:

```go
func TestWorkflow(t *testing.T) {
    h := dagnatstest.NewHarness(t)
    h.Handle(t, "my-task", func(ctx worker.TaskContext) error {
        return ctx.Complete([]byte(`{"ok": true}`))
    })
    h.Start(t)

    wf := dag.NewWorkflow("my-workflow")
    wf.Task("only", "my-task").WithTimeout(10 * time.Second)
    def, err := wf.Build()
    if err != nil {
        t.Fatal(err)
    }

    run := h.RegisterAndRun(t, def, []byte(`{}`), 30*time.Second)
    if run.Status != dag.RunStatusCompleted {
        t.Fatalf("status = %s", run.Status)
    }
}
```

Each test gets an isolated NATS server, so tests run in parallel without interference.

## Related

- [Worker Configuration](/docs/workers/worker-configuration) -- full configuration reference
- [HTTP Bridge](/docs/workers/http-bridge) -- non-Go worker support
