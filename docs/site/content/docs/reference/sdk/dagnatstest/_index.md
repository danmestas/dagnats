---
title: dagnatstest
weight: 9
---

```
import "github.com/danmestas/dagnats/dagnatstest"
```

Test helpers for DagNats workflows. Starts an embedded NATS server with all required streams and KV buckets in a single call, ready for workflow testing.

## Key Functions

| Function | Description |
|----------|-------------|
| `Server(t)` | Starts an embedded NATS server with JetStream and all DagNats infrastructure, returns a ready `*nats.Conn` |
| `NewHarness(t)` | Starts NATS, an orchestrator and an API service, and builds an unstarted worker. Returns a `*Harness` |
| `(*Harness).Handle(t, taskType, fn)` | Registers a handler on the harness worker |
| `HandleTypedOn[I, O](h, t, taskType, fn)` | Registers a typed handler (generic, so not a method) |
| `(*Harness).Start(t)` | Starts the worker and stops it when the test ends |
| `(*Harness).RegisterAndRun(t, def, input, timeout)` | Registers a definition, runs it, and waits for a terminal status |
| `RunAndWait(t, svc, workflow, input, timeout)` | Starts a run and blocks until it reaches a terminal status |
| `WaitForStatus(t, svc, runID, timeout, statuses...)` | Polls a run until it reaches one of the target statuses |
| `Workflow(t, svc, builder)` | Builds and registers a workflow, failing the test on error |
| `Worker[I, O](t, nc, taskType, fn)` | Starts a worker with one typed handler, stopped at test end |
| `MockTaskContext` | A `worker.TaskContext` for unit-testing handlers without NATS |

## What Server(t) Provides

A single call to `Server(t)` starts an embedded NATS server with JetStream enabled, creates the streams and KV buckets DagNats needs, returns a connected `*nats.Conn`, and shuts the server down with `t.Cleanup()`.

## RunAndWait

Starts a workflow run and blocks until it reaches any terminal status (`Completed`, `Failed`, `Cancelled`, `Compensated`, `CompensateFailed`). Returns the final `dag.WorkflowRun` snapshot. Fatals the test if the run does not finish within the given timeout. `svc` is the harness's `h.Svc`.

```go
run := dagnatstest.RunAndWait(t, h.Svc, "my-workflow", input, 10*time.Second)
if run.Status != dag.RunStatusCompleted {
    t.Fatalf("status = %s", run.Status)
}
```

## WaitForStatus

Polls the run every 25ms until it reaches one of the given statuses. Returns the matching snapshot, or fatals the test on timeout.

```go
run := dagnatstest.WaitForStatus(t, h.Svc, runID, 5*time.Second,
    dag.RunStatusCompleted, dag.RunStatusFailed,
)
```

## Usage

```go
func TestMyWorkflow(t *testing.T) {
    h := dagnatstest.NewHarness(t)
    h.Handle(t, "process", func(ctx worker.TaskContext) error {
        return ctx.Complete([]byte(`{"ok": true}`))
    })
    h.Start(t)

    wf := dag.NewWorkflow("my-workflow")
    wf.Task("only", "process").WithTimeout(10 * time.Second)
    def, err := wf.Build()
    if err != nil {
        t.Fatal(err)
    }

    run := h.RegisterAndRun(t, def, nil, 10*time.Second)
    if run.Status != dag.RunStatusCompleted {
        t.Fatalf("status = %s", run.Status)
    }
}
```

## Design Notes

Each test gets its own embedded NATS server. Servers are not shared between tests, preventing cross-test interference. The harness fields `Engine` and `Svc` have internal types, so use their methods but do not name the types.
