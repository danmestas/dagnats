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
| `RunAndWait(t, svc, workflow, input, timeout)` | Starts a workflow run and blocks until it reaches a terminal status, returns the final `WorkflowRun` snapshot |
| `WaitForStatus(t, svc, runID, timeout, statuses...)` | Polls a run until it reaches one of the target statuses, returns the matching snapshot |

## What Server(t) Provides

A single call to `Server(t)` handles all of the following:

- Starts an embedded NATS server with JetStream enabled
- Creates all required streams (`HISTORY`, `TASK_QUEUES`, `DEAD_LETTERS`, `TELEMETRY`)
- Creates all required KV buckets (`workflow_defs`, `run_snapshots`, `signals`, `triggers`, `workers`, `checkpoints`, `approval_tokens`, `scheduled_runs`, `idempotency_keys`)
- Returns a connected `*nats.Conn` ready for use
- Registers cleanup via `t.Cleanup()` to shut down the server when the test finishes

## RunAndWait

Starts a workflow run and blocks until it reaches any terminal status (`Completed`, `Failed`, `Cancelled`, `Compensated`, `CompensateFailed`). Returns the final `dag.WorkflowRun` snapshot. Fatals the test if the run does not finish within the given timeout.

```go
run := dagnatstest.RunAndWait(t, svc, "my-workflow", input, 10*time.Second)
assert.Equal(t, dag.RunStatusCompleted, run.Status)
```

Under the hood, `RunAndWait` calls `svc.StartRun` and then delegates to `WaitForStatus` with all terminal statuses.

## WaitForStatus

Polls `svc.GetRun` every 25ms until the run reaches one of the given target statuses. Returns the matching `dag.WorkflowRun` snapshot. Fatals the test with a descriptive message (including the last observed status) on timeout.

```go
run := dagnatstest.WaitForStatus(t, svc, runID, 5*time.Second,
    dag.RunStatusCompleted, dag.RunStatusFailed,
)
```

This is useful when you need to wait for a specific subset of statuses rather than all terminal states, or when you already have a run ID from a previous operation.

## Usage

```go
func TestMyWorkflow(t *testing.T) {
    nc := dagnatstest.Server(t)

    // Register a workflow
    tel := observe.NewNoopTelemetry()
    svc := api.NewService(nc, tel)
    svc.RegisterWorkflow(ctx, myWorkflowDef)

    // Start a worker
    w := worker.NewWorker(nc, tel)
    w.Handle("process", myHandler)
    go w.Start()

    // Start a run and wait for completion
    run := dagnatstest.RunAndWait(t, svc, "my-workflow", nil, 10*time.Second)
    assert.Equal(t, dag.RunStatusCompleted, run.Status)
}
```

## Design Notes

Each test gets its own embedded NATS server. Servers are not shared between tests, preventing cross-test interference. The server uses a temporary data directory that is cleaned up automatically.
