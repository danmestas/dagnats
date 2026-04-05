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

## What Server(t) Provides

A single call to `Server(t)` handles all of the following:

- Starts an embedded NATS server with JetStream enabled
- Creates all required streams (`HISTORY`, `TASK_QUEUES`, `DEAD_LETTERS`, `TELEMETRY`)
- Creates all required KV buckets (`workflow_defs`, `run_snapshots`, `signals`, `triggers`, `workers`, `checkpoints`, `approval_tokens`, `scheduled_runs`, `idempotency_keys`)
- Returns a connected `*nats.Conn` ready for use
- Registers cleanup via `t.Cleanup()` to shut down the server when the test finishes

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

    // Start a run and verify results
    runID, _ := svc.StartRun(ctx, "my-workflow", nil)
    // ... assert on run completion
}
```

## Design Notes

Each test gets its own embedded NATS server. Servers are not shared between tests, preventing cross-test interference. The server uses a temporary data directory that is cleaned up automatically.
