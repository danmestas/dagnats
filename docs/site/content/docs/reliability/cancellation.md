---
title: Cancellation
weight: 6
---

Running workflows can be cancelled through the CLI or API, with the engine propagating cancellation to all in-flight steps.

## Cancelling a Run

Cancel a running workflow by run ID:

```bash
dagnats run cancel <run-id>
```

Or via the REST API:

```bash
curl -X POST http://localhost:8080/runs/<run-id>/cancel
```

The engine publishes a `workflow.cancelled` event to the history stream. The run status transitions to `cancelled`.

## Propagation

When a run is cancelled, the engine:

1. Marks the run status as `cancelled`
2. Cancels all **pending** steps (not yet dispatched) by setting them to `cancelled`
3. Marks all **queued** and **running** steps `cancelled` in the run snapshot, and frees the task concurrency slots they held
4. Removes the run's event waiters and approval tokens, and cancels its non-detached child workflows

A queued task message stays on `TASK_QUEUES` until a worker picks it up; the worker checks the run's state first and skips the task if the run is already cancelled.

A task already running is told. The engine publishes an empty message on the core NATS subject `dagnats.cancel.{runID}`, and the worker SDK cancels the `Context()` of every in-flight handler of that run. A handler that passes `ctx.Context()` to its model calls, tool calls and HTTP requests stops promptly. A handler that ignores the context runs to the end, and the engine drops its events because the run is terminal.

Core NATS delivery is at-most-once, so the signal can be missed. The worker therefore also checks the run's state on each `Heartbeat()`, which returns `worker.ErrRunCancelled` once the run is cancelled. Over the [HTTP bridge](/docs/workers/http-bridge), a cancelled run is a `409 Conflict` on resolve and heartbeat. The cancel subject carries only a run ID; a deployment that grants broad publish rights should restrict `dagnats.cancel.>` to the engine.

## Event-Driven Cancellation

The `CancelOn` field on `WorkflowDef` specifies events that automatically cancel a running workflow:

```go
wf := dag.NewWorkflow("deploy").
    CancelOnWithTimeout("deploy.rollback", dag.Match{
        Left:  "env",
        Op:    dag.MatchOpEq,
        Right: "input.env",
    }, 1*time.Hour)
```

`CancelOn(event, match)` is the same without a timeout. The event name is the part of the subject after `event.`, so this watches `event.deploy.rollback` on the `EVENTS` stream. When a matching event arrives, the engine cancels the run without manual intervention. `Match` compares a dot-path in the event payload (`Left`) with a value taken from the run's input (`Right`, which must start with `input.`). `Op` is required and the only operator is `dag.MatchOpEq`. In this example only a rollback event whose `env` equals the run's `env` input cancels the run. A workflow can have at most 5 `cancel_on` entries.

## Graceful Shutdown

When the `dagnats serve` process receives a shutdown signal (SIGTERM/SIGINT), it shuts down within a 15-second deadline: the HTTP server first, then triggers and the API service, then any embedded config-driven workers, then the orchestrator, then telemetry, and finally it drains the NATS connection and stops the embedded server. See [Deployment Models]({{< ref "/docs/operations/deployment-models" >}}) for the full order.

Runs that were in progress during shutdown resume automatically when the server restarts. The orchestrator keeps no state between events: run state is in the `workflow_runs` snapshots, and its durable consumer on `WORKFLOW_HISTORY` continues from the last event it acknowledged.

## Child Workflow Cancellation

Cancelling a parent workflow propagates to child workflows spawned by [sub-workflow steps](/docs/step-types/sub-workflows). Each cancelled child cancels its own children in turn. Nesting depth is capped at 3. Children started as detached are not cancelled.

## Related Pages

- [Timeouts](/docs/reliability/timeouts) -- automatic cancellation on deadline
- [Error Handling](/docs/reliability/error-handling) -- failure handling after cancellation
- [Concurrency Limits](/docs/flow-control/concurrency-limits) -- preventing overload
