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
curl -X POST http://localhost:8080/v1/runs/<run-id>/cancel
```

The engine publishes a `workflow.cancelled` event to the history stream. The run status transitions to `cancelled`.

## Propagation

When a run is cancelled, the engine:

1. Marks the run status as `cancelled`
2. Cancels all **pending** steps (not yet dispatched) by setting them to `cancelled`
3. Cancels all **queued** steps by letting their NATS messages expire (AckWait timeout)
4. In-flight steps that are already running on a worker continue until the worker checks for cancellation or the step's AckWait expires

Workers do not receive an explicit cancellation signal mid-execution. Instead, the next time the worker attempts to publish a result event, the engine ignores events for cancelled runs. This design avoids the complexity of distributed cancellation protocols.

## Event-Driven Cancellation

The `CancelOn` field on `WorkflowDef` specifies events that automatically cancel a running workflow:

```go
wf := dag.NewWorkflow("deploy").
    WithCancelOn(dag.CancelOn{
        Event: "event.deploy.rollback",
        Match: dag.Match{
            Left:  "env",
            Right: "env",
        },
        Timeout: 1 * time.Hour,
    })
```

When a matching event arrives on the `EVENTS` stream, the engine cancels the run without manual intervention. The `Match` field correlates the event payload against the run input -- in this example, only a rollback event for the same `env` value cancels the run.

## Graceful Shutdown

When the `dagnats serve` process receives a shutdown signal (SIGTERM/SIGINT), it performs a graceful shutdown:

1. Stops accepting new workflow runs
2. Drains in-flight NATS subscriptions
3. Waits for active workers to finish current tasks (bounded by AckWait)
4. Persists final state to KV

Runs that were in progress during shutdown resume automatically when the server restarts -- the orchestrator replays from the `WORKFLOW_HISTORY` stream and picks up where it left off.

## Child Workflow Cancellation

Cancelling a parent workflow propagates to child workflows spawned by [sub-workflow steps](/docs/step-types/sub-workflows). The engine cancels child runs recursively (bounded by max nesting depth of 3).

## Related Pages

- [Timeouts](/docs/reliability/timeouts) -- automatic cancellation on deadline
- [Error Handling](/docs/reliability/error-handling) -- failure handling after cancellation
- [Concurrency Limits](/docs/flow-control/concurrency-limits) -- preventing overload
