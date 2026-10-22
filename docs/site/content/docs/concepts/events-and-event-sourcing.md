---
title: Events and Event Sourcing
weight: 5
---

DagNats is **event-driven**: every state change is an immutable event on a JetStream stream, and the engine advances a run by handling one event at a time. The event log is the ordered record of what happened. The current state of each run lives in a KV snapshot that the engine updates as it handles events.

## The WORKFLOW_HISTORY Stream

Every state change in a workflow run is recorded as an event on the `WORKFLOW_HISTORY` JetStream stream. Events are published to subjects matching `history.{run_id}`, with a 5-second deduplication window (via `Nats-Msg-Id` headers). The stream keeps events for 30 days, then ages them out.

This stream is **append-only**. Events are never modified. Within the retention window, you can read any run's full history with `dagnats run events <run-id>`.

## Event Types

Events are categorized by lifecycle scope:

### Workflow lifecycle

| Event | When published |
|-------|---------------|
| `workflow.started` | Run begins execution |
| `workflow.completed` | All non-auxiliary steps succeed |
| `workflow.failed` | A step fails permanently |
| `workflow.cancelled` | Run is cancelled |
| `workflow.spawn` | Sub-workflow child is created |
| `workflow.child.completed` | Child sub-workflow finishes successfully |
| `workflow.child.failed` | Child sub-workflow fails |

### Step lifecycle

| Event | When published |
|-------|---------------|
| `step.queued` | Engine dispatches a step's task |
| `step.started` | A worker begins an attempt |
| `step.completed` | Worker calls `Complete()` |
| `step.failed` | Worker calls `Fail()` / `FailPermanent()` / `FailRetryAfter()` |
| `step.cancelled` | Step cancelled during run cancellation |
| `step.continue` | Worker calls `Continue()` (agent loop iteration) |

### Agent loop

| Event | When published |
|-------|---------------|

### Sleep and wait

| Event | When published |
|-------|---------------|
| `step.sleep.started` | Sleep step timer begins |
| `step.sleep.completed` | Sleep duration elapses |
| `step.wait.started` | WaitForEvent step begins watching |
| `step.wait.matched` | External event matches the condition |
| `step.wait.timeout` | Wait timeout expires without a match |

### Map steps

| Event | When published |
|-------|---------------|
| `step.map.started` | Map step fans out to individual tasks |
| `step.map.completed` | All map instances finish (or one fails) |
| `step.map.instance.completed` | One map item finishes |

### Approval and planning

| Event | When published |
|-------|---------------|
| `approval.requested` | An approval step opens its gate |
| `approval.granted` | An approver accepts |
| `approval.rejected` | An approver rejects |
| `approval.expired` | The approval timeout elapses |
| `planner.materialized` | A planner step's generated steps are added to the run |

### Compensation (saga)

| Event | When published |
|-------|---------------|
| `compensate.started` | Saga rollback begins |
| `compensate.step.completed` | One compensation step finishes |
| `compensate.failed` | Compensation itself fails |
| `compensate.completed` | All compensation steps succeed |

## How Events Drive DAG Resolution

The engine's core function is `Advance(def, run, event)` -- a **pure function** that takes an immutable workflow definition, the current run state, and a new event, then returns the updated run and a list of side effects (enqueue a task, complete the workflow, fail the workflow, skip a step).

This function contains no I/O. It calculates which steps have all dependencies satisfied and produces the next set of side effects. The engine loop is:

1. Consume an event from `WORKFLOW_HISTORY`
2. Load the run snapshot from KV
3. Call `Advance()` to get the new state and side effects
4. Execute the side effects (publish tasks, publish new events)
5. Save the snapshot to KV, with a revision check
6. Acknowledge the event

Because `Advance()` is pure and the engine keeps nothing in memory between events, any engine instance can handle any event. A cluster runs one engine per node, and the revision check in step 5 stops two of them from overwriting each other. The loser reloads and redoes its transition.

## Redelivery and Recovery

The engine consumes `history.>` through a durable consumer named `orchestrator`. Acknowledgements persist, so a restarted engine resumes where it left off rather than replaying the stream. An event the engine did not acknowledge is redelivered on a backoff schedule; after eight deliveries it is dead-lettered. Handlers ignore events they have already applied, so redelivery is safe.

A periodic reconciler (every 60 seconds) finds runs stuck in `running` with no step in flight and either completes them or fails them with an explanatory step state, so a lost event cannot leave a run hanging forever.

Deduplication via `Nats-Msg-Id` stops a retried publish from entering the stream twice.

## Events vs KV Snapshots

DagNats maintains both an event stream and KV snapshots. They serve different purposes:

| Concern | Event stream | KV snapshot |
|---------|-------------|-------------|
| **Role** | Ordered record that drives the engine | Current state of each run |
| **Mutability** | Append-only, immutable | Overwritten on each update |
| **Retention** | 30 days | Terminal runs pruned after `runs_max_age` (default 30 days) |
| **Use case** | Audit, debugging, reconstructing a run's timeline | The engine's working state and fast reads (`GET /runs/{id}`) |
| **Concurrency control** | `Nats-Msg-Id` dedup | KV revision (compare-and-swap) |

The `workflow_runs` snapshot is what the engine loads for every event, so losing it loses the run's state. Back up the data directory (or run a replicated cluster) accordingly.

## Related pages

- [Runs](/docs/concepts/runs) -- the state that events modify
- [Workers](/docs/concepts/workers) -- the source of step completion events
- [Workflows and DAGs](/docs/concepts/workflows-and-dags) -- the definition that `Advance()` evaluates against
