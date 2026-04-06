---
title: Events and Event Sourcing
weight: 5
---

DagNats uses **event sourcing** as its persistence model -- the immutable event log is the source of truth, not the KV snapshots.

## The WORKFLOW_HISTORY Stream

Every state change in a workflow run is recorded as an event on the `WORKFLOW_HISTORY` JetStream stream. Events are published to subjects matching `history.{run_id}` and are retained indefinitely with a 5-second deduplication window (via `Nats-Msg-Id` headers).

This stream is **append-only**. Events are never modified or deleted. The full history of any run can be reconstructed by replaying its events from the stream.

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
| `step.completed` | Worker calls `Complete()` |
| `step.failed` | Worker calls `Fail()` / `FailPermanent()` / `FailRetryAfter()` |
| `step.cancelled` | Step cancelled during run cancellation |
| `step.continue` | Worker calls `Continue()` (agent loop iteration) |

### Agent loop

| Event | When published |
|-------|---------------|
| `agent.loop.iteration` | Engine processes a Continue event |

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

### Compensation (saga)

| Event | When published |
|-------|---------------|
| `compensate.started` | Saga rollback begins |
| `compensate.step.completed` | One compensation step finishes |
| `compensate.failed` | Compensation itself fails |
| `compensate.completed` | All compensation steps succeed |

## How Events Drive DAG Resolution

The engine's core function is `dag.Advance(def, run, event) []Action` -- a **pure function** that takes an immutable workflow definition, the current run state, and a new event, then returns a list of actions to execute (enqueue tasks, complete the workflow, fail the workflow, etc.).

This function contains no I/O. It calculates which steps have all dependencies satisfied and produces the next set of actions. The engine loop is:

1. Consume event from `WORKFLOW_HISTORY`
2. Load run snapshot from KV
3. Call `Advance()` to get actions
4. Execute actions (publish tasks, update KV, publish new events)
5. Repeat

Because `Advance()` is pure, the engine is **stateless**. On restart, it replays the event stream to reconstruct the current state of all active runs.

## Replay Semantics

Any run's complete state can be rebuilt by replaying its events from the `WORKFLOW_HISTORY` stream. This provides:

- **Crash recovery** -- the engine restarts and replays, no data lost
- **Debugging** -- replay a run's history to understand exactly what happened
- **Auditing** -- every state transition is permanently recorded with timestamps

Deduplication via `Nats-Msg-Id` ensures that replayed events (from worker retries or engine restarts) do not create duplicate state transitions.

## Events vs KV Snapshots

DagNats maintains both an event stream and KV snapshots. They serve different purposes:

| Concern | Event stream | KV snapshot |
|---------|-------------|-------------|
| **Authority** | Source of truth | Recovery convenience |
| **Mutability** | Append-only, immutable | Overwritten on each update |
| **Use case** | Audit, replay, debugging | Fast current-state lookup |
| **Optimistic locking** | N/A | KV Revision for CAS |

The KV snapshot in `workflow_runs` stores the latest `WorkflowRun` state for fast reads. The engine updates it after processing each event. If the KV snapshot is lost or corrupted, it can be rebuilt entirely from the event stream.

## Related pages

- [Runs](/docs/concepts/runs) -- the state that events modify
- [Workers](/docs/concepts/workers) -- the source of step completion events
- [Workflows and DAGs](/docs/concepts/workflows-and-dags) -- the definition that `Advance()` evaluates against
