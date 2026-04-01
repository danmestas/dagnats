# Core Architecture

## Design Philosophy

- **Ousterhout:** Deep modules, small interfaces, pull complexity downward
- **TigerStyle:** Safety > Performance > DX, bounded everything, assertions as contracts
- **HIPP:** Simplicity, self-containment, zero external deps beyond nats.go

## Five Components

| Package | Role | Key Constraint |
|---------|------|----------------|
| `dag/` | Pure DAG logic, workflow types, validation | Zero I/O. `Advance()` is a pure function |
| `engine/` | Stateless event processor, DAG orchestration | Reads history stream, writes KV snapshots |
| `worker/` | Task execution framework | Deep `TaskContext` hides all NATS mechanics |
| `api/` | Control plane (REST + NATS micro) | Wrapper → Inner pattern with tracing/metrics |
| `cli/` | Thin client over api.Service | No business logic, no direct NATS access |

## Event Sourcing Model

- **Source of truth:** Immutable event log on `WORKFLOW_HISTORY` stream (`history.{run_id}`)
- **KV snapshots:** Recovery convenience, not authoritative
- **Orchestrator:** Stateless — replays from stream on restart
- **Event types:** `workflow.started`, `workflow.completed`, `workflow.failed`, `workflow.cancelled`, `workflow.spawn`, `workflow.child.completed`, `workflow.child.failed`, `step.completed`, `step.failed`, `step.cancelled`, `step.continue`, `agent.loop.iteration`

## NATS Primitives (Instead of Custom Infrastructure)

| Need | NATS Primitive |
|------|---------------|
| Task distribution | JetStream pull consumers + MaxAckPending |
| Retry with backoff | NakWithDelay (no timer service) |
| Exactly-once delivery | Nats-Msg-Id dedup |
| Run state snapshots | KV with Revision (optimistic locking) |
| Cross-workflow signals | KV watches |
| Worker health | Consumer idle heartbeats |
| Internal API | NATS micro (discovery + load balancing) |
| Workflow def versioning | KV revision history |
| Large payloads | Object Store + event references |
| Step timeouts | AckWait + MaxDeliver |

## NATS Resources

**Streams:**

| Stream | Subjects | Purpose |
|--------|----------|---------|
| WORKFLOW_HISTORY | `history.>` | Immutable event log (5s dedup) |
| TASK_QUEUES | `task.>` | Work queue distribution |
| EVENTS | `event.>` | External triggers |
| DEAD_LETTERS | `dead.>` | Permanent failures (30-day retention) |
| TELEMETRY | `telemetry.>` | Observability signals (7-day, 1GB max) |

**KV Buckets:**

| Bucket | Purpose |
|--------|---------|
| workflow_defs | Immutable workflow definitions |
| workflow_runs | Mutable run state snapshots |
| checkpoints | Worker step state persistence |
| signals | Cross-workflow KV-based signaling |
| triggers | Trigger definitions |
| trigger_state | Cron last-run timestamps |
| concurrency_runs | Per-workflow run counters |

## DAG Resolution

`dag.Advance(def, run, event) []Action` is the core pure function:
- Input: immutable def + mutable run + new event
- Output: list of actions (EnqueueTask, CompleteWorkflow, FailWorkflow, ReEnqueueAgentLoop)
- Calculates ready steps from dependency graph
- Skipped steps treated as completed for downstream resolution
- No recursion — iterative with explicit visited set

## Step Types

| Type | Behavior |
|------|----------|
| Normal | Execute once, Complete or Fail |
| AgentLoop | Iterative with Continue(), bounded by MaxIterations/MaxDuration |
| Agent | Routed to agent SDK (opaque to engine, metadata-driven) |
| SubWorkflow | Spawn child workflow, parent waits via KV watch |

## Worker SDK (TaskContext)

Deep interface hiding NATS complexity:

- `Input()`, `RunID()`, `StepID()`, `RetryCount()` — read-only context
- `Complete(output)`, `Fail(err)`, `Continue(output)` — exactly one per invocation
- `PutStream(data)` — real-time streaming via core pub/sub (`stream.{runID}.{stepID}`)
- `Heartbeat()` — extends AckWait via InProgress()
- `Checkpoint(state)` / `LoadCheckpoint()` — KV persistence at `{runID}.{stepID}`
- `WaitForSignal(name, timeout)` / `SendSignal(runID, name, data)` — KV watch-based
- `WithGroups(groups...)` — worker group routing via subject subscription

## Child Workflows

- `ParentRunID` + `ParentStepID` on WorkflowRun link parent to child
- Max nesting depth: 3 (enforced on spawn)
- Lifecycle: `workflow.spawn` → child runs → `workflow.child.completed`/`failed` back to parent
- Parent step blocks until child completes (KV watch pattern)

## Competitive Context

Built to reach parity with Kestra, Hatchet, and Temporal. Key differentiators:
- NATS-native (no Postgres, no external queue)
- Event sourcing (not mutable row state)
- Agent loop as native primitive (not bolted on)
- Single-binary deployment target
