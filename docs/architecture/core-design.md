# Core Architecture

## Design Philosophy

- **Ousterhout:** Deep modules, small interfaces, pull complexity downward
- **TigerStyle:** Safety > Performance > DX, bounded everything, assertions as contracts
- **HIPP:** Simplicity, self-containment, zero external deps beyond nats.go

## Core Components

| Package | Role | Key Constraint |
|---------|------|----------------|
| `dag/` | Pure DAG logic, workflow types, validation | Zero I/O. `Advance()` is a pure function |
| `internal/engine/` | Stateless event processor, DAG orchestration | Reads history stream, writes KV snapshots |
| `worker/` | Task execution framework | Deep `TaskContext` hides all NATS mechanics |
| `internal/api/` | Control plane (REST + NATS micro) | Wrapper → Inner pattern with tracing/metrics |
| `cli/` | Thin client over api.Service | No business logic, no direct NATS access |
| `bridge/` | HTTP-to-NATS gateway for remote workers | 3 deep endpoints, ack map, capability parity |
| `sdk/httpclient/` | Go HTTP reference client | Validates wire protocol, template for other SDKs |
| `internal/natsutil/` | NATS resource setup (streams, KV) | Plumbing — not public API |
| `internal/trigger/` | Cron, subject, webhook triggers | Lives behind api/server |
| `internal/observe/simple/` | Concrete telemetry adapters | Implements `observe/` interfaces |

**Public vs internal:** `dag/`, `protocol/`, `observe/` (interfaces), `worker/`, `actor/`, `bridge/`, `sdk/`, `server/`, `cli/` are public API. Implementation packages live under `internal/` to prevent external import coupling.

## Event Sourcing Model

- **Source of truth:** Immutable event log on `WORKFLOW_HISTORY` stream (`history.{run_id}`)
- **KV snapshots:** Recovery convenience, not authoritative
- **Orchestrator:** Stateless — replays from stream on restart
- **Event types:** `workflow.started`, `workflow.completed`, `workflow.failed`, `workflow.cancelled`, `workflow.spawn`, `workflow.child.completed`, `workflow.child.failed`, `step.completed`, `step.failed`, `step.cancelled`, `step.continue`, `agent.loop.iteration`, `step.sleep.started`, `step.sleep.completed`, `step.wait.started`, `step.wait.matched`, `step.wait.timeout`, `step.map.started`, `step.map.completed`, `step.map.instance.completed`, `compensate.started`, `compensate.step.completed`, `compensate.failed`, `compensate.completed`

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
| SLEEP_TIMERS | `sleep.>`, `scheduled.>` | Durable timers via NakWithDelay (sleep, wait-timeout, rate-retry, scheduled runs) |

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
| scheduled_runs | One-shot scheduled workflow runs |
| workers | Worker directory (60s TTL heartbeat) |
| event_waiters | Wait-for-event correlation entries |
| rate_limits | Token bucket state per task type |
| debounce_state | Subject trigger debounce windows |
| batch_state | Trigger event batch accumulation (TTL: 2x max timeout) |
| idempotency_keys | Workflow dedup key→runID mapping (TTL: 24h default) |
| sticky_bindings | Run→worker affinity binding (TTL: workflow timeout + 1h) |

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
| Map | Fan-out over array: one task per item, fan-in on completion. Max 10,000 items. Fail-fast on any instance failure. `MapInstances` nested in step state (information hiding). |
| Sleep | Durable delay — engine handles via `SLEEP_TIMERS` NakWithDelay. No worker involved. Max 365 days. |
| WaitForEvent | Event correlation — blocks until matching external event arrives. In-memory waiter index via KV watch. Timeout via `SLEEP_TIMERS`. |

## Worker SDK

Three interfaces, split by concern. Handlers type-assert to optional capabilities.

**TaskContext (core):** Every handler receives this.
- `Input()`, `RunID()`, `StepID()`, `RetryCount()` — read-only context
- `Complete(output)`, `Fail(err)`, `FailPermanent(err)`, `FailRetryAfter(err, d)` — terminal actions
- `Continue(output)` — agent loop iteration
- `PutStream(data)` — real-time streaming via core pub/sub (`stream.{runID}.{stepID}`)
- `Heartbeat()` — extends AckWait via InProgress()

**Checkpointable:** Handlers that need state across retries type-assert to this.
- `Checkpoint(state)` / `LoadCheckpoint()` — KV persistence at `{runID}.{stepID}`
- `Pause(name, duration)` — checkpoint + NakWithDelay for mid-task durable delay

**Signaler:** Handlers that coordinate across steps type-assert to this.
- `WaitForSignal(name, timeout)` / `SendSignal(runID, name, data)` — KV watch-based

**Worker options:** `WithGroups(groups...)` for routing, `WithRateLimit` / `WithKeyedRateLimit` for KV token bucket rate limiting.

## Child Workflows

- `ParentRunID` + `ParentStepID` on WorkflowRun link parent to child
- Max nesting depth: 3 (enforced on spawn)
- Lifecycle: `workflow.spawn` → child runs → `workflow.child.completed`/`failed` back to parent
- Parent step blocks until child completes (KV watch pattern)

## Typed Workflow Generics (`dag/typed.go`)

- `WithSchemas[I, O](wf)` — generates JSON schemas from Go types via reflection
- `StartTyped[I](svc, ctx, name, input)` — type-safe workflow start with compile-time input checking
- Schema validation at `StartRun` (runtime) — rejects invalid input
- Flat struct schema generation only in v1 (no nested object recursion)
- Standalone functions, not a typed builder wrapper (Go generics limitation with interfaces)

## Competitive Context

Built to reach parity with Kestra, Hatchet, and Temporal. Key differentiators:
- NATS-native (no Postgres, no external queue)
- Event sourcing (not mutable row state)
- Agent loop as native primitive (not bolted on)
- Single-binary deployment target
