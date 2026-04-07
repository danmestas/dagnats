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
| `observe/` | OTel SDK bootstrap + NATS exporters | `InitTelemetry()`, `NATSHeaderCarrier`, `natsexporter/` |

**Public vs internal:** `dag/`, `protocol/`, `observe/` (OTel bootstrap + NATS exporters), `worker/`, `actor/`, `bridge/`, `sdk/`, `server/`, `cli/` are public API. Implementation packages live under `internal/` to prevent external import coupling.

## Event Sourcing Model

- **Source of truth:** Immutable event log on `WORKFLOW_HISTORY` stream (`history.{run_id}`)
- **KV snapshots:** Recovery convenience, not authoritative
- **Orchestrator:** Stateless — replays from stream on restart
- **Event types:** `workflow.started`, `workflow.completed`, `workflow.failed`, `workflow.cancelled`, `workflow.spawn`, `workflow.child.completed`, `workflow.child.failed`, `step.completed`, `step.failed`, `step.cancelled`, `step.continue`, `agent.loop.iteration`, `step.sleep.started`, `step.sleep.completed`, `step.wait.started`, `step.wait.matched`, `step.wait.timeout`, `step.map.started`, `step.map.completed`, `step.map.instance.completed`, `compensate.started`, `compensate.step.completed`, `compensate.failed`, `compensate.completed`

## NATS Primitives (Instead of Custom Infrastructure)

| Need | NATS Primitive |
|------|---------------|
| Task distribution | JetStream pull consumers + MaxAckPending |
| Atomic task fan-out | jetstreamext.PublishMsgBatch (orbit) |
| Worker affinity | pcgroups elastic consumer groups (orbit) |
| Singleton execution | pcgroups single-partition group (orbit) |
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

**Checkpointing** (methods on TaskContext directly):
- `Checkpoint(state)` / `LoadCheckpoint()` — KV persistence
- `Pause(name, duration)` — checkpoint + NakWithDelay

**Signals** (methods on TaskContext directly):
- `WaitForSignal(name, timeout)` / `SendSignal(runID, name, data)`

**Worker options:** `WithGroups(groups...)` for routing, `WithRateLimit` / `WithKeyedRateLimit` for KV token bucket rate limiting.

## Event-Based Cancellation

`CancelOn` declarations on `WorkflowDef` — "cancel this workflow if event X arrives." Reuses the `Correlator` with a `WaiterActionCancel` discriminator (same infrastructure as `WaitForEvent`).

**Flow:** On `workflow.started`, register one cancel waiter per `CancelOn` entry. On match: publish `workflow.cancelled` with triggering event as payload. All cancel waiters removed on any terminal state.

**Builder:** `wb.CancelOn(event, match)`, `wb.CancelOnWithTimeout(event, match, duration)`.

**Bounds:** Max 5 `CancelOn` entries per workflow. Timeout max 365 days. Cancel waiters share the 10,000-per-event-type bound with wait-for-event.

## Priority Queues

Numeric adjustment in seconds applied to a run's effective queue position. Computed from input data via dot-path + rules map.

```go
type PriorityConfig struct {
    Key           string         `json:"key"`            // dot-path into input
    Rules         map[string]int `json:"rules"`          // value -> offset seconds
    DefaultOffset int            `json:"default_offset"` // when no rule matches
}
```

`EffectiveTime() = CreatedAt - PriorityOffset`. Positive offsets advance the run, negative delay it. Range: [-600, +600] seconds. Only affects ordering when concurrency limits create backlogs. `--priority=N` CLI flag overrides expression evaluation.

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

## Dynamic DAG Generation (Planner Steps)

`StepTypePlanner` — a step whose output is a JSON DAG fragment. The engine materializes the fragment as runtime steps, then executes them.

**Flow:** planner step runs → returns `{steps: [...], edges: [...]}` → engine validates (bounds, cycles, ID collisions) → appends to `WorkflowRun.DynamicSteps` → `EffectiveSteps()` merges static + dynamic → normal execution resumes.

**Bounds:** 100 generated steps per planner, 500 total dynamic per run, 10 depth max. Generated step IDs namespaced: `{plannerStepID}.{generatedID}`. Output aggregation: 1 terminal = raw, N terminals = map.

**Event:** `EventPlannerMaterialized` records what was generated for observability.

## Human Approval Gates

`StepTypeApproval` — pauses execution until a human approves or rejects via HTTP endpoint or CLI.

**Token:** 256-bit cryptographically random, stored in `approval_tokens` KV (7-day TTL). Atomic consumption via CAS prevents double-approve.

**Endpoints:** `POST /runs/{id}/approval/{step_id}?action=approve&token={token}` and `?action=reject`. CLI: `dagnats run approve/reject <run-id> <step-id> --token=<token>`.

**Notification:** Published to `approval.{runID}.{stepID}` NATS subject for integrations (Slack, Discord, etc.). Timeout configurable (max 168h / 7 days) — auto-rejects on expiry.

## Per-Step Concurrency Limits

Two additional scopes beyond per-workflow `MaxRuns`:

**Per-task-type global:** `StepDef.MaxTaskConcurrency` — "at most N `call-claude` tasks across all runs." KV bucket: `concurrency_tasks` with CAS counters. Checked at task dispatch. If exhausted, retry via `SLEEP_TIMERS` with 1s delay.

**Per-run:** `ConcurrencyLimit.MaxSteps` — "at most N steps concurrent within this run." Enforced in `enqueueReady`. Builder: `WithConcurrency(maxRuns, maxSteps)`, `WithTaskConcurrency(max)`.

**Bounds:** 1-1000 per scope. CAS retry: 10 attempts.

## Non-Retriable Errors

`StepFailedPayload` wire protocol type with `FailureType` discriminator:

- `retriable` (default) — normal retry policy applies
- `non_retriable` — engine skips all retries, goes straight to on-failure/compensation/fail
- `retry_after` — engine schedules exact delay via `SLEEP_TIMERS` (`TimerActionRetryAfter`), bypassing backoff policy

Worker API: `FailPermanent(err)`, `FailRetryAfter(err, duration)`. Existing `Fail(err)` defaults to retriable. Backward compatible: old raw-string payloads parsed as retriable. `RetryAfter` clamped to [100ms, 1 hour].

HTTP bridge: `failure_type` and `retry_after_ms` fields on fail resolve action.

Observability: `step.failure.non_retriable` and `step.failure.retry_after` counters.

## Bulk Operations

**Bulk run** (`POST /runs/bulk`, `dagnats run bulk`): Start up to 1000 runs of the same workflow in one call. Def loaded once, schema parsed once. Atomic validation: first bad input fails the entire batch before any events publish. CLI supports positional JSON args and `--from-file=inputs.jsonl`.

**Bulk retry** (`POST /runs/retry`, `dagnats run retry-all`): Retry up to 1000 failed runs. Two modes:
- `rerun` — fresh start with original input (new run ID, clean state, uses current workflow def)
- `replay` — re-publish DLQ task messages to resume at failed step (existing run, limited by 30-day DLQ retention)

Both support `--dry-run`, time range filters (`--after`, `--before`). Sequential processing, 1000-run cap.

## Competitive Context

Built to reach parity with Kestra, Hatchet, and Temporal. Key differentiators:
- NATS-native (no Postgres, no external queue)
- Event sourcing (not mutable row state)
- Agent loop as native primitive (not bolted on)
- Single-binary deployment target
