# Tier 1: Workflow Primitives Design Spec

**Date:** 2026-04-04
**Status:** Draft
**Scope:** Durable sleep, wait-for-event, rate limiting, worker registration protocol, HTTP-to-NATS bridge

## Context

DagNats has a solid foundation: DAG execution, retries, per-workflow concurrency, cancellation, signals, worker groups, checkpointing, triggers, and dead-letter queues. Comparing against Inngest and Hatchet reveals five categories of missing primitives that would bring DagNats to feature parity with modern workflow engines while staying NATS-native.

This is Tier 1 of a three-tier plan:

- **Tier 1** (this spec): Durable sleep, wait-for-event, rate limiting, worker registration, HTTP bridge
- **Tier 2** (future): Debounce, event batching, idempotency by expression, typed workflow generics
- **Tier 3** (future): Sticky workers, OnFailure hook execution, parallel fan-out primitive, per-step concurrency

## Design Principles

All decisions in this spec are guided by:

- **Ousterhout:** Deep modules with small interfaces. Pull complexity downward. Define errors out of existence.
- **TigerStyle:** Safety > Performance > DX. Assertions as contracts. Bounded everything.
- **NATS-native:** Use NATS primitives (NakWithDelay, KV, consumer config) instead of custom infrastructure.
- **Capability parity:** Every feature available to embedded Go workers must be available to remote workers through the HTTP bridge. No feature ships without both NATS-native and HTTP bridge support.

## 1. Worker Directory (Registration Protocol)

### Problem

Workers are currently invisible. A Go process subscribes to `task.{type}` subjects and pulls work, but there is no registry of what workers exist, what they handle, or whether they're alive. Distributed polyglot workers need explicit registration for operational visibility.

### Design

The worker directory is an **observability feature, not a control plane feature**. The engine never reads it for dispatch decisions. NATS consumer groups handle load balancing. The directory exists so operators can answer: "what workers are running, what can they do, are they healthy?"

#### Registration Flow

1. Worker starts, connects to NATS (directly or via HTTP bridge)
2. Worker PUTs a `WorkerRegistration` entry to the `workers` KV bucket at key `{worker_id}`:
   ```json
   {
     "worker_id":    "worker-abc123",
     "task_types":   ["send-email", "process-payment"],
     "language":     "go",
     "transport":    "nats",
     "max_tasks":    10,
     "metadata":     {"region": "us-east-1"}
   }
   ```
3. The `workers` KV bucket has a TTL (e.g., 60 seconds)
4. Worker periodically re-PUTs its registration entry to refresh the TTL (heartbeat)
5. If the worker dies, the KV entry expires — worker is implicitly gone
6. In-flight tasks on dead workers eventually NAK via AckWait and get redelivered

#### Deregistration

Worker DELETEs its KV entry on graceful shutdown. Ungraceful shutdown handled by TTL expiry. No separate heartbeat subjects needed — KV TTL refresh is the heartbeat mechanism.

#### Key Constraint

The engine **never** reads the `workers` KV bucket. It is consumed by:
- `dagnats workers list` CLI command
- API endpoints for dashboard/monitoring
- Future alerting on worker health

#### NATS Resources

- KV bucket: `workers` (TTL-based entries, keyed by `{worker_id}`)

## 2. Durable Sleep

Two distinct primitives for two distinct use cases.

### 2a. Step-Level Sleep (DAG Node)

A delay between tasks. No worker involvement. The engine handles it entirely.

#### Builder API

```go
NewWorkflow("onboarding").
    Task("send-welcome", "send-email").
    Sleep("wait-3-days", 72*time.Hour).DependsOn("send-welcome").
    Task("send-followup", "send-email").DependsOn("wait-3-days").
    Build()
```

#### New Step Type

`StepTypeSleep` added to `dag/types.go`. The `StepDef` gains a `Duration time.Duration` field, populated only for sleep steps.

#### Engine Behavior

1. `ResolveReady()` returns a sleep step
2. Engine does NOT publish to `task.>` — no worker needed
3. Engine writes a `step.sleep.started` event with a `wake_at` timestamp to `history.{runID}`
4. Engine publishes a timer message to a `SLEEP_TIMERS` stream at subject `sleep.{runID}.{stepID}`
5. A dedicated pull consumer on `SLEEP_TIMERS` fetches each timer message immediately and NAKs it with `NakWithDelay(duration)` — this defers redelivery by the exact sleep duration per-message
6. On redelivery, the consumer reads the `action` field from the message payload and dispatches accordingly, then ACKs
7. Orchestrator processes the resulting event like any `step.completed` — resolves ready downstream steps

This approach is fully NATS-native: `NakWithDelay` provides per-message delay without custom timers.

#### Timer Message Format

The `SLEEP_TIMERS` stream is a shared timer facility used by durable sleep, wait-for-event timeouts, and rate-limit retries. Each timer message includes an `action` field that tells the consumer what to do on fire:

```json
{"action": "sleep_complete", "run_id": "...", "step_id": "...", "duration_ms": 100}
{"action": "wait_timeout", "run_id": "...", "step_id": "..."}
{"action": "rate_retry", "run_id": "...", "step_id": "...", "task_type": "...", "input": "..."}
```

The consumer dispatches on `action`:
- `sleep_complete`: publish `step.sleep.completed` to `history.{runID}`
- `wait_timeout`: publish `step.wait.timeout` to `history.{runID}` (ignored if step already matched)
- `rate_retry`: re-publish the task to `task.{taskType}.{runID}` (re-enters the dispatch path)

This makes the timer a deep module — single interface (`Schedule`), multiple behaviors selected by payload. Callers don't need to know about each other.

#### Persistence

The `wake_at` timestamp is stored in `StepState`. If the engine restarts, any pending timer messages in `SLEEP_TIMERS` will redeliver naturally — NATS handles persistence. The engine recomputes any missing timers from workflow run snapshots as a recovery fallback.

#### Validation

- Duration must be positive and finite (checked at `Build()` time)
- Maximum sleep duration of 365 days — prevents overflow/programmer errors while allowing all legitimate business use cases
- Warning logged if sleep exceeds 30 days

### 2b. Worker-Level Pause (Mid-Task)

A pause within a task. Uses checkpoint + NAK.

#### TaskContext API

```go
// Pause checkpoints state, releases the message, and resumes after duration.
// Name must be unique within the task for idempotent resume.
Pause(name string, duration time.Duration) error
```

Named `Pause` (not `Sleep`) to signal that this is a different concept from the DAG-level `Sleep`.

#### Behavior

1. Worker calls `ctx.Pause("wait-for-propagation", 30*time.Second)`
2. Worker checkpoints current state to KV at `checkpoints.{runID}.{stepID}`
3. Checkpoint includes a `pause_resume` marker with the pause name
4. Worker NAKs the message with `NakWithDelay(duration)`
5. Message redelivers after delay
6. Worker loads checkpoint, detects `pause_resume` marker, resumes after the pause call
7. Attempt counter does NOT increment on pause resume

#### Engine Isolation

The engine is not involved in worker-level pause. The NAK/redeliver cycle is invisible to the orchestrator — the step remains in `StepStatusRunning` throughout. The worker alone manages resume via checkpoint markers. No new step status or engine event is needed.

#### Validation

- Duration must be positive and finite
- No hard maximum, but practically limited by NATS AckWait mechanics
- For long waits (hours+), prefer step-level `Sleep` in the DAG

## 3. Wait-for-Event (Event Correlation)

### Problem

Current signals are direct: sender calls `SendSignal(runID, name, data)` targeting a specific run. Wait-for-event matches any incoming event by type and correlation criteria, without the sender knowing about running workflows.

### Design

A new step type `StepTypeWaitForEvent` that pauses a workflow run until a matching external event arrives on the `EVENTS` stream.

#### Builder API

```go
NewWorkflow("order-fulfillment").
    Task("create-order", "create-order").
    WaitForEvent("payment-received", WaitForEventOpts{
        Event:   "payment.completed",
        Match: Match{
            Left:  "event.data.order_id",
            Op:    "eq",
            Right: "step.create-order.output.order_id",
        },
        Timeout: 48 * time.Hour,
    }).DependsOn("create-order").
    Task("ship-order", "ship-order").DependsOn("payment-received").
    Build()
```

#### Match Expression

Two types for two phases. Builder-time `Match` uses dot-paths. Runtime `ResolvedMatch` uses concrete values. This eliminates ambiguity — the types enforce which phase you're in.

```go
// Match is the builder-time type. Both sides are dot-path strings.
type Match struct {
    Left  string   // dot-path: "event.data.X"
    Op    MatchOp  // "eq" to start; extensible later
    Right string   // dot-path: "step.{id}.output.Y" or "input.Z"
}

// ResolvedMatch is the runtime type stored in KV waiter entries.
// Right is resolved to a concrete value when the waiter is created.
type ResolvedMatch struct {
    Left  string   // dot-path evaluated against incoming event
    Op    MatchOp
    Right any      // concrete value (e.g., "ord-123")
}
```

Dot-paths are evaluated against:
- `event.*` — the incoming external event (Left side, evaluated at match time)
- `step.{id}.output.*` — completed step outputs from the current run (Right side, resolved at waiter creation)
- `input.*` — the workflow run's input (Right side, resolved at waiter creation)

#### Engine Behavior

1. `ResolveReady()` returns a wait-for-event step
2. Engine writes `step.wait.started` event with match criteria and timeout
3. Engine creates KV entry at `event_waiters.{event_type}.{runID}.{stepID}` containing the resolved match expression (right-side path evaluated against current step outputs and stored as a concrete value)

#### Event Correlation (Inside the Engine)

The correlator runs as a function within the orchestrator, not a separate component. This avoids a shallow module with its own lifecycle.

The correlator maintains an **in-memory waiter index** populated by a KV watch on `event_waiters.>`. This is the same pattern used by the existing signal system. The index is rebuilt on startup from current KV state.

1. Orchestrator subscribes to `EVENTS` stream via pull consumer
2. KV watch on `event_waiters.>` keeps the in-memory index up to date (reactive, not polled)
3. For each incoming event, the correlator looks up `event_type` in the in-memory index — O(1) per event type, then iterates only the waiters for that type
4. Evaluates match: extract left-side value from event, compare to stored right-side value
5. On match: publish `step.wait.matched` to `history.{runID}` with the matched event data as step output. Delete waiter KV entry (watch updates the index).
6. On timeout: publish `step.wait.timeout` to `history.{runID}`. Step completes with a timeout status. Downstream steps can branch on this.

#### Timeout Handling

Same mechanism as durable sleep: `NakWithDelay` on a timer message in the `SLEEP_TIMERS` stream. If a match arrives before timeout, the timeout message is ignored (idempotent — step already completed).

#### Cancellation Cleanup

When a workflow is cancelled, the engine deletes any active `event_waiters` entries for that run. The KV watch updates the in-memory index automatically.

#### Bounded

Maximum 10,000 active waiters per event type (configurable). Rejects new waiters beyond this with an error on the workflow run.

#### Coexistence with Signals

Signals remain unchanged. They are direct, targeted inter-workflow communication. Wait-for-event is pattern-matched external event correlation. Different tools for different problems.

#### NATS Resources

- KV bucket: `event_waiters` (entries keyed by `{event_type}.{runID}.{stepID}`)
- Existing stream: `EVENTS` (already exists for triggers)
- Existing stream: `WORKFLOW_HISTORY` (for match/timeout events)

## 4. Rate Limiting

One mechanism — KV-backed token bucket — for both global and per-key rate limiting. This is simpler than two mechanisms (Ousterhout: fewer concepts). NATS consumer `RateLimit` is bytes-per-second throughput control, not requests-per-period — it does not serve our use case.

### 4a. Global Per-Task-Type Rate Limiting

A single token bucket per task type.

#### Configuration

```go
NewWorkflow("notifications").
    Task("send-sms", "send-sms", WithRateLimit(RateLimit{
        Limit:  100,
        Period: time.Minute,
    })).
    Build()
```

#### Implementation

Same KV token bucket as per-key (below), with a fixed key of `_global` per task type. KV entry at `rate_limits.send-sms._global`.

### 4b. Per-Key Rate Limiting

For multi-tenant or per-resource throttling.

#### Configuration

```go
NewWorkflow("api-calls").
    Task("call-api", "call-api", WithKeyedRateLimit(KeyedRateLimit{
        Key:    "input.tenant_id",
        Limit:  10,
        Period: time.Minute,
        Units:  1,
    })).
    Build()
```

### Token Bucket Implementation (Shared)

1. Engine evaluates the key dot-path (or uses `_global`) to get a concrete key value
2. Checks KV bucket `rate_limits.{task_type}.{key_value}` for token bucket state:
   ```json
   {
     "tokens":      7,
     "last_refill": "2026-04-04T10:00:00Z",
     "limit":       10,
     "period_ms":   60000
   }
   ```
3. Refills tokens based on elapsed time since `last_refill`
4. If tokens available: decrement, update KV (optimistic locking via revision), dispatch task
5. If exhausted: NAK the task message with `NakWithDelay` calculated from when the next token refills. The message redelivers when tokens are available.
6. CAS loop bounded at 10 retries for concurrent decrements

#### Bounded

Maximum 100,000 distinct keys per task type. KV entries expire after 2x the rate period (auto-cleanup of inactive keys).

#### NATS Resources

- KV bucket: `rate_limits` (entries keyed by `{task_type}.{key_value}`, TTL = 2x period)

## 5. HTTP-to-NATS Bridge

### Problem

Non-Go workers need HTTP access to the task system. The bridge translates HTTP into NATS operations.

### Design

A gateway service with three deep endpoints. Complexity is pulled downward — the bridge handles all NATS mechanics internally.

#### Endpoints

```
POST /v1/workers/connect     — register + start heartbeat (SSE stream)
POST /v1/tasks/poll          — long-poll for tasks
POST /v1/tasks/{id}/resolve  — complete, fail, pause, or checkpoint
```

#### POST /v1/workers/connect

Request:
```json
{
  "worker_id":  "worker-abc123",
  "task_types": ["send-email", "process-payment"],
  "max_tasks":  10
}
```

Response: SSE stream. Bridge sends periodic heartbeat-ack events. If the SSE connection drops, bridge deregisters the worker after a grace period. The bridge registers on behalf of the HTTP worker in the `workers` KV bucket and proxies heartbeats automatically.

#### POST /v1/tasks/poll

Request:
```json
{
  "task_types": ["send-email"],
  "max_tasks":  5,
  "timeout_ms": 30000
}
```

Response: array of task payloads, or empty array after timeout. Bridge pulls from NATS consumers for those task types internally.

#### POST /v1/tasks/{id}/resolve

Single deep endpoint with an action discriminator. Replaces six shallow endpoints.

```json
{"action": "complete", "output": {"sent": true}}
```
```json
{"action": "fail", "error": "SMTP timeout"}
```
```json
{"action": "pause", "name": "wait-propagation", "duration_ms": 30000, "checkpoint": {...}}
```
```json
{"action": "checkpoint", "data": {"progress": 42}}
```

The bridge translates each action to the appropriate NATS operation (Ack, Nak, InProgress, checkpoint KV write, NakWithDelay).

#### Task Identity

The `{id}` in `/v1/tasks/{id}/resolve` is the compound key `{runID}.{stepID}`, which uniquely identifies a task execution. This ID is included in the poll response payload alongside the task data. Workers use it for all subsequent task operations.

#### Bridge Internals

- In-memory map of `task_id ({runID}.{stepID}) -> nats.Msg` for pending acks
- Proxies task heartbeats as `InProgress()` calls on the NATS message
- Stateless beyond the ack map — multiple instances behind a load balancer with session affinity
- Workers can only poll for task types they registered for
- On bridge restart, in-flight tasks become unresolvable — they time out via `AckWait` and NATS redelivers them to healthy workers. This is the same failure mode as a NATS worker crashing.

#### Authentication

Credentials validated on `/connect`. Scoped to registered task types.

#### NATS Resources

- No new resources. Bridge uses existing `task.>` consumers and KV buckets.

## 6. Changes to Existing Code

### dag/types.go
- New step types: `StepTypeSleep`, `StepTypeWaitForEvent`
- New fields on `StepDef`: `Duration time.Duration`, `WaitForEvent *WaitForEventOpts`
- New type: `Match{Left, Op, Right}`, `WaitForEventOpts{Event, Match, Timeout}`
- New fields on `StepDef` for rate limiting: `RateLimit *RateLimit`, `KeyedRateLimit *KeyedRateLimit`

### dag/builder.go
- New builder methods: `Sleep(id, duration)`, `WaitForEvent(id, opts)`
- New step options: `WithRateLimit(...)`, `WithKeyedRateLimit(...)`

### dag/validation.go
- Validate sleep duration is positive, finite, and <= 365 days
- Validate wait-for-event has event type, match, and timeout
- Validate rate limit period is positive
- Validate `step.*` dot-paths in match expressions reference valid step IDs declared in the DAG
- `event.*` and `input.*` dot-paths are not validated at build time (event shape is unknown until runtime)

### engine/orchestrator.go
- New event handlers: `step.sleep.started`, `step.sleep.completed`, `step.wait.started`, `step.wait.matched`, `step.wait.timeout`
- Sleep timer consumer: fetch from `SLEEP_TIMERS`, NAK with delay, publish completion on redeliver
- Event correlation: KV watch on `event_waiters.>` maintains in-memory waiter index, subscribe to `EVENTS` stream, match incoming events against index
- Rate limit check in task dispatch path (KV token bucket, shared for global and per-key)
- Cancellation cleanup: delete `event_waiters` entries for cancelled runs

### worker/context.go
- New method: `Pause(name string, duration time.Duration) error`
- Checkpoint includes `pause_resume` marker

### protocol/events.go
- New event types: `EventStepSleepStarted`, `EventStepSleepCompleted`, `EventStepWaitStarted`, `EventStepWaitMatched`, `EventStepWaitTimeout`

### natsutil/setup.go
- New KV buckets: `workers`, `event_waiters`, `rate_limits`
- New stream: `SLEEP_TIMERS`

### protocol/protocol.go
- Add `TaskID string` field to `TaskPayload` (canonical identity: `{runID}.{stepID}`)
- New `TaskResolution` struct for HTTP bridge resolve actions
- New `TimerMessage` struct with `Action` field for SLEEP_TIMERS dispatch

### New packages
- `bridge/` — HTTP-to-NATS gateway service

### Removed from original plan
- `sdk/` package — not needed for Tier 1. The bridge imports `protocol.TaskPayload` and `worker.WorkerRegistration` directly. No duplicate types. Wire protocol documentation references existing types as the canonical format. An `sdk/` package is warranted when there is SDK *behavior* to encapsulate (Tier 2+), not just re-exported types.

## 7. Build Order

1. **Worker registration protocol + KV bucket** — foundation for visibility
2. **Durable sleep (step-level)** — engine-only, no SDK changes
3. **Durable sleep (worker-level Pause)** — extend TaskContext
4. **Rate limiting (global)** — KV token bucket, single key per task type
5. **Rate limiting (per-key)** — KV token bucket, dynamic key per task type
6. **Wait-for-event** — new step type + correlator in engine
7. **HTTP-to-NATS bridge** — gateway service
8. **Wire protocol documentation** — reference doc for other language SDK authors

Each step is independently shippable and testable. Later steps build on earlier ones but don't invalidate them.

**Note on capability parity:** Steps 2-6 ship for Go/embedded workers before the HTTP bridge (step 7). This is a pragmatic concession — the bridge cannot exist before the primitives it bridges. Once the bridge ships, it must support all primitives from day one. No subsequent primitive ships without bridge support in the same change.

## 8. Bridge Completion (Post-Implementation Additions)

The following items were identified during implementation as necessary to fulfill the capability parity principle. They are small, tactical additions — not new primitives.

### 8a. Bridge Integration with `dagnats serve`

The bridge must be startable as part of the all-in-one server lifecycle, not just as a standalone package.

**Design:** Add the bridge to `server/server.go:startComponents()` after the orchestrator and trigger service are started. The bridge mounts on the existing HTTP mux at `/v1/` prefix, sharing the same `http.Server` as the REST API and webhook handler.

```
mux.Handle("/v1/", bridge.Handler())
```

The bridge is created with the server's `*nats.Conn` and `nats.JetStreamContext`. It starts automatically when the server starts. No separate process or port needed.

**Configuration:** A new config key `bridge_enabled` (default: `true` when `dagnats serve` runs). The `DAGNATS_BRIDGE_TOKEN` env var controls auth as already implemented.

**Shutdown:** The bridge has no persistent state beyond the in-memory ack map. HTTP server shutdown (existing 15s deadline) handles in-flight requests. Pending ack map entries expire via NATS AckWait.

### 8b. Signal Support in Bridge

The existing `TaskContext` interface exposes `WaitForSignal(name, timeout)` and `SendSignal(runID, name, data)`. These are available to embedded Go workers but not through the HTTP bridge — a capability parity violation.

**Design:** Add two new actions to the resolve endpoint's action discriminator:

```json
{"action": "wait_signal", "name": "approval", "timeout_ms": 60000}
{"action": "send_signal", "run_id": "run-xyz", "name": "approval", "data": {"approved": true}}
```

**`wait_signal` behavior:**
1. Bridge reads the `signals` KV bucket for key `signals.{runID}.{name}` (using the task's runID from the ack map)
2. If signal already present: return the signal data immediately as JSON response
3. If not present: start a KV watch with the specified timeout
4. On signal arrival: return signal data as JSON response
5. On timeout: return HTTP 408 (Request Timeout)
6. The NATS message stays in-flight (InProgress called periodically during the watch)

**`send_signal` behavior:**
1. Bridge writes to `signals.{runID}.{name}` KV bucket with the provided data
2. Returns HTTP 200

**Important:** `wait_signal` is a blocking HTTP call (long-poll pattern, same as task poll). The bridge calls `msg.InProgress()` periodically during the wait to prevent AckWait expiry.

### 8c. Bridge Observability

The bridge currently has no telemetry instrumentation. All other DagNats components (api, engine, worker) have provider-agnostic observability via `observe.Telemetry`.

**Design:** Add to `Bridge` struct:
- `tel *observe.Telemetry` field (passed via constructor)
- Spans on each endpoint: `bridge.connect`, `bridge.poll`, `bridge.resolve`
- Metrics: `bridge_requests_total` (counter by endpoint), `bridge_poll_duration_ms` (histogram), `bridge_ackmap_size` (gauge)
- Structured logging on connect/disconnect, poll (empty vs tasks returned), resolve (action type)

Follow the exact pattern from `api/service.go` — instrumented wrapper method calling inner logic method.

### 8d. Go HTTP Reference Client

A Go package that implements the bridge's HTTP protocol, validating the wire format end-to-end and serving as a template for other language SDKs.

**Design:** Create `sdk/httpclient/` package:

```go
package httpclient

// Client implements the DagNats worker protocol over HTTP.
type Client struct {
    baseURL string
    token   string
    http    *http.Client
}

func New(baseURL, token string) *Client
func (c *Client) Connect(ctx context.Context, reg WorkerRegistration) error
func (c *Client) Poll(ctx context.Context, taskTypes []string, maxTasks int, timeout time.Duration) ([]TaskPayload, error)
func (c *Client) Complete(ctx context.Context, taskID string, output json.RawMessage) error
func (c *Client) Fail(ctx context.Context, taskID string, errMsg string) error
func (c *Client) Pause(ctx context.Context, taskID string, name string, duration time.Duration, checkpoint json.RawMessage) error
func (c *Client) Checkpoint(ctx context.Context, taskID string, data json.RawMessage) error
func (c *Client) WaitSignal(ctx context.Context, taskID string, name string, timeout time.Duration) (json.RawMessage, error)
func (c *Client) SendSignal(ctx context.Context, taskID string, runID string, name string, data json.RawMessage) error
```

The client uses `protocol.TaskPayload`, `protocol.TaskResolution`, and `worker.WorkerRegistration` types directly — no duplication. It handles SSE heartbeat parsing in `Connect()` via a background goroutine.

**Test:** An E2E test that starts a full DagNats server, connects via the HTTP client, polls a task, completes it, and verifies workflow completion — proving the protocol works end-to-end through the Go reference implementation.

## 9. Build Order (Updated)

Previous build order items 1-8 are complete. The following extend Tier 1:

9. **Bridge integration with `dagnats serve`** — mount on existing HTTP mux
10. **Signal support in bridge** — two new resolve actions
11. **Bridge observability** — spans, metrics, structured logging
12. **Go HTTP reference client** — validates wire protocol end-to-end

Each is independently shippable. Items 9-10 are highest priority (capability parity). Item 11 is operational hygiene. Item 12 validates the protocol for SDK authors.

## 10. Out of Scope (Unchanged)

- Debounce and event batching (Tier 2)
- Idempotency by expression (Tier 2)
- ~~Typed workflow generics~~ (landed on main)
- Sticky workers / worker affinity (Tier 3)
- ~~OnFailure hook execution~~ (landed on main)
- ~~Parallel fan-out (Map step)~~ (landed on main)
- Per-step concurrency limits (Tier 3)
- Non-Go SDK implementations (protocol ships in Tier 1, SDKs follow)
- Expression language beyond dot-path + equality
- `StepDef` refactoring to type-specific config variants (now more urgent — StepDef has 14+ fields after Map, OnFailure, Compensate, and Tier 1 additions)
