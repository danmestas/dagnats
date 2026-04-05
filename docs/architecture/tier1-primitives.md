# Tier 1 Workflow Primitives

## Durable Sleep

**Two forms for two use cases:**

| Form | Location | Mechanism | Worker Involved |
|------|----------|-----------|-----------------|
| Step-level `Sleep("id", duration)` | DAG node | Engine publishes to `SLEEP_TIMERS`, consumer NAKs with delay, fires `step.sleep.completed` on redeliver | No |
| Worker-level `ctx.Pause("name", duration)` | Mid-task | Checkpoint + `NakWithDelay`, resume via checkpoint marker | Yes (same worker) |

**Step-level:** `StepTypeSleep` in DAG. Engine handles entirely — no task published. `SLEEP_TIMERS` stream shared with wait-for-event timeouts and rate-limit retries via action discriminator in message payload.

**Worker-level:** `paused` flag on `taskContext` prevents double-ack (Pause NAKs, handleMessage skips Ack). Attempt counter does not increment on resume.

**Bounds:** Max 365 days step-level. Warning logged >30 days. Worker-level limited by NATS AckWait.

## Wait-for-Event (Event Correlation)

**Distinct from signals.** Signals are push (sender targets a specific runID). Wait-for-event is pull (workflow declares what it's waiting for, correlator matches incoming events).

**Types split by phase:**
- `Match` (builder-time): `Left` and `Right` are dot-path strings
- `ResolvedMatch` (runtime): `Right` is a concrete value, resolved when waiter is created

**Correlator** runs inside the orchestrator (not a separate component). Maintains in-memory waiter index via KV watch on `event_waiters.>`. O(1) lookup per event type.

**Flow:** `ResolveReady()` returns wait step → engine resolves Match → writes `event_waiters` KV entry → subscribes to `EVENTS` stream → on match publishes `step.wait.matched` → on timeout publishes `step.wait.timeout` (timeout = `{"timeout": true}` output, not failure).

**Bounds:** 10,000 waiters per event type. Cancellation cleanup deletes waiter KV entries.

## Rate Limiting

**One mechanism** for both global and per-key: KV-backed token bucket.

- Global: key `{taskType}._global`
- Per-key: key `{taskType}.{keyValue}` extracted via dot-path expression

**Check happens in task dispatch path.** If exhausted: NAK via `SLEEP_TIMERS` with `rate_retry` action, which re-publishes the task after refill delay.

**CAS loop** bounded at 10 retries. KV entries auto-expire at 2x rate period.

**Config via StepRef:** `WithRateLimit(RateLimit{...})`, `WithKeyedRateLimit(KeyedRateLimit{...})`

## Worker Directory

**Observability-only.** Engine never reads it. Answers: "what workers are running, what can they do, are they healthy?"

- KV bucket: `workers` (60s TTL)
- Heartbeat: worker re-PUTs entry every 30s (survives one missed beat)
- Auto-register on `Worker.Start()`, deregister on `Stop()`
- CLI: `dagnats workers list`
- Graceful degradation: if KV bucket missing, worker functions normally

## HTTP-to-NATS Bridge

**Three deep endpoints** (Ousterhout: few endpoints, rich behavior):

| Endpoint | Method | Behavior |
|----------|--------|----------|
| `/v1/workers/connect` | POST | Register + SSE heartbeat stream |
| `/v1/tasks/poll` | POST | Long-poll NATS consumers, return task batch |
| `/v1/tasks/{id}/resolve` | POST | Action discriminator: complete, fail, pause, checkpoint, send_signal, wait_signal |

**Task identity:** `{runID}.{stepID}` compound key, included in poll response.

**Ack map:** In-memory `sync.Map` of `taskID → nats.Msg`. Bridge holds NATS ack until HTTP worker resolves. On bridge restart, in-flight tasks timeout via AckWait and NATS redelivers.

**Auth:** Bearer token via `DAGNATS_BRIDGE_TOKEN` env var. No token = allow all (dev mode).

**Mounted on `dagnats serve`** at `/v1/` on the existing HTTP mux. No separate port.

**Observability:** Spans, metrics (request count, poll duration, ackmap size), structured logging. Follows `api/service.go` pattern.

## Go HTTP Reference Client (`sdk/httpclient/`)

Reference implementation of the wire protocol for other language SDK authors.

```go
client := httpclient.New("http://localhost:8080", httpclient.WithToken("secret"))
client.Connect(ctx, reg)
tasks, _ := client.Poll(ctx, []string{"echo"}, 5, 30*time.Second)
client.Complete(ctx, tasks[0].TaskID, output)
client.Disconnect()
```

Methods: Connect, Disconnect, Poll, Complete, Fail, Pause, Checkpoint, SendSignal, WaitSignal.

Uses `protocol.TaskPayload`, `protocol.TaskResolution`, `worker.WorkerRegistration` directly — no type duplication.

## Dot-Path Extraction (`dag/dotpath.go`)

Shared utility for rate limit key extraction and event match resolution. Walks nested JSON via dot-separated path. Returns `any`. Panics on empty path, errors on missing keys.

## NATS Resources Added

| Resource | Type | Purpose |
|----------|------|---------|
| `workers` | KV (60s TTL) | Worker directory |
| `event_waiters` | KV | Wait-for-event waiter entries |
| `rate_limits` | KV | Token bucket state |

`SLEEP_TIMERS` stream already existed (scheduled runs). Extended with `sleep.>` subjects for sleep/timeout/rate-retry timers.

## Timer Message Format

All timer actions share `SLEEP_TIMERS` via action discriminator:

| Action | Fires | Result |
|--------|-------|--------|
| `sleep_complete` | After sleep duration | Publishes `step.sleep.completed` |
| `wait_timeout` | After wait-for-event timeout | Publishes `step.wait.timeout` |
| `rate_retry` | After rate limit refill delay | Re-publishes task to `task.>` |
| `debounce_fire` | After debounce window | Fires debounced trigger |
| `batch_fire` | After batch timeout | Fires accumulated batch |
