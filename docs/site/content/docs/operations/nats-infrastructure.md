---
title: NATS Infrastructure
weight: 3
---

DagNats provisions all its NATS resources automatically on startup via `natsutil.SetupAll`.

## JetStream Streams

Seven streams handle all durable messaging. `dagnats serve` creates these on first boot; distributed deployments must ensure they exist before components start.

| Stream | Subjects | Retention | Storage | Limits | Purpose |
|--------|----------|-----------|---------|--------|---------|
| `WORKFLOW_HISTORY` | `history.>` | Limits | File | 5s dedup window | Immutable event log (source of truth) |
| `TASK_QUEUES` | `task.>` | WorkQueue | File | Atomic publish enabled | Work distribution to workers |
| `EVENTS` | `event.>` | Limits | File | | External event ingestion |
| `DEAD_LETTERS` | `dead.>` | Limits | File | 30-day retention | Permanent failures for inspection |
| `TELEMETRY` | `telemetry.>` | Limits | File | 7-day, 1 GiB max, 5s dedup | Spans, metrics, logs |
| `SLEEP_TIMERS` | `sleep.>`, `scheduled.>` | Limits | File | | Durable timers (sleep, timeout, rate-retry, scheduled runs) |
| `STICKY_TASKS` | `sticky.>` | Limits | Memory | 30-minute max age | Worker-affinity task routing |

### WORKFLOW_HISTORY

The event log. Every state change for every workflow run is an immutable event published to `history.{runID}`. The 5-second dedup window (via `Nats-Msg-Id`) prevents duplicate events during retries. The orchestrator replays this stream on startup to rebuild in-memory actor state.

### TASK_QUEUES

Work distribution. Uses **WorkQueuePolicy** so each message is delivered to exactly one consumer. Tasks are published to `task.{taskType}` and workers create pull consumers filtered to the task types they handle. Atomic publish is enabled for fan-out operations (map steps).

### SLEEP_TIMERS

A shared timer stream using an action discriminator in the message payload:

| Action | Fires After | Result |
|--------|-------------|--------|
| `sleep_complete` | Sleep duration | Publishes `step.sleep.completed` event |
| `wait_timeout` | Wait-for-event timeout | Publishes `step.wait.timeout` event |
| `rate_retry` | Rate limit refill delay | Re-publishes task to `task.>` |
| `debounce_fire` | Debounce window | Fires debounced trigger |
| `batch_fire` | Batch timeout | Fires accumulated batch |
| `retry_after` | Requested delay | Re-publishes task for retry |

All timers use NATS `NakWithDelay` -- the message is negatively acknowledged with a delay, and NATS redelivers it after the specified duration. No external timer service needed.

## KV Buckets

Fifteen KV buckets store workflow state, coordination data, and operational metadata.

| Bucket | TTL | History | Purpose |
|--------|-----|---------|---------|
| `workflow_defs` | -- | default | Immutable workflow definitions |
| `workflow_runs` | -- | default | Mutable run state snapshots |
| `scheduled_runs` | -- | default | One-shot scheduled workflow runs |
| `workers` | 60s | default | Worker directory (heartbeat) |
| `event_waiters` | -- | default | Wait-for-event correlation entries |
| `rate_limits` | -- | default | Token bucket state per task type |
| `concurrency_tasks` | -- | 1 | Per-task-type concurrency counters |
| `approval_tokens` | 7 days | 1 | Human approval gate tokens |
| `debounce_state` | 14 days | default | Subject trigger debounce windows |
| `idempotency_keys` | 24 hours | default | Workflow dedup key-to-runID mapping |
| `sticky_bindings` | ~25 hours | default | Run-to-worker affinity binding |
| `singleton_locks` | -- | default | Singleton execution locks |
| `checkpoints` | -- | default | Worker step state persistence |
| `signals` | -- | default | Cross-workflow KV-based signaling |
| `triggers` | -- | default | Trigger definitions |
| `trigger_state` | -- | default | Cron last-run timestamps |

### Workers Bucket

The `workers` bucket has a 60-second TTL. Workers re-PUT their entry every 30 seconds, so a single missed heartbeat is tolerated. The engine never reads this bucket -- it exists purely for observability (`dagnats workers list`). If the bucket is missing, workers function normally.

### Concurrency Buckets

`concurrency_tasks` uses `History: 1` to minimize storage for CAS-based counters. The engine checks these counters before dispatching tasks. If a limit is exhausted, the task is retried via `SLEEP_TIMERS` with a 1-second delay.

## Subject Hierarchy

All NATS subjects follow a dot-separated hierarchy. The `>` wildcard matches one or more tokens.

```
history.{runID}                    # Workflow events
task.{taskType}                    # Task distribution
event.{eventType}                  # External events
dead.{runID}.{stepID}              # Dead letter entries
telemetry.spans                    # Trace spans
telemetry.metrics                  # Metrics
telemetry.logs                     # Log records
sleep.{runID}.{stepID}             # Timer messages
scheduled.{workflowName}          # Scheduled run triggers
sticky.{workerID}.{taskType}      # Worker-affinity tasks
stream.{runID}.{stepID}           # Real-time step output streaming
approval.{runID}.{stepID}         # Approval notifications
```

The subject design ensures that consumers can filter to exactly the messages they need. A worker subscribing to `task.summarize` only receives summarize tasks. The orchestrator subscribing to `history.>` receives all workflow events.

## Resource Setup

On startup, `natsutil.SetupAll(nc)` calls:

1. `SetupStreams` -- creates the 5 core streams
2. `SetupKVBuckets` -- creates all KV buckets
3. `SetupTelemetryStream` -- creates the TELEMETRY stream
4. `SetupStickyStream` -- creates the STICKY_TASKS stream
5. `enableAtomicPublish` -- enables atomic batch publish on TASK_QUEUES

Each call uses `CreateOrUpdateStream`/`CreateOrUpdateKeyValue`, making them idempotent. Running `dagnats serve` multiple times against the same NATS data directory is safe.

All setup operations have a 30-second timeout. If NATS is unavailable, startup fails fast.
