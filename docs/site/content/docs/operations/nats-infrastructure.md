---
title: NATS Infrastructure
weight: 3
---

DagNats provisions all its NATS resources automatically on startup via `natsutil.SetupAll`. In a cluster every stream and bucket below is created at the cluster's replication factor, except the buckets listed under [Buckets outside SetupAll](#buckets-outside-setupall).

## JetStream Streams

Ten streams handle all durable messaging. `dagnats serve` creates these on first boot; distributed deployments must ensure they exist before components start.

| Stream | Subjects | Retention | Storage | Limits | Purpose |
|--------|----------|-----------|---------|--------|---------|
| `WORKFLOW_HISTORY` | `history.>` | Limits | File | 30-day max age, 5s dedup window | Immutable event log (source of truth) |
| `TASK_QUEUES` | `task.>` | WorkQueue | File | No max age, atomic publish, direct get | Work distribution to workers |
| `EVENTS` | `event.>` | Limits | File | 14-day max age | External event ingestion |
| `DEAD_LETTERS` | `dead.>` | Limits | File | 30-day max age, 24h dedup window | Permanent failures for inspection |
| `TELEMETRY` | `telemetry.>` | Limits | File | 7-day max age, 5s dedup window | Spans, metrics, logs |
| `SLEEP_TIMERS` | `sleep.>`, `scheduled.>` | Limits | File | No max age | Durable timers (sleep, timeout, rate-retry, scheduled runs) |
| `STICKY_TASKS` | `sticky.>` | Limits | Memory | 30-minute max age | Worker-affinity task routing |
| `TRIGGER_HISTORY` | `trigger.fire.>` | Limits | File | 30-day max age | Trigger fire records (`dagnats trigger history`) |
| `BUILD_LOGS` | `logs.>` | Limits | File | `DAGNATS_BUILD_LOGS_TTL`, default 7 days (1h to 8760h); 2-minute dedup window | Task log lines (`logs.{run}.{step}.{attempt}.{iteration}`) |
| `AGENT_JOURNAL` | `journal.>` | Limits | File | 30-day max age, 2-minute dedup window, direct get off | Agent journal records (`journal.{run}.{step}`), one fenced subject per step |

`TASK_QUEUES` and `SLEEP_TIMERS` have no max age because their messages are live work: an age limit would silently delete pending tasks and unfired timers. Every file-backed stream also gets a byte ceiling that is a share of the JetStream store budget (`max_store_bytes`): `WORKFLOW_HISTORY` 25%, `EVENTS` 15%, `TASK_QUEUES` 12%, `TELEMETRY` 10%, `BUILD_LOGS` 5%, `DEAD_LETTERS` 5%, `AGENT_JOURNAL` 4%, `TRIGGER_HISTORY` 4% and `SLEEP_TIMERS` 4%, plus 0.5% for the `submissions` KV bucket, 84.5% in all, with the rest left as headroom for the other KV buckets and JetStream's own index. `AGENT_JOURNAL` is sized small on purpose: one record per claim, pause, entry and commit per step, aged out at 30 days like history. `STICKY_TASKS` is in memory and bounded by its age.

### WORKFLOW_HISTORY

The event log. Every state change for every workflow run is an immutable event published to `history.{runID}`. The 5-second dedup window (via `Nats-Msg-Id`) prevents duplicate events during retries. The orchestrator consumes it through a durable consumer; current run state is kept as snapshots in the `workflow_runs` KV bucket. In a cluster, every node's engine consumes the same history stream, so snapshots are read from the stream leader and each save is a compare-and-set on the revision that was read. A save that loses the race is handled again from a fresh read.

### TASK_QUEUES

Work distribution. Uses **WorkQueuePolicy** so each message is delivered to exactly one consumer. Tasks are published to `task.{taskType}` and workers create pull consumers filtered to the task types they handle. Atomic publish is enabled for fan-out operations (map steps).

### SLEEP_TIMERS

A shared timer stream using an action discriminator in the message payload:

| Action | Fires After | Result |
|--------|-------------|--------|
| `sleep_complete` | Sleep duration | Publishes `step.sleep.completed` event |
| `wait_timeout` | Wait-for-event timeout | Publishes `step.wait.timeout` event |
| `rate_retry` | Rate limit refill delay | Re-publishes task to `task.>` |
| `task_concurrency_retry` | Concurrency slot backoff | Re-publishes a task that found its concurrency limit full |
| `debounce_fire` | Debounce window | Fires debounced trigger |
| `retry_after` | Delay a worker requested | Dispatches the next attempt |
| `retry_backoff` | Retry policy backoff | Dispatches the next attempt |
| `step_timeout` | Step timeout | Fails a still-running attempt as retriable (stale timers from an earlier attempt are ignored) |
| `approval_timeout` | Approval gate timeout | Expires the approval |

All timers use NATS `NakWithDelay` -- the message is negatively acknowledged with a delay, and NATS redelivers it after the specified duration. No external timer service needed.

## KV Buckets

These KV buckets store workflow state, coordination data, and operational metadata.

| Bucket | TTL | History | Purpose |
|--------|-----|---------|---------|
| `workflow_defs` | -- | default | Immutable workflow definitions |
| `workflow_runs` | -- | default | Mutable run state snapshots |
| `scheduled_runs` | -- | default | One-shot scheduled workflow runs |
| `workers` | 60s | default | Worker directory (heartbeat) |
| `event_waiters` | -- | default | Wait-for-event correlation entries |
| `rate_limits` | -- | default | Token bucket state per task type |
| `concurrency_runs` | -- | default | Per-workflow sets of runs holding a concurrency slot |
| `concurrency_tasks` | -- | 1 | Per-task-type concurrency slot holders |
| `approval_tokens` | 7 days | 1 | Human approval gate tokens |
| `debounce_state` | 14 days | default | Subject trigger debounce windows |
| `idempotency_keys` | 24 hours | default | Workflow dedup key-to-runID mapping |
| `sticky_bindings` | ~25 hours | default | Run-to-worker affinity binding |
| `singleton_locks` | -- | default | Singleton execution locks |
| `checkpoints` | 30 days | default | Resume markers written by `Pause()`, and loop state for workers on an older engine |
| `signals` | -- | default | Cross-workflow KV-based signaling |
| `triggers` | -- | default | Trigger definitions |
| `trigger_state` | -- | default | Cron last-run timestamps |
| `http_idempotency` | 1 hour | default | Cached responses for HTTP trigger idempotency keys |
| `trigger_types` | -- | 1 | External trigger type schemas |
| `services` | -- | 1 | Registered service metadata |
| `worker_tokens` | -- | 1 | Minted worker tokens (revoked records kept for audit) |
| `worker_status` | 120s | default | Per-worker counter snapshots for `dagnats status` |
| `console_audit` | 90 days | default | Audit log of console write actions |
| `leases` | per key (1s-10m) | 1 | Coordination leases with revision fencing ([ADR-024](https://github.com/danmestas/dagnats/blob/main/docs/architecture/adr-024-leases.md)); changes republished on `dagnats.lease.changed.<key>` |
| `submissions` | 7 days | 1 | Inputs submitted to running agent steps, keyed `{run}.{step}.{request_id}` ([ADR-027](https://github.com/danmestas/dagnats/blob/main/docs/architecture/adr-027-agent-inputs-and-waits.md)); values at most 256 KiB, the one bucket with a byte ceiling (0.5% of the store budget) |

History `default` is the JetStream KV default of 1 revision. `console_audit` is created by `dagnats serve` for the console and control-plane audit log; every other bucket comes from `SetupAll`, so `dagnats-engine` and `dagnats-api` create them too.

### Replication

Every bucket, `console_audit` included, uses the cluster's replication factor, and all of them appear in the `/health/cluster` report (`console_audit` only on a node that has created it). Through 0.0.27 `triggers`, `trigger_state`, `signals`, `checkpoints`, `concurrency_runs` and `console_audit` were single-replica in a cluster; an existing deployment is upgraded in place on restart. See [Clustering]({{< ref "/docs/operations/clustering" >}}).

### Workers Bucket

The `workers` bucket has a 60-second TTL. Workers re-PUT their entry every 30 seconds, so a single missed heartbeat is tolerated. The engine never reads this bucket -- it exists purely for observability (`dagnats workers list`). If the bucket is missing, workers function normally.

### Concurrency Buckets

`concurrency_runs` holds, per workflow, the set of run IDs that hold a slot. `concurrency_tasks` uses `History: 1` to minimize storage. Each key holds the set of steps (`<run_id>/<step_id>`) that hold a slot of one task type, written with CAS. Taking or freeing a slot twice for the same step is a no-op, so a redelivered event cannot drift the count. Counters written by an older version read as no holders, so a limit can over-admit once for tasks in flight across the upgrade. The engine checks these sets before dispatching tasks. If a limit is exhausted, the task is retried via `SLEEP_TIMERS` with a 1-second delay.

## Subject Hierarchy

All NATS subjects follow a dot-separated hierarchy. The `>` wildcard matches one or more tokens.

```
history.{runID}                         # Workflow events
task.{taskType}.{runID}                 # Task distribution (default route)
event.{eventType}                       # External events
dead.{taskType}.{runID}.{stepID}        # Dead letter entries
telemetry.spans.{service}.{runID}       # Trace spans
telemetry.metrics.{service}.{name}      # Metrics
telemetry.logs.{service}.{severity}     # Log records
sleep.{runID}.{stepID}                  # Timer messages
scheduled.{runID}                       # Scheduled run triggers
sticky.{taskType}.{workerID}.{runID}    # Worker-affinity tasks
trigger.fire.{triggerID}                # Trigger fire records
logs.{runID}.{stepID}.{attempt}.{iter}  # Task log lines
stream.{runID}.{stepID}                 # Real-time step output (core NATS)
```

The subject design ensures that consumers can filter to exactly the messages they need. A worker subscribing to `task.summarize` only receives summarize tasks. The orchestrator subscribing to `history.>` receives all workflow events.

## Resource Setup

On startup, `natsutil.SetupAll(nc)`:

1. In a cluster, waits up to 60 seconds for JetStream to answer, so a meta leader exists before anything is created.
2. Creates `WORKFLOW_HISTORY`, `TASK_QUEUES`, `EVENTS`, `DEAD_LETTERS` and `SLEEP_TIMERS`.
3. Creates all KV buckets, `leases` last. On a nats-server older than 2.11 the `leases` bucket is skipped with a warning.
4. Creates `TELEMETRY`, `STICKY_TASKS`, `TRIGGER_HISTORY` and `BUILD_LOGS`.
5. Enables atomic batch publish on `TASK_QUEUES`, which needs nats-server 2.12 or newer.
6. Creates any extra streams and buckets the caller passed (`dagnats serve` passes five buckets, see above).

Every resource takes the cluster's replication factor, except the extras in step 6. A replicated stream or bucket is created first and updated only if it already exists with a different config. Running `dagnats serve` multiple times against the same NATS data directory is safe.

The whole sweep shares one time budget: 30 seconds standalone, so a server that does not answer fails startup fast, and 3 minutes in a cluster, where nodes starting together wait on each other's meta leader. Each create or update call is attempted up to 5 times, 8 seconds each, with a pause that grows from 500ms to 4s. Only "no answer in time" errors are retried: timeouts, JetStream error 10008 (system temporarily unavailable) and 10005 (no suitable peers for placement). Any other error fails startup at once, naming the stream or bucket.
