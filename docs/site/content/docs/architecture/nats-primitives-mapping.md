---
title: NATS Primitives Mapping
weight: 5
---

DagNats maps every infrastructure need to a NATS primitive, eliminating external dependencies entirely.

## Why NATS-Native

Most workflow engines layer custom infrastructure on top of general-purpose databases: task queues in PostgreSQL, state in Redis, events in Kafka. Each dependency adds operational complexity, failure modes, and configuration surface.

DagNats inverts this. NATS JetStream provides durable streams, key-value storage, object storage, pub/sub, and request/reply in a single binary. By mapping every need to a NATS primitive, the entire system runs on one infrastructure dependency.

## The Mapping

| Need | NATS Primitive | How DagNats Uses It |
|------|---------------|---------------------|
| **Event log** | JetStream stream (WORKFLOW_HISTORY) | Immutable, append-only event sourcing. Every workflow state change is a message on `history.{runID}`. Retention: limits policy, 5s dedup. |
| **Task distribution** | JetStream stream (TASK_QUEUES) + pull consumers | WorkQueue retention ensures each task is delivered to exactly one worker. Workers create filtered pull consumers for their task types. `MaxAckPending` controls parallelism. |
| **Atomic fan-out** | JetStream atomic batch publish | Map steps publish all instance tasks in a single atomic operation. Either all tasks publish or none do. Requires NATS >= 2.12. |
| **Run state** | KV bucket (workflow_runs) | Snapshot of current run state for fast API reads. Optimistic locking via KV Revision prevents stale writes. |
| **Workflow definitions** | KV bucket (workflow_defs) | Immutable after creation. KV revision history provides versioning. |
| **Retries with backoff** | NakWithDelay | When a task fails with a retriable error, the engine NAKs the NATS message with a delay. NATS redelivers after the delay. No timer service, no retry queue, no cron job. |
| **Durable sleep** | NakWithDelay via SLEEP_TIMERS | Sleep steps publish a message to the `SLEEP_TIMERS` stream, then NAK it with the sleep duration. On redeliver, the engine publishes a `step.sleep.completed` event. Sleeps up to 365 days. |
| **Step timeouts** | AckWait + MaxDeliver | Each task has an `AckWait` deadline. If the worker does not acknowledge within the deadline, NATS redelivers. After `MaxDeliver` attempts, the task goes to the dead letter stream. |
| **Exactly-once events** | Nats-Msg-Id header | Every event published to WORKFLOW_HISTORY includes a dedup ID. The 5-second dedup window rejects duplicate publishes during retries. |
| **Cross-workflow signals** | KV watches | The `signals` KV bucket stores signal values at `{runID}.{name}`. A step calls `WaitForSignal()` which creates a KV watch. When another step calls `SendSignal()`, the KV update triggers the watcher immediately. |
| **Event correlation** | KV watches (event_waiters) | Wait-for-event steps register a waiter in the `event_waiters` KV bucket. The correlator watches this bucket and matches incoming events from the `EVENTS` stream. O(1) lookup per event type. |
| **Worker discovery** | KV bucket (workers) with TTL | Workers heartbeat every 30s by re-putting their entry. The 60s TTL auto-expires stale entries. Observability-only -- the engine never reads it. |
| **Internal API** | NATS micro framework | The API service uses `micro` for service discovery and load balancing over NATS request/reply. CLI and engine communicate via NATS subjects, not HTTP. |
| **Real-time streaming** | Core pub/sub | Workers call `PutStream(data)` which publishes to `stream.{runID}.{stepID}`. Subscribers receive updates in real time. No persistence needed -- this is ephemeral output streaming. |
| **Rate limiting** | KV CAS (compare-and-swap) | Token bucket state stored in the `rate_limits` KV bucket. CAS operations ensure atomic token acquisition. When exhausted, tasks retry via `SLEEP_TIMERS` with the refill delay. |
| **Concurrency control** | KV CAS counters | Per-task-type and per-run counters in `concurrency_tasks` KV bucket. CAS increment/decrement prevents races. Bounded retry: 10 CAS attempts. |
| **Large payloads** | Object Store + event references | When payloads exceed message size limits, they are stored in NATS Object Store. Events reference them by key. |
| **Worker affinity** | KV (sticky_bindings) + dedicated stream (STICKY_TASKS) | Bindings map runs to workers. Affinity tasks route to worker-specific subjects on the `STICKY_TASKS` memory stream. |
| **Human approval** | KV (approval_tokens) with TTL | 256-bit tokens stored with 7-day TTL. Atomic consumption via CAS prevents double-approve. |
| **Idempotency** | KV (idempotency_keys) with TTL | Dedup keys map to run IDs with 24-hour TTL. Prevents duplicate workflow starts from retry logic. |

## Key Patterns

### NakWithDelay as Universal Timer

The most powerful pattern in the mapping. NATS `NakWithDelay` tells the server "redeliver this message after N duration." DagNats uses this for:

- **Retry backoff**: failed tasks are NAK'd with exponentially increasing delays
- **Durable sleep**: sleep steps NAK with the sleep duration (up to 365 days)
- **Wait-for-event timeout**: NAK with the timeout duration, fires if no match arrives
- **Rate limit retry**: NAK with the token refill delay
- **Debounce**: NAK with the debounce window

One primitive replaces what would otherwise require a timer service, a scheduler, a delay queue, and a cron system.

### KV as Coordination Primitive

NATS KV provides atomic operations (Put, CAS, Delete) and real-time watches. DagNats uses these for:

- **Optimistic locking**: `workflow_runs` snapshots use KV Revision to detect concurrent writes
- **Signal delivery**: KV watches provide instant notification when a signal is written
- **Token buckets**: CAS operations ensure atomic rate limit token acquisition
- **Worker health**: TTL-based auto-expiry replaces explicit health check polling

### Pull Consumers for Work Distribution

Unlike push-based messaging, pull consumers let workers control their own pace. A worker pulls tasks when it is ready, processes one, then pulls the next. `MaxAckPending` caps the number of in-flight tasks. This naturally handles back-pressure without complex flow control.

## What This Eliminates

By mapping everything to NATS primitives, DagNats does not need:

| Eliminated | NATS Replacement |
|-----------|-----------------|
| PostgreSQL / MySQL | KV buckets + JetStream streams |
| Redis | KV buckets with TTL |
| Kafka / RabbitMQ | JetStream streams + consumers |
| Celery / Sidekiq | TASK_QUEUES stream + pull consumers |
| Cron daemon | SLEEP_TIMERS + NakWithDelay |
| Timer service | NakWithDelay |
| Service mesh | NATS micro + request/reply |
| etcd / Consul | KV watches + CAS |

The result is a single infrastructure dependency. One binary to deploy, one system to monitor, one backup strategy to implement.
