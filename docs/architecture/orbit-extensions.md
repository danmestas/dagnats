# Orbit Extensions

## Design Decision: jetstreamext for Atomic Publish, pcgroups for Worker Affinity

Adopted two Synadia Orbit extensions (`github.com/synadia-io/orbit.go`):

- **jetstreamext** — atomic batch publish for DAG task fan-out
- **pcgroups** — elastic partitioned consumer groups for worker affinity

Evaluated and rejected **counters** — the API tracks source streams, not
source engines, making per-engine crash recovery impossible as designed.

## Atomic Task Fan-Out (jetstreamext)

When a DAG step completes and multiple downstream steps become ready,
`enqueueReadySteps` now publishes all task messages atomically via
`jetstreamext.PublishMsgBatch`. All messages land or none do.

**Before:** Per-step publish loop. Crash mid-loop = partial fan-out, fixed
by idempotent replay.

**After:** Single atomic batch per stream. Crash = either all tasks
dispatched or none. Idempotent replay still works as a safety net.

**Mixed-stream handling:** Normal steps go to `TASK_QUEUES` (`task.>`),
agent steps go to `AGENT_TASKS` (`agent_task.>`). Atomic publish works
within a single stream, so messages are split into two batches — one
per stream. Each batch is independently all-or-nothing.

**Stream config:** `AllowAtomicPublish` enabled on `TASK_QUEUES` via
`natsutil.EnableAtomicPublish`. Requires nats-server v2.12.0+, enforced
by `natsutil.RequireServerVersion` at startup.

## Elastic Consumer Groups (pcgroups)

Workers can now use partition-based message routing via pcgroups. Messages
with the same subject (same runID) always route to the same worker,
providing affinity for stateful processing (LLM context windows, local
caches). Automatic failover when a worker dies.

**Configuration:** `WithPartitions(n)` option on Worker. Infrastructure
concern — partition count is not in the DAG schema.

**Singleton steps:** `HandleSingleton(taskType, handler)` registers a
handler as a single-partition elastic group. Only one consumer processes
messages across all worker instances. `dag.StepDef.Singleton` is the
declarative intent field.

**Two-step setup:** `pcgroups.CreateElastic` (idempotent, called at worker
start) then `pcgroups.ElasticConsume` (joins as member). Worker ID is the
member name. `pcgroups.AddMembers` registers the worker before consuming.

**Legacy path preserved:** `WithPartitions(0)` (default) uses the standard
jetstream consumer API. Existing deployments are unaffected.

## Rejected: Orbit Counters

`orbit.go/counters` provides distributed counters with source tracking.
The spec proposed using source attribution for per-engine crash recovery
(identifying which engine holds phantom concurrency slots).

**Why rejected:** `Counter.AddInt(ctx, subject, value)` has no `source`
parameter. `Entry.Sources` is `map[string]map[string]*big.Int` — tracking
source *streams*, not application instances. The library solves cross-stream
aggregation, not per-engine attribution.

**Alternative for crash recovery:** KV-backed leases with engine ID + TTL.
~80 lines, no dependency. Not yet implemented.

## Relationship to ConcurrencyManager

pcgroups and ConcurrencyManager operate at different levels:

| Concern | Mechanism |
|---------|-----------|
| Task routing/affinity | pcgroups partition count |
| Task concurrency limits | ConcurrencyManager KV+CAS |
| Run concurrency limits | ConcurrencyManager KV+CAS |

Partition count controls parallelism. ConcurrencyManager controls
enforcement. Both remain.

## Server Version

All Orbit extensions require nats-server v2.12.0+. Enforced once at
startup via `natsutil.RequireServerVersion(nc, "2.12.0")`.
