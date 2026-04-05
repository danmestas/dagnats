# Orbit: Partitioned Consumer Groups (pcgroups)

**Status:** Design
**Date:** 2026-04-04 (revised 2026-04-05)
**Depends on:** JetStream API migration (Phase 3 minimum)
**Source:** github.com/synadia-io/orbit.go/pcgroups v0.2.1
**Requires:** nats-server v2.11+ (covered by v2.12.0+ floor)

## Problem

Workers subscribe to task subjects via legacy `js.Subscribe`. Distribution is
round-robin with no affinity — for stateful steps (LLM context windows, local
caches), messages for the same workflow scatter across workers, causing context
thrashing. There is no partitioning, no ordered delivery guarantee per workflow,
and no built-in failover.

## Actual API (verified)

```go
// Two-step: create group, then consume
func CreateElastic(
    ctx context.Context,
    js jetstream.JetStream,          // NEW API required
    streamName string,
    config ElasticConsumerGroupConfig,
) (*ElasticConsumerGroupConfig, error)

func ElasticConsume(
    ctx context.Context,
    js jetstream.JetStream,
    streamName string,
    consumerGroupName string,
    memberName string,
    messageHandler func(msg jetstream.Msg),  // NEW message type
    config jetstream.ConsumerConfig,
) (ConsumerGroupConsumeContext, error)

type ConsumerGroupConsumeContext interface {
    Stop()
    Done() <-chan error
}
```

Key differences from original spec:
- Handler takes `jetstream.Msg` not `*nats.Msg` — requires worker migration
- Two-step: `CreateElastic` then `ElasticConsume` (no single-call convenience)
- No `ElasticConfig` with `Filter/Partitions/HashBy` — uses
  `ElasticConsumerGroupConfig` for creation, `jetstream.ConsumerConfig` for
  consumption
- Returns `ConsumerGroupConsumeContext` with `Stop()` and `Done()` channel

## Design

This spec covers two independent adoption points. Each can be adopted or
rejected without affecting the other. Engine orchestrator partitioning
(original Section B) is deferred.

### A. Worker Pool Partitioning (Elastic Groups)

After the worker migrates to the new jetstream API (migration Phase 3), replace
`js.Subscribe` / consumer creation with pcgroups elastic groups.

**Setup (once per task type, at worker start):**

```go
_, err := pcgroups.CreateElastic(ctx, jsNew, "TASK_QUEUES",
    pcgroups.ElasticConsumerGroupConfig{
        // Config fields TBD — inspect actual struct
    },
)
```

**Consume (per worker instance):**

```go
cc, err := pcgroups.ElasticConsume(
    ctx, jsNew,
    "TASK_QUEUES",
    "workers-"+taskType,     // group name
    w.workerID,              // member name (unique per instance)
    func(msg jetstream.Msg) {
        w.handleMessage(tt, h, msg)  // handler already takes jetstream.Msg post-migration
    },
    jetstream.ConsumerConfig{
        FilterSubject: "task." + taskType + ".>",
        AckPolicy:     jetstream.AckExplicitPolicy,
    },
)
```

Messages with the same subject (same runID) route to the same worker via
consistent hashing. If a worker dies, its partitions reassign automatically.

**Worker configuration** stays in worker config, not DAG schema:

```go
// WithPartitions configures pcgroups elastic consumer groups.
// 0 = legacy subscribe (default). Max 256.
func WithPartitions(n int) WorkerOption
```

**Naming:** The Worker struct already has `groups []string` for worker group
subscriptions. pcgroups handles use a distinct field: `elasticGroups`.

### B. Singleton Steps (Single-Partition Elastic Group)

Steps marked `Singleton: true` in the DAG get a single-partition group:

```go
cc, err := pcgroups.ElasticConsume(
    ctx, jsNew, "TASK_QUEUES",
    "singleton-"+taskType, w.workerID,
    handler,
    jetstream.ConsumerConfig{
        FilterSubject: "task." + taskType + ".>",
        AckPolicy:     jetstream.AckExplicitPolicy,
    },
)
```

Only one member receives messages at a time. Failover is automatic.

`Singleton` is the only pcgroups-related field on `StepDef`:

```go
type StepDef struct {
    // ... existing fields ...
    Singleton bool `json:"singleton,omitempty"`
}
```

Declarative intent — the worker maps it to single-partition internally.

### Relationship to ConcurrencyManager

Unchanged from original spec. Partitions control routing and parallelism.
ConcurrencyManager controls enforcement (how many instances allowed). They
operate at different levels and both remain.

### NATS Resources

Elastic groups create infrastructure per group:

| Resource | Type | Purpose |
|----------|------|---------|
| Consumer group config | Stream metadata | Partition routing rules |
| Per-member consumers | JetStream consumers | Message delivery |

### Bounds

- Max partitions per group: 256
- Max members per partition: 10 (standby replicas)
- Member heartbeat interval: 5s
- Failover timeout: 15s (3 missed heartbeats)

### Observability

- Metric: `worker.partition.active_members` — gauge per group
- Metric: `worker.partition.rebalance_count` — counter of rebalance events
- Log: info on partition assignment/revocation

### Risks

- **Rebalance latency:** 15s failover window. Acceptable for LLM tasks.
- **Handler type change:** `jetstream.Msg` vs `*nats.Msg` — handled by the
  prerequisite API migration, not by this spec.
- **Two-step setup:** `CreateElastic` is idempotent (create-if-not-exists
  semantics). Called once at worker startup.

### Interaction Matrix

| Feature | Compatible | Notes |
|---------|-----------|-------|
| Rate limiting | Yes | Rate check in handler, after partition routing |
| Concurrency (run) | Yes | AcquireRun gates at trigger time |
| Concurrency (task) | Yes | AcquireTask gates at dispatch time |
| Sticky workers | **Superseded** | Partition affinity replaces this |
| Batching | Yes | Batched runs dispatched normally |
| Sub-workflows | Yes | Routed by their own subject |
