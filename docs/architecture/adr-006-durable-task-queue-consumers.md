# ADR-006: Durable Consumers on `TASK_QUEUES`

**Status:** Accepted (2026-05-01)
**Deciders:** Dan Mestas
**Spec:** [`docs/superpowers/specs/2026-05-01-issue-136-durable-task-queue-consumers-design.md`](../superpowers/specs/2026-05-01-issue-136-durable-task-queue-consumers-design.md)
**Issue:** [#136](https://github.com/danmestas/issues/136)

## Context

`worker.createConsumer` called `CreateOrUpdateConsumer` with a `ConsumerConfig` that set neither `Durable` nor `Name`. NATS treated the result as an ephemeral consumer. The `TASK_QUEUES` stream uses `WorkQueuePolicy` retention, which enforces one consumer per unique filter subject regardless of consumer name. On worker restart the dead worker's ephemeral consumer was still registered in stream metadata; the new worker's create call collided on the filter and panicked with `filtered consumer not unique on workqueue stream` (NATS error code 10100). Recovery required wiping the data directory.

Two pre-fix workarounds existed: run exactly one worker per task type (defeating scale-out), or wipe data on every restart (data loss).

## Decision

Make every `TASK_QUEUES` pull consumer durable with a deterministically derived name. Replace `createConsumer` with one deep helper:

```go
func (w *Worker) subscribePullConsumer(taskType, group string,
    handler HandlerFunc) jetstream.ConsumeContext
```

The helper owns the entire `ConsumerConfig`: durable name (`workers-<sanitized-task>` or `workers-<sanitized-task>-<sanitized-group>`), filter subject (`task.<task>.>` or `task.<task>.<group>.>`), `AckPolicy` (`AckExplicitPolicy`), `DeliverPolicy` (`DeliverAllPolicy`), `AckWait` (5-minute default), and `MaxDeliver` (`-1`, engine-owned DLQ). Three pure helpers (`sanitizeConsumerName`, `consumerNameFor`, `consumerFilterFor`) keep the naming convention in one place. A registration-time precheck (`assertNoConsumerNameCollisions`) refuses to start if any two `(taskType, group)` pairs sanitize to the same durable name. A self-healing migration step at the top of the helper deletes pre-existing ephemeral orphans matching the filter, so deployments upgrade cleanly without manual NATS state cleanup.

Sticky and elastic consumer code is unchanged. The collision precheck covers their inputs as a strict-gain side effect — latent name collisions in the elastic path that previously caused silent corruption now panic at `Start()` with both originals named.

### Cleanup race semantics

NATS server's `DeleteConsumer` is **idempotent across concurrent callers**: when two workers race to delete the same orphan, both calls return success — neither surfaces `ErrConsumerNotFound`. To distinguish winner from loser without distributed coordination, `cleanupOrphanEphemerals` performs a pre-delete `Consumer(ctx, name)` lookup. The metadata layer serializes lookups with deletes, so the loser observes `ErrConsumerNotFound` on the lookup and silently moves on; only the winner runs the delete and emits the audit log. This keeps the audit trail consistent with the principle "log the actions you took on shared state."

## Alternatives considered

**A. Stream retention change to `LimitsPolicy`.** Drops the one-consumer-per-filter constraint. Rejected: changes engine semantics (`WorkQueuePolicy` is what makes `TASK_QUEUES` an actual work queue with per-message ownership), forces re-architecting the rest of the engine, and `LimitsPolicy` doesn't reclaim space when consumers acknowledge.

**B. Manual consumer-name parameter on `Worker.Handle`.** Push naming up to the caller. Rejected: every caller would replicate the same naming convention (or worse, drift from it). Deep helper hides the policy.

**C. Cleanup as a separate phase in `Start()` (one `ListConsumers` for the whole worker).** Rejected: couples `Start()` to cleanup ordering, pulls helper-internal state to the worker level, marginal efficiency win at typical N (≤10 task types). Per-call cleanup keeps the helper self-contained. Re-evaluate if N grows past ~50.

**D. Per-task `AckWait` override in this PR.** Captured but deferred — the helper already coalesces `w.handlerAckWait[taskType]` over `defaultAckWait`, so the lookup is wired; only the population of the override map is missing. The future API is `worker.Handle("task", h, WithAckWait(d))`, co-located with the handler. No helper-API churn when it lands.

**E. Cross-process collision detection.** The in-process precheck doesn't catch worker A registering `render.gpu` while worker B registers `render-gpu` — both sanitize to the same durable. Deferred to ADR-009 candidate; cheap addition to the cleanup pass that detects "consumer with our exact durable name but different filter" and panics.

## Consequences

**Positive:**
- `Worker.Start()` is idempotent across restarts.
- N>1 workers per task type share a single durable consumer (NATS-native scale-out).
- Existing deployments upgrade cleanly — orphan ephemerals are auto-cleaned with full INFO audit trail.
- Latent collisions in the elastic path now panic at `Start()` instead of corrupting silently.

**Negative:**
- Reverting the merge does *not* restore prior NATS state. Operators must run `dagnats workers list --task-types | xargs -I{} nats consumer rm TASK_QUEUES "workers-{}"` before reverting, or hit the original #136 panic with the original diagnostic.
- One extra `ListConsumers` per subscribe call at startup. Cheap at N≤50; revisit if N grows.
- 5-minute `AckWait` default is a workload knob; sub-second tasks bear the worst case until the per-task override lands.

**Neutral:**
- Sticky path (`STICKY_TASKS`, `LimitsPolicy`) is unaffected.
- Elastic path code is unchanged. Its inputs are now precheck-validated.

## Out of scope (deferred)

- Per-task `AckWait` override via `WithAckWait` handler-registration option — separate follow-up issue.
- In-handler heartbeats via `msg.InProgress()` — ADR-008 (tracked as follow-up issue, not yet written).
- Cross-process consumer-name collision detection — ADR-009 candidate (tracked as follow-up issue).
- Unification of default and elastic consumer paths — ADR-007 (proposed alongside this ADR).

## Rollback

Operational rollback (must appear verbatim in the PR description):

```bash
# Before reverting the deployment, on each environment:
dagnats workers list --task-types | xargs -I{} nats consumer rm TASK_QUEUES "workers-{}"
# Then revert the deployment.
```

Acceptable because: rollback is rare, the operation is local to the affected stream, and `nats consumer rm` is idempotent (NotFound is fine).
