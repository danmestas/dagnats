# Flow Control (Planned)

Features designed but not yet implemented. See `docs/superpowers/specs/` for full design specs (to be deleted once implemented).

## Event-Based Cancellation

Cancel running workflows when a matching event arrives — e.g., cancel reminder when task completes.

**Design:** `CancelOn` field on `WorkflowDef` (max 5 entries). Each entry: event type + `Match` condition. Reuses the existing `Correlator` with a new `WaiterActionCancel` discriminator. On match, publishes `workflow.cancelled` with the triggering event as payload (no `CancelledBy` field on `WorkflowRun` — cause lives in the event log only). Optional timeout stops watching after a period.

**Builder:** `wb.CancelOn(event, match)`, `wb.CancelOnWithTimeout(event, match, timeout)`.

Cleanup: cancel waiters removed on any terminal state. Shares 10,000-per-event-type waiter bound with wait-for-event.

## Priority Queues

Reorder pending runs by business priority when concurrency limits create backlogs.

**Design:** `PriorityConfig` on `WorkflowDef` with `Key` (dot-path into input), `Rules` (`map[string]int`), `DefaultOffset`. Offset range: [-600, +600] seconds. `PriorityOffset` stored on `WorkflowRun`. `EffectiveTime()` computed method (`CreatedAt - offset`), not a stored field. `findOldestPendingRun` sorts by `EffectiveTime()` instead of `CreatedAt`.

**Builder:** `WithPriority(PriorityConfig{Key: "data.tier", Rules: map[string]int{"enterprise": 300}})`.

Only meaningful with concurrency limits (validation warns without them). Max 20 rules.

## Singleton

Ensure at most one active run per key. Two conflict modes:

- **Skip:** discard new run if one is already active
- **Cancel:** cancel existing run, start new one ("last write wins")

**Design:** `SingletonConfig` on `WorkflowDef` with `Mode` and optional `Key` (dot-path for per-entity scope). KV bucket: `singleton_locks` (no TTL, explicitly managed). Lock acquired via CAS `Create` in `handleWorkflowStarted`. Stale lock detection: verify existing run's status from KV. Lock released on any terminal state. CAS retry: 3 attempts.

**Builder:** `WithSingleton(mode)`, `WithSingletonKey(mode, key)`.

Compatible with concurrency, debounce, throttling, priority, CancelOn. Incompatible with batching. CLI: `dagnats singleton list`, `dagnats singleton release <key>` (admin escape hatch).

**Admission pipeline:** Singleton, priority, and concurrency checks accumulate in `handleWorkflowStarted`. Extract into `admitRun(wfDef, run, input) → (action, cancelID, offset)` — one deep module, called once. Future gates added there, not in the event handler.
