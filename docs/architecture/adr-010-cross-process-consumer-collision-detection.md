# ADR-010: Cross-Process Consumer Name Collision Detection

**Status:** Accepted (2026-05-02)
**Deciders:** Dan Mestas
**Closes:** [#145](https://github.com/danmestas/dagnats/issues/145)
**Related:** ADR-006 §5.5, ADR-007 §5.5

## Context

ADR-006 introduced `assertNoConsumerNameCollisions`
(`worker/consumer_collision.go`) — a registration-time precheck that
panics when a single Worker registers two task types whose sanitized
durable names collide on `TASK_QUEUES`. The classic shape is
`render.gpu` and `render-gpu` both sanitizing to
`workers-render-gpu`. The check is pure: it operates on the Worker's
in-memory `(handlers, groups)` view and never touches NATS.

That precheck does **not** catch the cross-process variant of the
same collision class:

- Worker A (process 1) registers `render.gpu` → durable
  `workers-render-gpu`, filter `task.render.gpu.>`.
- Worker B (process 2) registers `render-gpu` → durable
  `workers-render-gpu`, filter `task.render-gpu.>`.

Both processes call `CreateOrUpdateConsumer` against the same shared
`TASK_QUEUES` stream. The second call mutates `FilterSubject` on the
shared durable (NATS treats matching `Durable` as "update this
consumer" — no error, no warning). From that moment on, the
single-named consumer routes one task type's traffic and silently
drops the other's. Recovery is observable only as missing work, not
as a startup error.

ADR-006 §5.5 and ADR-007 §5.5 both explicitly deferred this case as a
follow-up. Two options were named:

- **Option A** — extend the in-helper precheck to also scan
  `ListConsumers` for "consumer with our exact durable name but
  different filter." Catches the common shape (steady-state running
  workers detect each other on startup). Doesn't catch the
  simultaneous-startup race.
- **Option B** — operational tooling (`dagnats doctor` or similar)
  that scans the stream and reports collisions across all configured
  workers. More flexible. More ops surface area.

## Decision

Ship Option A. Add `assertNoCrossProcessCollision(ctx, js, filter,
durable)` (`worker/consumer_collision_xprocess.go`) — a new helper
that lists consumers on `TASK_QUEUES` once and panics if any existing
consumer matches our durable name but advertises a different filter
subject. Call it from `subscribePullConsumer` **before**
`cleanupOrphanEphemerals` and **before** `CreateOrUpdateConsumer`.

Defer Option B. The tactical fix is cheap and catches the steady-state
case; the operational tool is a larger surface that can ride a future
ADR if the rate of cross-process collisions in the field justifies it.

### Why a separate helper, not extending `cleanupOrphanEphemerals`

The issue body suggested folding the check into the existing cleanup
pass to share its `ListConsumers` iteration. Per Ousterhout, two
single-purpose helpers are deeper than one helper doing two things —
`cleanupOrphanEphemerals` is named for what it does. A consumer with
our exact durable name (i.e. a *non-orphan*) is outside that helper's
contract. Coupling the two means future readers must learn that
"cleanup" also does collision detection, and either operation
becomes harder to reason about in isolation.

The duplicated `ListConsumers` is acceptable — both passes happen
once per `subscribePullConsumer` call, both are O(N consumers on the
stream), and N is bounded at typical deployment scale (≤50 task
types × ≤1 group ≈ ≤50 entries). If `Start()` time becomes a
hotspot at higher N, ADR-006 §5.5 already names the consolidation
path: a single sweep at the top of `Start()` driving both.

## Consequences

**Positive:**
- Cross-process collisions panic at `Start()` with both filter
  subjects and the colliding durable name in the message.
- Deployment-time programmer error (one task-type rename) replaces
  silent runtime corruption (lost work routed to the wrong handler).
- The fix is local: one helper, one call site, no engine changes.
- Independent of ADR-007 — this lands on the existing
  `subscribePullConsumer` path and doesn't need to wait for path
  unification.

**Negative:**
- Extra `ListConsumers` per `subscribePullConsumer` call. Cheap at
  N≤50 but doubles the per-call cost relative to ADR-006. Revisit if
  N grows past ~50 — see "Consolidation path" below.
- Doesn't catch the simultaneous-startup race (two processes both
  see the stream as empty, both call `CreateOrUpdateConsumer`,
  whichever lands second silently mutates the filter). Mitigated in
  practice because production deployments stagger startup; not
  mitigated in chaos-testing scenarios. Option B (operational
  tooling) would close this gap.

**Neutral:**
- Sticky path (`STICKY_TASKS`, `LimitsPolicy`) is unaffected — its
  consumers do not share namespace with `TASK_QUEUES` durables.
- Elastic path (pcgroups) is unaffected by this ADR. The in-process
  precheck added in ADR-006 still covers its inputs.

### Consolidation path (deferred)

If `ListConsumers` traffic becomes a hotspot (~N>50 task types per
worker), fold the cross-process check, the orphan cleanup, and any
future stream-scan checks into a single `Start()`-level sweep. ADR-006
§5.5 already names this path. The cost today is one extra round-trip
per task type per worker per startup — negligible at typical scale.

## Alternatives Considered

**Extend `cleanupOrphanEphemerals` to also detect collisions.**
Rejected. Two mixed responsibilities make the helper harder to read
and harder to evolve. The duplicated `ListConsumers` is a known,
bounded cost.

**Defer entirely to operational tooling (Option B).** Rejected for
this ADR. The tactical fix is small and catches the common case;
shipping it does not preclude Option B later. Option B is a larger
surface (CLI, output format, integration with deployment tooling) that
deserves its own ADR if the field demands it.

**Replace the in-process precheck.** Rejected. The in-process check
catches the same-Worker case at registration without any NATS round
trip; it's the cheapest layer in the stack. The cross-process check
is additive, not a replacement.

**Use NATS `Consumer(durable)` lookup with config compare instead of
`ListConsumers`.** Considered. A targeted lookup would avoid scanning
N consumers. Rejected because it adds a second NATS call to the hot
path (`Consumer` lookup, then either `CreateOrUpdateConsumer` or
panic) where today we already pay for the cleanup-pass scan. At N≤50,
one extra scan and one panic-on-mismatch is simpler than two calls
plus reconciliation logic. Re-evaluate alongside the consolidation
path above.
