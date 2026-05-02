# Durable Consumers on TASK_QUEUES — Design Spec

**Date:** 2026-05-01
**Author:** Dan Mestas + Claude
**Status:** Draft (pending implementation)
**Scope:** Fix for [issue #136](https://github.com/danmestas/dagnats/issues/136) — worker panic on restart with `filtered consumer not unique on workqueue stream` (NATS error code 10100).

## Summary

`worker.createConsumer` (`worker/worker.go:385–418`) calls `CreateOrUpdateConsumer` with a `ConsumerConfig` that sets no `Durable` or `Name` field. NATS treats the result as ephemeral. The `TASK_QUEUES` stream uses `WorkQueuePolicy` retention (`internal/natsutil/conn.go:34–40`), which enforces **one consumer per unique filter subject** regardless of consumer name. On worker restart the dead worker's ephemeral consumer is still registered in stream metadata; the new worker's create call collides on the filter and panics. Recovery currently requires wiping the data dir.

The fix: make the consumer durable with a deterministic name. `CreateOrUpdateConsumer` becomes idempotent on restart and supports N>1 workers per task type sharing a single durable consumer (NATS-native scale-out). A registration-time precheck panics on internal naming collisions. A self-healing migration step removes pre-existing ephemeral orphans before claiming the durable, with a full INFO audit trail.

The change is contained to `worker/`. Sticky and elastic consumer code is untouched. One follow-up ADR (ADR-007) commits to unifying the default and elastic paths under a falsifiable contract; one further ADR (ADR-008) commits to in-handler heartbeats as the long-term answer to the AckWait knob.

## Outcome (the contract this PR ships)

After merge:

1. `worker.Start()` is idempotent across restarts.
2. N>1 workers per task type share a single durable consumer (NATS-native scale-out).
3. Existing deployments upgrade cleanly without manual NATS state cleanup.

Sticky and elastic paths' **code** is untouched. Their **runtime behavior** gains the registration-time collision check as a strict-gain side effect — latent name collisions that previously caused silent corruption now panic at `Start()`.

## Non-goals (explicit, scoped out)

- Fixing issues #137 (status doesn't transition `queued`→`running`) or #138 (`MAX_TASKS=9` doesn't pick up second task). Different code paths; same cluster of symptoms but distinct root causes.
- Changing message routing or stream retention policy.
- Restructuring the worker lifecycle or handler dispatch loop.
- Exposing per-deployment consumer config knobs beyond what this fix legitimately needs (`AckWait`).
- Cross-process consumer-name collision detection. In-process precheck only; cross-process is captured for follow-up (ADR-009 candidate).
- `msg.InProgress()` heartbeats. Real long-term answer to `AckWait`-as-workload-knob; deferred to ADR-008.

---

## §1. Helper: `subscribePullConsumer`

Single source of truth for any `TASK_QUEUES` pull consumer. Replaces both open-coded callsites in `subscribeTask` (`worker.go:352` default branch, `worker.go:371-373` groups branch). `createConsumer` is deleted outright — audit confirms no other callers.

The helper is **deep**: it takes the smallest semantic input (`taskType` + optional `group`) and internally derives the durable name, filter subject, ackWait, and full ConsumerConfig. Callers never construct or know about consumer-naming, filter-subject patterns, or ackWait policy.

### Signature

```go
// subscribePullConsumer attaches a worker to a durable JetStream pull
// consumer on TASK_QUEUES, creating it if absent. Idempotent across
// worker restarts. Cleans up orphan ephemeral consumers with the same
// filter subject before creation (see §3). Panics on setup failure;
// stream/consumer setup errors are startup-fatal.
//
// The durable name and filter subject are derived from (taskType, group)
// via consumerNameFor and consumerFilterFor (see §2). AckWait is derived
// from the worker's per-task config with a package-private default
// fallback. Callers carry no naming, filter, or policy knowledge.
func (w *Worker) subscribePullConsumer(
    taskType string,        // e.g. "render", "nasr-ingest"
    group    string,        // "" for default branch, otherwise e.g. "gpu"
    handler  HandlerFunc,
) jetstream.ConsumeContext
```

### Owned config (single source of truth)

All callsites get the same values. The only inputs that legitimately vary across calls are `taskType`, `group`, and `handler`.

| Field | Value | Why |
|---|---|---|
| `Durable` | `consumerNameFor(taskType, group)` | Fixes #136. Idempotent restart, shared scale-out. |
| `Name` | same as `Durable` | NATS treats `Durable`==`Name` when both set; explicit avoids surprise. |
| `FilterSubject` | `consumerFilterFor(taskType, group)` | Derived alongside the durable name from the same `(taskType, group)` pair. |
| `AckPolicy` | `AckExplicitPolicy` | Crash-safety — unacked msgs redeliver. (Already set today.) |
| `DeliverPolicy` | `DeliverAllPolicy` | Durable retains position; no loss when all workers down briefly. (Already set today.) |
| `AckWait` | `coalesce(w.handlerAckWait[taskType], defaultAckWait)` | **Workload knob.** Different task types legitimately need different values; the override map is populated by `WithAckWait` at handler registration (deferred — see below). Today every task gets `defaultAckWait`. |
| `MaxDeliver` | `-1` (unlimited) | Engine drives retry/backoff via `NakWithDelay` per CLAUDE.md NATS-native patterns. Capping `MaxDeliver` here would shadow engine policy. |
| `MaxAckPending` | NATS default (1000) | Aggregate in-flight cap across the worker fleet. Tune later if real bottleneck emerges. Documented, not parameterized. |

The `MaxDeliver: -1` line gets a comment naming the engine-owned-DLQ contract:

```go
// DLQ routing and retry budgets are the engine's responsibility
// (NakWithDelay + attempt count in step state), not NATS's. We
// leave NATS unbounded so engine policy isn't silently shadowed.
MaxDeliver: -1,
```

### `defaultAckWait` constant (package-private)

```go
// defaultAckWait bounds the longest expected task duration plus a margin.
// Workers running tasks longer than this should call msg.InProgress()
// periodically (see ADR-008) or override at handler registration via
// WithAckWait (deferred follow-up).
const defaultAckWait = 5 * time.Minute
```

Lowercase: implementation detail of the `worker` package, not exposed in the helper signature or the `Worker` API.

### Per-task override (deferred, future-proof shape)

Per-task override is a one-line follow-up. The future API is **`WithAckWait` as a handler-registration option**, not a parallel map:

```go
// Future (deferred, not in this PR):
worker.Handle("nasr-ingest", handler, WithAckWait(5*time.Minute))
```

Internally, `Worker.handlers` evolves from `map[string]HandlerFunc` to `map[string]handlerInfo{handler, ackWait}`. The helper's `coalesce(w.handlerAckWait[taskType], defaultAckWait)` line stays unchanged when the override lands; the only code that changes is how `handlerAckWait` is populated. **No helper-API churn.**

Co-locates operational config with the handler it governs — one source of truth per task type, no risk of "I registered the handler but forgot to set the ackWait elsewhere."

### Assertions (TigerStyle)

- `assert taskType != ""` — programmer error if unset.
- `assert handler != nil` — same as today.
- `assert defaultAckWait > 0` — defends the constant at package init time (compile-time-checkable for a const, so this is documentation more than runtime defense; included for symmetry with the contract).

### Callsite shape

```go
// default branch (was line 351-352)
cc := w.subscribePullConsumer(taskType, "", h)

// groups branch (was line 371-373)
for _, group := range w.groups {
    cc := w.subscribePullConsumer(taskType, group, h)
}
```

Two-line callers. No naming, filter, or policy knowledge required.

---

## §2. Naming convention

### Format

```
workers-<sanitized-taskType>                    // default branch (group="")
workers-<sanitized-taskType>-<sanitized-group>  // groups branch
```

Matches the elastic path's `groupName` exactly (`worker.go:325, 330-331`). Operators see one consistent family of consumer names regardless of mode. The `workers-` prefix doubles as the "this consumer is owned by dagnats workers" identification signal — used by the migration cleanup (§3) as belt-and-suspenders alongside the primary "ephemeral + matching filter" rule.

### Canonical helpers (single home for the convention)

The naming convention has one home — two pure functions that turn a `(taskType, group)` pair into the durable name and filter subject. Used by both `subscribePullConsumer` and `assertNoConsumerNameCollisions` so the convention can never drift between subscribe and precheck.

```go
// consumerNameFor produces the durable consumer name for a (taskType, group) pair.
// group="" means the default branch. Both inputs are sanitized via
// sanitizeConsumerName before being concatenated under the "workers-" prefix.
func consumerNameFor(taskType, group string) string

// consumerFilterFor produces the filter subject for a (taskType, group) pair.
// Inputs are NOT sanitized — they appear in the message subject hierarchy and
// must round-trip exactly. (Sanitization is a consumer-naming concern; subject
// validity is the publisher's contract.)
func consumerFilterFor(taskType, group string) string
```

Two TigerStyle assertions on each: input non-empty (taskType), output non-empty.

### Sanitization

```go
// sanitizeConsumerName maps a task-type or group string to a NATS-legal
// consumer-name fragment. Dots collapse to hyphens for the common
// dotted-namespace case; other disallowed characters fall back to
// underscore. Empty input or empty output is a programmer error.
func sanitizeConsumerName(s string) string
```

| Input character class | Mapping |
|---|---|
| `A-Z`, `a-z`, `0-9`, `-`, `_` | preserved |
| `.` | → `-` (visual round-trip preserved for the common case: `render.gpu` → `render-gpu`) |
| anything else | → `_` (safe escape, includes whitespace, `*`, `>`, unicode, etc.) |

Two TigerStyle assertions:
1. `assert s != ""` — calling with zero string is a contract violation.
2. `assert result != ""` — defends against future input-class additions that all map away.

`workers-` is reserved as a contract. The library does not let users pick consumer names that collide. If a future feature genuinely needs operator-controlled naming, it adds a separate prefix family — never reuses `workers-`.

### Collision precheck

Sanitization is lossy: `render.gpu` and `render-gpu` both produce `workers-render-gpu`. The `FilterSubject` distinguishes them at the NATS level, but the durable consumer is bound to a single filter — `CreateOrUpdateConsumer` would either error or silently update the filter to whichever taskType called second. Either way: silent breakage worse than #136.

`assertNoConsumerNameCollisions()` runs once at `Start()` before any subscribe call. Enumerates the full set of durable names this worker will create — **using `consumerNameFor` directly**, so the precheck and the subscribe path can never disagree about what name a `(taskType, group)` pair produces. Detects duplicates, panics with the originals named:

```go
// default mode collision message:
// dagnats: task types %q and %q both produce durable %q — rename one

// groups mode collision message:
// dagnats: (task=%q,group=%q) and (task=%q,group=%q) both produce durable %q — rename one
```

Naming the **originals** (not the sanitized result) is load-bearing. Operator gets an actionable answer.

The precheck is a separate helper (not inline in `Start`) for unit-testability without a NATS server, and to give future registration-time validation (per-task-AckWait, etc.) an obvious home.

**Coverage clarification — elastic path included by precheck, not by code change:** `assertNoConsumerNameCollisions` runs at `Start()` before `subscribeTask` chooses default vs. elastic. It validates inputs to either path. We don't modify `createElasticConsumer`; we just refuse to call it with collision-prone names. This is a strict gain — elastic had the same latent bug; we close it for free.

---

## §3. Migration cleanup — orphan-ephemeral removal

Self-healing. No operator action required. Lives inside the helper so every subscribe call is responsible for its own filter-subject's hygiene; no separate phase in `Start()`, no coupling between subscribe order and cleanup order.

### Where

First step of `subscribePullConsumer`, before the `CreateOrUpdateConsumer` call.

### Identification rule (3-prong)

A consumer on `TASK_QUEUES` is treated as a removable orphan iff **all three** hold:

1. `FilterSubject == filter` (the one this helper is about to claim).
2. `Durable == ""` (ephemeral — pre-fix dagnats was the only thing making these on `TASK_QUEUES`).
3. `Name` doesn't start with `workers-` (belt-and-suspenders — refuses to delete anything that looks deliberately named under our scheme, even if it's somehow ephemeral).

The first two are sufficient identity ("provably a pre-fix dagnats orphan"); the third is defense against future state we haven't anticipated. Any of the three can be relaxed later if a real case demands it.

### Algorithm

```
iter = stream.Consumers(ctx)              // iterator form, NOT single-page list
for each consumerInfo in iter:
    if consumerInfo.Config.FilterSubject == filter
       AND consumerInfo.Config.Durable == ""
       AND not strings.HasPrefix(consumerInfo.Name, "workers-"):
        slog.Info("removing orphan ephemeral consumer for migration to durable",
            "consumer_name",         consumerInfo.Name,
            "filter_subject",        consumerInfo.Config.FilterSubject,
            "stream",                "TASK_QUEUES",
            "durable_being_claimed", durable,
            "reason",                "ephemeral with matching filter; pre-fix dagnats orphan",
        )
        err = stream.DeleteConsumer(ctx, consumerInfo.Name)
        if err != nil and not errors.Is(err, jetstream.ErrConsumerNotFound):
            panic("subscribePullConsumer: orphan cleanup failed for " + name + ": " + err.Error())
```

**Use the SDK iterator form, not the single-page list form.** Single-page list silently truncates beyond ~256 entries; an orphan past page-1 would survive cleanup and re-trigger #136 in deployments with enough state.

### Failure-mode policy

- `ListConsumers`/iterator failure → panic. Can't safely proceed without knowing consumer state.
- `DeleteConsumer` returning `ErrConsumerNotFound` → swallowed (concurrent-startup race won by sibling).
- `DeleteConsumer` returning any other error → panic. Don't `CreateOrUpdateConsumer` on a stream we can't clean.
- Subsequent `CreateOrUpdateConsumer` failure → panic with the original NATS error verbatim. Same as today.

### Considered alternative — single-pass cleanup at `Start()`

A separate phase in `Start()` could scan the stream once and clean all matching orphans before any subscribe runs (one `ListConsumers` instead of N). Marginal efficiency win at typical N (≤10 task types), at the cost of coupling `Start()` to cleanup ordering and pulling helper-internal state up to the worker level. Rejected: per-call cleanup keeps the helper self-contained, and the cost is dominated by network round-trip even at N=10. Re-evaluate if N grows past ~50 or `ListConsumers` cost becomes observable.

### Observability contract

INFO log on every deletion, structured (`slog`), all five fields: `consumer_name`, `filter_subject`, `stream`, `durable_being_claimed`, `reason`. Self-healing is never silent — pre-state and post-state of every migration are both audit-traceable.

---

## §4. Test matrix

Single-package home: `worker/` (per CLAUDE.md, integration tests with real embedded NATS server, no sharing). Each test file opens with a methodology comment. Bounded timeouts on every wait.

### §4.1 Pure unit (no NATS)

- **`TestSanitizeConsumerName`** — table-driven (`render`→`render`, `render.gpu`→`render-gpu`, `nasr-ingest`→`nasr-ingest`, `vendor::ingest`→`vendor__ingest`, `a b c`→`a_b_c`, `....`→`----`). Plus assertion-panic cases: empty input, hypothetical input that would map entirely away.
- **`TestAssertNoConsumerNameCollisions`** —
  - `render.gpu` + `render-gpu` → panic, message names both originals + colliding durable.
  - With groups, `(render, gpu.fast)` + `(render, gpu-fast)` → panic, message names both pairs + colliding durable.
  - **Cross-product no-collision: 2 task types × 2 groups → 4 distinct durables, no panic.** Guards the cross-product enumeration.
  - No-collision baseline: `nasr-ingest` + `airports-canonical-refresh` → no panic.
  - Empty handlers map → no panic.

### §4.2 Integration — single worker, durability + restart

- **`TestWorkerStart_DurableIdempotent`** — `Start()` against a fresh stream, then `Stop()` and `Start()` again. Both calls succeed; durable survives stop/start cycle; message published between phases delivers after second `Start()`.
- **`TestWorkerStart_NewProcessReclaimsDurable`** — first Worker starts, registers durable, processes a message, exits without unbinding. Second Worker (separate instance, same handlers) starts against the same stream. No panic; durable resumes; in-flight message redelivers within `AckWait` if first worker died holding it.

### §4.3 Integration — multi-worker scale-out

- **`TestTwoWorkers_SameTaskType_NoPanic`** — two Workers handling `render`, both `Start()` against the same stream. **The original repro from #136. Both start cleanly. This test fails on current `main`; it's the red of red-green.**
- **`TestTwoWorkers_LoadBalance`** — two Workers handling `render`, publish 10 messages, assert all 10 are processed exactly once across the pair (NATS-managed load balance via shared durable).
- **`TestTwoWorkers_KillOne_OtherDrains`** — two Workers handling `render`, kill one mid-processing, assert remaining worker drains the queue. Bounded timeout = `AckWait + 30s`.

### §4.4 Integration — migration cleanup

- **`TestMigration_OrphanEphemeralRemoved`** — pre-seed an ephemeral consumer with matching `FilterSubject`. Start a Worker. Assert: orphan deleted, INFO log emitted with all five expected fields, durable created, message round-trip works.
- **`TestMigration_ConcurrentStartup_OneOrphan`** — pre-seed orphan, start 2 Workers concurrently using `sync.WaitGroup`. Assert: both `Start()` return without panic; orphan deleted exactly once (look at log records); both bound to the same durable; messages load-balance.
- **`TestMigration_NoOrphan`** — fresh stream. Worker starts, no migration log line, durable created, message round-trip works.
- **`TestMigration_PreservesManagedConsumer`** — pre-seed a durable consumer named `workers-render` with matching filter. Start Worker. Assert: not deleted, no migration log line, `CreateOrUpdateConsumer` is idempotent (consumer count on stream stays 1).
- **`TestMigration_PreservesUnrelatedConsumer`** — pre-seed an unrelated consumer (e.g., durable `audit-tap`, non-matching filter). Start Worker handling `render`. Assert: untouched, no migration log line about it.
- **`TestMigration_PaginationManyConsumers`** — pre-seed 300 consumers on the stream (above the SDK's typical 256-entry page boundary, with a healthy margin), one of which is an orphan ephemeral matching `task.render.>` placed deep in the list. Start Worker handling `render`. Assert: orphan found and deleted regardless of page position. Catches "iterator-vs-single-page" regressions. Gate behind `testing.Short()` if it's > 5s wall-clock.

### §4.5 Integration — sanitization end-to-end

- **`TestRealisticTaskNames_AllSanitizationPaths`** — Worker registered with task types covering each sanitization branch (identity: `nasr-ingest`; dot-collapse: `render.gpu`; safe-escape: `vendor::ingest`). Start, publish one message per type, assert each is processed by the correct handler. Verifies the durable name produced for each is what we expect (`workers-nasr-ingest`, `workers-render-gpu`, `workers-vendor__ingest`) by reading `stream.Consumers(ctx)`.

### §4.6 Integration — config readback (drift defense)

- **`TestSubscribePullConsumer_AppliesExpectedConfig`** — Start a Worker handling `render`. Read back `ConsumerInfo` via `stream.Consumer(ctx, "workers-render").Info(ctx)`. Assert:
  - `Config.Durable == "workers-render"`
  - `Config.Name == "workers-render"`
  - `Config.FilterSubject == "task.render.>"`
  - `Config.AckPolicy == jetstream.AckExplicitPolicy`
  - `Config.DeliverPolicy == jetstream.DeliverAllPolicy`
  - `Config.AckWait == defaultAckWait` (i.e. `5 * time.Minute`)
  - `Config.MaxDeliver == -1`

  Asserts only on fields the helper owns. Specifically prevents the "someone tweaks `MaxDeliver: -1` to `MaxDeliver: 5` for safety and silently breaks the engine-owned-DLQ contract" drift. Also pins `defaultAckWait` so a future "I'll just bump this" change can't slip in without updating the test.

### §4.7 Assertion defense

- **`TestSubscribePullConsumer_RejectsEmptyTaskType`** — call helper with `taskType=""`, assert panic. Defends the TigerStyle assertion as a contract, not just documentation.
- **`TestConsumerNameFor_RejectsEmptyTaskType`** — call helper with `taskType=""`, assert panic. Same, for the naming helper.

### §4.8 Failure-mode tests for cleanup

- **`TestMigration_ListFailure_Panics`** — kill embedded NATS (or inject error at SDK boundary), Worker.Start() panics with expected message naming "list orphan consumers".
- **`TestMigration_DeleteFailure_Panics`** — same pattern, panic on non-NotFound delete error.

If SDK error-injection is genuinely painful, both can defer to a follow-up issue — but the issue must be filed and referenced from the test file's methodology comment. Tracked, not skipped.

### §4.9 What's deliberately not tested

- The elastic path's runtime behavior — out of scope per the outcome contract. The collision-precheck coverage is the only behavior change there and it's covered in §4.1.
- Sticky consumer behavior — out of scope. Sticky uses `STICKY_TASKS` with `LimitsPolicy` (`internal/natsutil/conn.go:149-157`), not affected by workqueue uniqueness.
- `msg.InProgress()` heartbeats — deferred to ADR-008.
- Cross-process collision detection — deferred to ADR-009 candidate.

### §4.10 Test infrastructure

Each integration test gets its own embedded NATS server. For `TestMigration_PaginationManyConsumers`, the 300-consumer setup adds runtime — gate behind `testing.Short()` if > 5s wall-clock.

---

## §5. ADR contract for path unification

This PR ships **ADR-006: Durable consumers on `TASK_QUEUES`** (this design). It also commits to writing **ADR-007: Unify default and elastic consumer paths** — but as a contract, not a TODO.

### §5.1 Target code shape (the load-bearing claim)

Post-unification, `subscribeTask` has exactly one consumer-creation path. The default mode is the elastic mode with `partitions=1`:

```go
// AFTER unification (ADR-007):
func (w *Worker) subscribeTask(taskType string, handler HandlerFunc) {
    partCount := w.partitions
    if partCount == 0 {
        partCount = 1   // default-mode collapses into elastic-with-1-partition
    }
    if w.singletons[taskType] {
        partCount = 1
    }
    if len(w.groups) == 0 {
        cc := w.createElasticConsumer(taskType, "", partCount, handler)
        w.stoppers = append(w.stoppers, cc)
    } else {
        for _, group := range w.groups {
            cc := w.createElasticConsumer(taskType, group, partCount, handler)
            w.stoppers = append(w.stoppers, cc)
        }
    }
}
```

Note: this target keeps the deeper-helper shape from §1. `createElasticConsumer` takes `(taskType, group, partCount, handler)` — naming, filter subject, AckPolicy, DeliverPolicy, AckWait, MaxDeliver are all derived inside the helper using the same `consumerNameFor` / `consumerFilterFor` plumbing. `subscribePullConsumer` and `createConsumer` are deleted. The migration cleanup logic from §3 either also moves into `createElasticConsumer` (if pcgroups creates differently named consumers) or gets retired (if pcgroups is naturally orphan-resistant).

### §5.2 Open questions (must close before ADR-007 lands)

Each question is falsifiable: phrased as "verify by reading source + running test X." The answer is observable, not opinion.

1. **What does pcgroups do at `partitions=1`?** Does it short-circuit to a single consumer with no partition-routing overhead, or run a degenerate group lifecycle every time? Investigate via reading pcgroups source + a microbenchmark of `partitions=1` vs. plain pull consumer creation. Result: numbers + acceptance verdict.
2. **Are pcgroups consumers nameable to match `workers-<task>` exactly, or does pcgroups impose its own naming?** If it prefixes/suffixes (e.g., `workers-render-p0`), the migration story changes. Verify by creating one and inspecting `nats consumer ls`.
3. **Migration story.** ADR-006 deployments have durable consumers named `workers-<task>`. If ADR-007 produces differently named consumers, every ADR-006 deployment hits the same panic class on first ADR-007 startup. Either (a) names match (no-op), or (b) names differ and the §3 cleanup logic generalizes to also delete previous-shape durables. Falsifiable: roll an ADR-006 deployment forward to ADR-007 in a test fixture, assert no panic, no message loss.
4. **Sticky consumer interaction.** Sticky uses `STICKY_TASKS` (different stream, `LimitsPolicy`). Does ADR-007 fold sticky into elastic, leave sticky alone, or eliminate sticky entirely? Out of scope for ADR-007; if the answer is "fold sticky in," that becomes its own ADR sequenced after ADR-009 (cross-process collision detection). ADR-008 is heartbeats; ADR-009 is cross-process collision; ADR-010+ is everything else, including this if it surfaces.
5. **`MaxAckPending` semantics under pcgroups.** Currently NATS-default (1000) per consumer; under pcgroups partitioning, is it per-partition or per-group? Affects in-flight cap math at scale. Verify by reading pcgroups + flow-control test.
6. **Heartbeat direction.** Long-term answer: handlers signal liveness via `msg.InProgress()`. Sub-questions: where does the ticker live (per-message goroutine vs. dispatch-loop tick); what's the contract for handlers to signal "I'm wedged, don't keep me alive"? Needs its own ADR (**ADR-008**). Unification depends on heartbeats, so they sequence as ADR-008 first, ADR-007 second. ADR-007 frontmatter declares `Depends on: ADR-008`.

### §5.3 Reproducible test plan for unification safety

The ADR-007 safety check has **two required parts**, both must pass:

**Part 1 — Parity matrix.** Run the §4 test matrix in two configurations:

| Configuration | What it exercises |
|---|---|
| `partitions=0` (current default path, post-ADR-006) | Baseline — what we ship today |
| `partitions=1` (elastic-degenerate, post-ADR-007 candidate) | The proposed unified shape |

Both must produce identical observable behavior on:

| Observable | Tolerance |
|---|---|
| Redelivery timing (`AckWait`) | ±10% (jitter, scheduler noise) |
| Ack semantics (delivered exactly once on success) | Exact (1:1) |
| Restart recovery (resumes at same offset) | Exact |
| Message ordering (single producer) | Exact |
| Concurrent-startup race (no panic, no duplicate processing) | Exact |
| Migration cleanup (§3 fires as expected) | Exact |

**Part 2 — Forward-compat fixture.** State seeded by an ADR-006 deployment (durables named `workers-<task>`, ephemeral orphans cleaned per §3) → upgrade to ADR-007 → assert no panic, no message loss, durable identity preserved or migrated cleanly.

Without Part 2, a parity-clean unified path could still wedge every existing ADR-006 deployment on first ADR-007 boot.

### §5.4 ADR-007 status

ADR-007 lands with `Status: Proposed` and the §5.2 open questions listed in the doc itself. It moves to `Status: Accepted` only after every open question has a closed answer in the doc.

### §5.5 Cross-process collision detection (deferred)

The in-process collision precheck from §2 doesn't catch the case where worker A registers `render.gpu` and worker B registers `render-gpu` — both sanitize to `workers-render-gpu` and race for the same durable. Two paths:

- **A. Build cross-process awareness into the helper.** On cleanup-list, also detect "consumer with our exact durable name but different filter than ours" → panic with a message naming both filter subjects and the colliding durable. Cheap addition, doesn't catch every race but catches the common one.
- **B. Defer to operational tooling.** `dagnats doctor` or similar that scans the stream and reports name collisions across all configured workers. More flexible, more ops surface.

Closes alongside ADR-007, doesn't block it. Filed as **ADR-009 candidate**.

---

## §6. Risk, rollback, PR checklist

### §6.1 Risk inventory

| Risk | Surface | Mitigation |
|---|---|---|
| Auto-cleanup deletes a consumer that wasn't ours | §3 identification false-positive — future state slips past the 3-prong rule, or rule itself is wrong | 3-prong rule (filter match + ephemeral + not-`workers-*`) is conservative; INFO log on every deletion gives audit trail; fail-loud on any unexpected error; `TestMigration_PreservesUnrelatedConsumer` and `TestMigration_PreservesManagedConsumer` guard the two concrete don't-touch cases |
| Collision precheck panics at startup, blocking deploy | Latent elastic-path collision newly surfaced (strict-gain side effect) | Acknowledged in outcome contract; panic message names originals + colliding durable so operator can act; no NATS state mutated before precheck |
| `AckWait` default of 5min slows crash recovery for sub-second tasks | Per-task override deferred; users with mixed durations bear worst case | User accepted trade-off; per-task override is one-line follow-up; ADR-008 heartbeats is the real fix |
| Pagination iterator regression silently leaves orphans | Future SDK change reverts iterator to single-page, or test removed | `TestMigration_PaginationManyConsumers` (§4.4) guards; failure mode is original #136 panic at `CreateOrUpdateConsumer`, not silent corruption |
| `MaxDeliver: -1` shadowed by future "safety" tweak | Maintainer reads `-1` as a placeholder | Code comment in §1 explicitly names the engine-owns-DLQ contract; `TestSubscribePullConsumer_AppliesExpectedConfig` (§4.6) asserts the value |

### §6.2 Rollback

The PR is contained to `worker/` (and `docs/architecture/` for ADR-006 plus ADR-007 in `Status: Proposed`). **Reverting the merge restores prior code, but does *not* restore prior NATS state** — any deployment that ran the new code created a durable consumer named `workers-<task>` on `TASK_QUEUES`. Post-rollback, the old code creates a new ephemeral consumer with the same `FilterSubject` → original #136 panic.

**Operational rollback procedure (must appear verbatim in the PR description):**

```bash
# Before reverting the deployment, on each environment:
dagnats workers list --task-types | xargs -I{} nats consumer rm TASK_QUEUES "workers-{}"
# Then revert the deployment.
```

Acceptable because: rollback is rare, the operation is local to the affected stream, and `nats consumer rm` is idempotent (NotFound is fine). An operator who reverts without this gets the original bug back, with the same diagnostic — not a new failure mode.

### §6.3 PR checklist

Items that must resolve before merge:

- [ ] `defaultAckWait` doc comment references **ADR-008** (heartbeats), not the placeholder `ADR-XXX`.
- [ ] **ADR-006** committed to `docs/architecture/adr-006-durable-task-queue-consumers.md` with `Status: Accepted`.
- [ ] **ADR-007** committed as `Status: Proposed` with the §5.2 open questions and §5.3 test plan written into the doc, and `Depends on: ADR-008` in frontmatter.
- [ ] `Depends on:` ADR-frontmatter convention either appended to the existing ADR template doc or — if none exists — a thin `docs/architecture/README.md` created naming the convention.
- [ ] If failure-mode tests (§4.8 `TestMigration_*Failure_Panics`) are deferred due to SDK injection cost, the follow-up issue is filed and referenced from the test file's methodology comment. Issue title: *"Add failure-mode tests for migration cleanup (deferred from #136 fix)."*
- [ ] Follow-up issue filed: *"Per-task `AckWait` override via `WithAckWait` handler-registration option"* — captures the deferred per-task knob (co-located with handler, not a parallel map).
- [ ] Follow-up issue filed: *"ADR-009: cross-process consumer name collision detection"* — captures §5.5.
- [ ] Rollback procedure (§6.2) included verbatim in the PR description.
- [ ] Branch is feature, not main. PR awaits manual merge per global CLAUDE.md.
- [ ] Local CI (Go test + vet + staticcheck) green per global CLAUDE.md before declaring ready.

### §6.4 What §6 deliberately doesn't cover

- Implementation sequencing — that's the writing-plans phase, next.
- Performance benchmarks — none warranted at this layer; the change is cheap (one extra `ListConsumers` per subscribe at startup, no per-message overhead).
- Migration timeline / staged rollout — single environment per the user's setup; if multi-env appears, it's a separate ops concern.

---

## Appendix A — File-level changes

Captured for orientation, not as a prescription. Writing-plans owns the actual sequencing.

- `worker/worker.go` — delete `createConsumer`. Add `subscribePullConsumer(taskType, group, handler)` + `consumerNameFor(taskType, group)` + `consumerFilterFor(taskType, group)` + `sanitizeConsumerName(s)` + `assertNoConsumerNameCollisions()` + private constant `defaultAckWait`. Simplify `subscribeTask` callsites (lines ~352, ~371-373) to two-line shape per §1. Wire `assertNoConsumerNameCollisions` into `Start()` before the subscribe loop.
- `worker/worker_test.go` (or new `worker_consumer_test.go`) — full §4 matrix.
- `docs/architecture/adr-006-durable-task-queue-consumers.md` — new file. Distilled version of this spec, formatted as project ADR.
- `docs/architecture/adr-007-unify-consumer-paths.md` — new file. `Status: Proposed`, includes §5.2 open questions verbatim and §5.3 test plan, frontmatter `Depends on: ADR-008`.
- `docs/architecture/README.md` (or equivalent ADR template doc) — adds the `Depends on:` frontmatter convention if not already present.

## Appendix B — Source citations (for the implementer)

- The bug location: `worker/worker.go:385-418` (`createConsumer`), called from `worker.go:352` (default), `worker.go:371-373` (groups).
- The retention policy that triggers it: `internal/natsutil/conn.go:34-40` (`TASK_QUEUES`, `WorkQueuePolicy`).
- Sticky stream that's NOT affected: `internal/natsutil/conn.go:149-157` (`STICKY_TASKS`, default `LimitsPolicy`).
- The elastic naming we mirror: `worker/worker.go:325, 330-331` (`workers-<task>` / `workers-<task>-<group>`).
- GitHub issue: [#136](https://github.com/danmestas/dagnats/issues/136).
- Related but out-of-scope clusters: [#137](https://github.com/danmestas/dagnats/issues/137), [#138](https://github.com/danmestas/dagnats/issues/138).
