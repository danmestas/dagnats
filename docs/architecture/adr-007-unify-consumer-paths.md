# ADR-007: Unify Default and Elastic Consumer Paths

**Status:** Proposed (2026-05-01)
**Deciders:** TBD
**Depends on:** ADR-008 (in-handler heartbeats; tracked as follow-up issue)
**Spec reference:** [`docs/superpowers/specs/2026-05-01-issue-136-durable-task-queue-consumers-design.md`](../superpowers/specs/2026-05-01-issue-136-durable-task-queue-consumers-design.md) §5

## Context

After ADR-006, `subscribeTask` has two consumer-creation paths: the default branch (`subscribePullConsumer`, plain pull consumer) and the elastic branch (`createElasticConsumer`, pcgroups-managed elastic consumer group with N partitions). The default branch is logically the elastic branch with `partitions=1`. Two paths means twice the surface area for bugs, twice the surface area for migrations, and the inevitable drift between what each path supports.

ADR-006's collision precheck closed one side of this drift (latent collisions in the elastic path now panic at `Start()`). The next step: collapse to a single path so future changes only touch one place.

## Decision (proposed)

Replace `subscribeTask` with a single elastic-based shape:

```go
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

`createElasticConsumer` keeps the deeper-helper shape from ADR-006 §1: takes `(taskType, group, partCount, handler)` and derives naming, filter subject, AckPolicy, DeliverPolicy, AckWait, MaxDeliver internally. `subscribePullConsumer` and `createConsumer` are deleted. Migration cleanup either moves into `createElasticConsumer` or retires (depends on Open Questions §1, §2 below).

ADR-007 lands as **Proposed** until every Open Question has a closed answer. It moves to **Accepted** when:

1. Open Questions §5.2 from the spec all have falsifiable closed answers in this doc.
2. The §5.3 reproducible test plan passes (parity matrix + forward-compat fixture).

## Open Questions (must close before promotion to Accepted)

Each question is falsifiable. The answer is observable, not opinion.

1. **What does pcgroups do at `partitions=1`?** Does it short-circuit to a single consumer with no partition-routing overhead, or run a degenerate group lifecycle every time? Investigate via reading pcgroups source + a microbenchmark of `partitions=1` vs. plain pull consumer creation. Result: numbers + acceptance verdict.

2. **Are pcgroups consumers nameable to match `workers-<task>` exactly, or does pcgroups impose its own naming?** If it prefixes/suffixes (e.g., `workers-render-p0`), the migration story changes. Verify by creating one and inspecting `nats consumer ls`.

3. **Migration story.** ADR-006 deployments have durable consumers named `workers-<task>`. If ADR-007 produces differently named consumers, every ADR-006 deployment hits the same panic class on first ADR-007 startup. Either (a) names match (no-op), or (b) names differ and the ADR-006 §3 cleanup logic generalizes to also delete previous-shape durables. Falsifiable: roll an ADR-006 deployment forward to ADR-007 in a test fixture, assert no panic, no message loss.

4. **Sticky consumer interaction.** Sticky uses `STICKY_TASKS` (different stream, `LimitsPolicy`). Does ADR-007 fold sticky into elastic, leave sticky alone, or eliminate sticky entirely? Out of scope for ADR-007; if the answer is "fold sticky in," that becomes its own ADR sequenced after ADR-009 (cross-process collision detection). For now: leave sticky alone.

5. **`MaxAckPending` semantics under pcgroups.** Currently NATS-default (1000) per consumer; under pcgroups partitioning, is it per-partition or per-group? Affects in-flight cap math at scale. Verify by reading pcgroups + flow-control test.

6. **Heartbeat direction.** Long-term answer to `AckWait`-as-workload-knob: handlers signal liveness via `msg.InProgress()`. Sub-questions: where does the ticker live (per-message goroutine vs. dispatch-loop tick); what's the contract for handlers to signal "I'm wedged, don't keep me alive"? Needs its own ADR (**ADR-008**, tracked as follow-up issue). Unification depends on heartbeats — they sequence as ADR-008 first, ADR-007 second. Hence the `Depends on: ADR-008` in this ADR's frontmatter.

## Reproducible test plan for unification safety

The promotion-to-Accepted check has **two required parts**, both must pass.

### Part 1 — Parity matrix

Run the ADR-006 §4 test matrix in two configurations:

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
| Migration cleanup (ADR-006 §3 fires as expected) | Exact |

### Part 2 — Forward-compat fixture

State seeded by an ADR-006 deployment (durables named `workers-<task>`, ephemeral orphans cleaned per ADR-006 §3) → upgrade to ADR-007 → assert no panic, no message loss, durable identity preserved or migrated cleanly.

Without Part 2, a parity-clean unified path could still wedge every existing ADR-006 deployment on first ADR-007 boot.

## Consequences (when Accepted)

**Positive:**
- One consumer-creation path. Future changes touch one place.
- Elastic features (partitioning, group rebalancing) become uniformly available even at `partitions=1`.

**Negative:**
- Forward-compat migration cost (covered by Part 2 of the test plan).
- Possible pcgroups overhead at `partitions=1` if Open Question §1 closes badly.

**Neutral:**
- Sticky path is unchanged (Open Question §4 explicitly defers).

## Cross-process collision detection (deferred, alongside this ADR)

The in-process precheck from ADR-006 §2 doesn't catch the case where worker A registers `render.gpu` and worker B registers `render-gpu` — both sanitize to `workers-render-gpu` and race for the same durable. Two paths:

- **A. Build cross-process awareness into the helper.** On cleanup-list, also detect "consumer with our exact durable name but different filter than ours" → panic with a message naming both filter subjects and the colliding durable. Cheap addition, doesn't catch every race but catches the common one.
- **B. Defer to operational tooling.** `dagnats doctor` or similar that scans the stream and reports name collisions across all configured workers. More flexible, more ops surface.

Closes alongside ADR-007, doesn't block it. Filed as **ADR-009 candidate** (follow-up issue tracked separately).
