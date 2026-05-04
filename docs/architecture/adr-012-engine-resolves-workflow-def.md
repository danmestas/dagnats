# ADR-012: Engine Resolves WorkflowDef from KV at Handle Time

**Status:** Accepted (2026-05-04)
**Deciders:** Dan Mestas
**Closes:** [#167](https://github.com/danmestas/dagnats/issues/167)
**Related:** ADR-011 (engine as sole retry authority), PR for #166 (engine resilience)

## Context

`workflow.started` events flow into the engine from two distinct
producers:

- **`internal/api`** — manual run starts via `dagnats run start`. The
  API service reads the registered `WorkflowDef` from `workflow_defs`
  KV and ships a `{workflow_def, input}` payload.
- **`internal/trigger`** — automatic firings (cron, subject, webhook).
  Every trigger publish path marshals a `TriggerEnvelope`
  (`{trigger, source, timestamp, data}`) as the event payload.

The orchestrator's `handleWorkflowStarted` previously expected an
embedded `WorkflowDef`. Trigger events carried none. The unmarshal
silently produced a zero-valued `dag.WorkflowDef`, which then violated
the constructor invariant `len(Steps) > 0` and panicked the engine
goroutine, taking the entire process down (#166).

The trigger publish paths had three plausible fixes:

1. **Embed the def at publish time.** Each trigger publisher reads
   `workflow_defs` KV before publishing and ships `{workflow_def, input}`.
2. **Resolve at handle time.** The orchestrator looks up the def by
   `WorkflowID` carried in the trigger envelope.
3. **Hybrid.** Some events carry the def, others don't.

## Decision

**The orchestrator resolves the `WorkflowDef` from `workflow_defs` KV
at handle time, by `WorkflowID` carried in the trigger envelope.**

`TriggerEnvelope` gains a `WorkflowID` field; the cron, subject, and
webhook publish paths populate it. The orchestrator's
`handleWorkflowStarted` accepts three payload shapes, in priority order:

1. Structured `{workflow_def, input}` — manual API runs (unchanged).
2. `TriggerEnvelope` carrying `{trigger, workflow_id, ...}` — trigger
   fires. The def is resolved from `workflow_defs` KV; the envelope
   itself becomes the run's `Input` so workflows can observe how they
   were fired.
3. Bare `WorkflowDef` — backward compat for direct callers (tests,
   embedded users predating the structured shape).

A trigger envelope referencing a non-existent workflow produces a
`RunStatusFailed` snapshot (no crash); the engine resilience guarantee
from #166 covers it.

## Rationale

**Pull complexity downward.** The orchestrator already does
`WorkflowDef` lookups for every non-`workflow.started` event via
`loadRunAndDef`. Extending it to also handle `workflow.started` for
trigger-fired runs concentrates def resolution in one place. Each
trigger publisher stays a small, focused module — it knows how to
detect a fire and identify the workflow, but it doesn't know about
`workflow_defs` KV.

**Define errors out of existence.** A trigger publisher that doesn't
look up the def cannot ship a wrong def. The class of "trigger
embedded a stale or empty def" is structurally impossible.

**Smaller event payloads.** Trigger fires no longer carry full DAG
definitions on the history stream; only an envelope. For workflows
with many steps or large schemas, this is meaningful.

**Single source of truth.** `workflow_defs` KV is already authoritative.
Embedding the def in events copies it into history; the copy can drift
or become stale. Resolving at handle time means the most recent
registered def is always used. (For workflows that need def-version
pinning, that's a separate concern best handled explicitly via a
`Version` field rather than implicit history-stream snapshots.)

## Alternatives considered

**Option 1 — Embed at publish time.** Three publish paths each gain a
KV dependency and a synchronous lookup before every fire. Triggers
acquire knowledge of `workflow_defs`, even though their job is to
produce trigger events, not to know about workflow storage. The
unification across publishers requires the same lookup helper in three
places (or a shared one — but then we have a "trigger uses a workflow
lookup helper" coupling regardless). This is the smaller-blast-radius
choice on paper but distributes complexity rather than concentrating it.

**Option 3 — Hybrid.** Whichever event source happens to look up gets
to embed; otherwise the engine resolves. This is the worst of both
worlds: the engine still needs the lookup branch, the publish paths
still need the option to embed, and reviewers must reason about which
shape applies to which event source. Strictly worse than picking one.

## Consequences

**Positive:**

- Trigger publishers stay small and uncoupled from workflow storage.
- The engine becomes the unambiguous boundary for def resolution.
- Permanently-malformed trigger events ACK (after persisting a failed
  snapshot) instead of NAK-looping until `MaxDeliver` runs out.
- Future trigger types (e.g., scheduled runs from external systems)
  inherit correct behavior by setting one field on the envelope.

**Negative / risks:**

- Manual API runs still embed the def. We now have two payload shapes
  in `handleWorkflowStarted`. Mitigated by the priority-ordered
  decoder in `resolveStartPayload`. A future ADR could unify the API
  path onto the resolve-at-handle pattern, eliminating shape #1
  entirely; deferred because the API path is not the load-bearing
  pain point.
- Trigger events on the history stream no longer self-contain the def.
  A consumer that wants to replay history must also have access to
  `workflow_defs` KV. This matches the existing constraint for
  `step.*` events (which already require `workflow_defs` for replay).

## Implementation

- `TriggerEnvelope.WorkflowID` field added in `internal/trigger/types.go`.
- `Scheduler.fireWorkflow`, `SubjectTrigger.publishWorkflowStarted`,
  and `WebhookHandler.publishWorkflowEvent` populate `WorkflowID`
  from `def.WorkflowID`.
- `Orchestrator.resolveStartPayload` (new helper) implements the
  priority-ordered three-shape decoder, including KV lookup.
- `Orchestrator.persistFailedStartRun` (new helper) records a permanent
  failure snapshot for any payload that cannot be resolved into a
  runnable def. Reused for both KV-miss and `dag.Validate` failures.
- E2E tests in `e2e_trigger_resolution_test.go` cover all three
  trigger types end-to-end through `TriggerService` and a real
  `Orchestrator`.
- Engine-level tests in `internal/engine/orchestrator_trigger_resolution_test.go`
  cover the resolution branch and the missing-workflow failure path
  in isolation.

## Replay / migration

- No persisted state changes shape. `workflow_runs` snapshots and
  the `HISTORY` stream are unchanged.
- Any in-flight trigger events published before this ADR landed
  carried no `WorkflowID`. They flow through the bare-`WorkflowDef`
  path, fail validation, and are recorded as failed runs. The engine
  no longer crashes on them (#166), so this is graceful degradation
  rather than a migration step.
