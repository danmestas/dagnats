# ADR-009: Remove the Experimental Actor Orchestrator

**Status:** Accepted (2026-05-02)
**Deciders:** Dan Mestas
**Closes:** [#149](https://github.com/danmestas/dagnats/issues/149)
**Related:** PR #137 (StepState transitions / Attempts counter via lifecycle events)

## Context

The `internal/engine` package carried two parallel orchestrator
implementations:

- `Orchestrator` (`orchestrator.go`) — event-sourced, KV-backed run
  state, wired into every `cmd/` binary (`dagnats serve`,
  `dagnats-engine`).
- `ActorOrchestrator` + `WorkflowActor` (`actor_orch.go`,
  `workflow_actor.go`) — actor-based variant that holds run state
  in-memory inside a per-run actor, snapshotting to KV for durability.

The actor variant was a parallel-implementation experiment. It was
**never** wired into any `cmd/` binary; the only callers of
`NewActorOrchestrator` lived in `actor_orch_test.go` and
`workflow_actor_test.go`.

PR #137 fixed a bug where `state.Attempts` was incremented on
`step.failed` even though lifecycle events (`step.queued` /
`step.started`) already own the counter via a `max()` rule. The fix
landed only on the production `Orchestrator` path. The implementer
flagged that `workflow_actor.go:187` had the same `state.Attempts++`
pattern, but since no production code path ever exercises
`WorkflowActor`, the bug was dormant.

We had two choices:

1. Port the #137 fix into `WorkflowActor` to keep the variants in lockstep.
2. Delete the experimental variant. Resurrect from git history if a
   future actor-model exploration needs a starting point.

## Decision

Delete `actor_orch.go`, `actor_orch_test.go`, `workflow_actor.go`, and
`workflow_actor_test.go`. The `actor/` runtime package (pure Go,
NATS-free) stays — it is general-purpose and not tied to engine.

One engine helper became an orphan and was removed alongside the
actor files: `publishIterationTask` (formerly in `task_publish.go`)
was only called by `WorkflowActor.handleStepContinue`. The remaining
helpers (`enqueueReadySteps`, `findStepDef`, `completedSet`,
`queuedSet`, `checkLoopBounds`, `publishWorkflowEvent`,
`isHandledEventType`) all retain multiple production callers in
`orchestrator.go`, `recovery_manager.go`, `approval.go`,
`advance_exec.go`, `task_publish.go`, and `planner.go`.

## Consequences

- One orchestrator path. No drift between variants.
- The bug fix from #137 is sufficient — no parallel patch needed.
- If we revisit the actor-model architecture, recover the deleted
  files via `git log -- internal/engine/workflow_actor.go` and
  re-apply the #137 fix at the same time.
- Documentation that referenced `ActorOrchestrator` /
  `WorkflowActor` (`agent-system.md`, `serve-command-design.md`,
  `jetstream-api-migration.md`) was updated to point at
  `Orchestrator` and to note the removal.

## Alternatives Considered

**Port the #137 fix and keep the variant.** Rejected. Maintaining two
orchestrators that diverge silently is exactly the cost we wanted to
shed. The actor variant has no consumers and was not on a path to
production use; carrying it forward would just multiply future
maintenance.
