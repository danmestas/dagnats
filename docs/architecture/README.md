# Architecture Decision Records

Two kinds of files live here:

- **ADRs** (`adr-NNN-*.md`) — load-bearing decisions with context, alternatives, and consequences. Numbered sequentially, never renumbered. See `CLAUDE.md` for the project-wide convention.
- **Design notes** (everything else, e.g., `core-design.md`) — background reading. May be superseded by later ADRs; check the file header for status.

## ADR frontmatter conventions

Every ADR begins with a YAML-style frontmatter block:

```
**Status:** Proposed | Accepted | Superseded
**Deciders:** <names or TBD>
**Depends on:** <ADR-NNN, optional>
**Spec reference:** <relative link to spec, optional>
**Issue:** <link to GitHub issue, optional>
```

### `Depends on:` semantics

When ADR-X declares `Depends on: ADR-Y`:

- ADR-X cannot reach `Status: Accepted` until ADR-Y is accepted.
- ADR-X's Decision section may reference primitives, contracts, or invariants established only by ADR-Y. Reviewers should not require ADR-X to re-prove those.
- If ADR-Y is Superseded, ADR-X must be revisited and either updated or marked Superseded as well.

This convention makes dependency between proposals explicit and prevents accidental forward-references that paper over real sequencing problems.

## Currently active ADRs

- `adr-001-agent-harness-gaps.md` — interface gaps in the agent harness.
- `adr-002-durable-agent-loop.md` — durable agent loop via dagnats primitives.
- `adr-003-sidecar-dx-improvements.md` — sidecar DX improvements.
- `adr-004-lazy-orchestrator-subsystems.md` — lazy orchestrator subsystems.
- `adr-005-embedded-nats-cluster-mode.md` — embedded NATS cluster mode.
- `adr-006-durable-task-queue-consumers.md` — durable consumers on TASK_QUEUES (this fix).
- `adr-007-unify-consumer-paths.md` — unify default + elastic paths (Proposed).
- `adr-009-remove-experimental-actor-orchestrator.md` — delete unused `WorkflowActor` / `ActorOrchestrator`, single orchestrator path.
