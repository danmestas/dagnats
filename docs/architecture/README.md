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
- `adr-006-durable-task-queue-consumers.md` — durable consumers on TASK_QUEUES.
- `adr-007-unify-consumer-paths.md` — unify default + elastic paths (Proposed).
- `adr-009-remove-experimental-actor-orchestrator.md` — delete unused `WorkflowActor` / `ActorOrchestrator`, single orchestrator path.
- `adr-010-cross-process-consumer-collision-detection.md` — runtime check that panics when a different process already owns our durable name with a different filter subject.
- `adr-011-engine-sole-retry-authority.md` — engine owns Attempts, retry timing, and timeout enforcement; closes #141, #147, #140.
- `adr-012-engine-resolves-workflow-def.md` — engine resolves the WorkflowDef from KV at handle time.
- `adr-013-http-trigger-respond-step.md` — synchronous HTTP triggers + respond step.
- `adr-014-control-plane-ui.md` — embedded control plane UI at `/console/` with loopback-trust auth.
- `adr-015-operator-first-console-reskin.md` — operator-first console re-skin (supersedes ADR-014's aesthetic).
- `adr-016-trigger-registrar.md` — trigger registrar interface.
- `adr-017-services-namespace.md` — services namespace.
- `adr-018-dagnats-yaml-hot-reload.md` — declarative workflows and triggers in `dagnats.yaml`, with hot reload.
- `adr-020-nats-websocket-browser.md` — NATS WebSocket listener for browser clients.
- `adr-021-ad-hoc-agent-runtimes.md` — ad-hoc agent runtimes, a scoped generative control-plane capability.
- `adr-022-gated-write-actions.md` — gated, operator-facing write actions.
- `adr-023-accept-dagnatstest-internal-api-coupling.md` — accept the `dagnatstest` → `internal/api` coupling for now.
- `adr-024-leases.md` — coordination leases (leader election, shard ownership) over `api.leases.*` request/reply, with revision fencing and server-republished change events.
- `adr-025-fenced-step-events.md` — lifecycle events carry iteration, attempt and dispatch nonce; the engine drops stale ones; agent state rides in `step.continue`.
- `adr-026-agent-journal.md` — a fenced, append-only journal per agent step: leader tail reads, claims, single-message commits, recovery and the `journal check` audit.
- `adr-027-agent-inputs-and-waits.md` — idempotent inputs with steering, placed through the journal, and durable waits (`Delay`, `Until`, `UntilInput`) that hold no worker.
- `adr-028-cancellation-crash-loops-spawn.md` — cancel reaches running handlers, a crash-loop guard, idempotent child spawn.
- `adr-029-hosting-pi-durable.md` — running pi-durable agents as dagnats steps on a JetStream storage adapter (Proposed, deferred).
