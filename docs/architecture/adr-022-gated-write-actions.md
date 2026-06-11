# ADR-022: Gated write actions (operator-facing infrastructure mutations)

**Status:** Accepted (2026-06-11, #382)
**Deciders:** Dan Mestas
**Depends on:** ADR-014 (control-plane-ui), ADR-021 (ad-hoc-agent-runtimes)

## Context

The console ships seven reversible domain mutations (`dlq.retry`, `dlq.discard`,
`dlq.undo-discard`, `trigger.enable`, `trigger.disable`, `trigger.fire.manual`,
`workflow.run` — `internal/console/actions.go`), gated by `DAGNATS_CONSOLE_MODE`
(ADR-014 `handler.go`), attributed via the auth model (`auth.go`: loopback /
forward-auth / basic / disabled), and recorded to the `console_audit` KV bucket
(90-day TTL, outcomes Success/Denied/Failed — ADR-014).

The new System console views (Streams, Consumers, Connections, Server, KV,
Workers) expose NATS/JetStream infrastructure whose admin operations — purge,
drain, lame-duck, snapshot/restore, consumer delete, KV purge — have far higher
blast radius. Several would corrupt or brick the engine if exposed naively:

- Purging `WORKFLOW_HISTORY`, `EVENTS`, or `console_audit` erases operational
  state and audit trails.
- Deleting the `orchestrator`, `event-correlator`, `sleep-timer`,
  `scheduled-run-timer`, or `console_metrics_aggregator` consumers crashes
  internal subsystems.
- Raw-editing engine-internal KV buckets (`workflow_runs`, `checkpoints`,
  `concurrency_tasks`, `singleton_locks`) can leave tasks orphaned or cause
  deadlocks.

ADR-021 set an observe-first / rotate-not-CRUD stance for runtimes. This ADR
defines the coherent policy for which infrastructure mutations the console
exposes and how each is gated.

## Decision (proposed)

### 1. Engine-owned vs operator-managed boundary

Engine-owned resources are surfaced read-only with destructive affordances
disabled:

- **Never purge or delete:** event-source streams (`WORKFLOW_HISTORY`, `EVENTS`,
  `console_audit`); engine consumers (`orchestrator`, `event-correlator`,
  `sleep-timer`, `scheduled-run-timer`, `console_metrics_aggregator`);
  engine-internal KV (`workflow_runs`, `checkpoints`, `concurrency_tasks`,
  `singleton_locks`, `triggers`, `workflow_defs`).
- **UI label:** "Engine-owned — managed by the orchestrator. Destructive
  operations disabled."
- **Principle:** The safest destructive action is the one the UI will not let
  you start (Ousterhout: define errors out of existence).

### 2. Blast-radius tiers with escalating guardrail ladders

| Tier | Actions | Example | Reversible | Guardrails |
|---|---|---|---|---|
| **Tier 0** | Reversible domain mutations | `run.cancel`, `trigger.enable`, `dlq.retry` | Yes | Read-only mode off |
| **Tier 1** | Graceful / recoverable infra operations | Worker drain, connection drain, server lame-duck, stream snapshot (backup) | Yes / N/A | Read-only mode off |
| **Tier 2** | Destructive but bounded | Stream purge (filtered by subject), KV purge (operator buckets only), consumer redeliver/reset-ack-floor, worker decommission | No, except auto-backup | Read-only off + `DAGNATS_CONSOLE_ALLOW_DESTRUCTIVE=true` + mandatory dry-run preview + typed confirmation + auto-backup before action |
| **Tier 3** | Disaster recovery | Stream restore (overwrites existing) | No | All tier-2 guardrails + two-step confirm + literal "overwrite" + auto-backup-first |

**Guardrail ladder rungs** (each tier adds layers):

1. `DAGNATS_CONSOLE_ALLOW_DESTRUCTIVE=true` flag (off by default) for tier 2–3.
2. Mandatory dry-run/preview showing blast radius before commit.
3. Typed confirmation (resource name; "overwrite" for restore).
4. Auto-backup before tier-2 purge / tier-3 restore.
5. Every action emits a `console_audit` event (params + dry-run-vs-commit +
   outcome; denials logged).

### 3. Authorization model

**Deployment-wide binary flags** (no new identity infrastructure):

- `DAGNATS_CONSOLE_MODE=readonly` (existing) — gates tier 0–1 mutations.
- `DAGNATS_CONSOLE_ALLOW_DESTRUCTIVE=true` — gates tier 2–3 (default: off).

**Additive future step:** A `DAGNATS_CONSOLE_ADMINS` per-user allowlist (checked
against the already-resolved forward-auth identity) is the documented next step
when multi-operator deployments need per-user gating — purely additive, no
schema changes. Full capability/RBAC is explicitly deferred; it composes on top
later (flags/allowlist become the default policy under an RBAC engine), so this
choice forecloses nothing.

**Rationale:** The safety of these actions comes from the guardrail ladder
(dry-run, typed confirm, auto-backup, audit), not from authorization.
Authorization answers "who may attempt"; the ladder answers "can it happen by
accident / can it be undone."

### 4. Reuse existing primitives

Purge actions wrap the existing `dagnats clean --type=… --force` capability
(which already has a dry-run) for identical semantics and shared audit — do not
reinvent purge.

### 5. Action catalog

| Action | Tier | NATS primitive | Console host view | Reversible | Notes |
|---|---|---|---|---|---|
| Worker drain | 1 | Stop pulling, finish in-flight | Worker detail | Yes | New `POST /console/api/workers/<id>/drain` endpoint |
| Worker resume | 1 | Resume pulling | Worker detail | Yes | New `POST /console/api/workers/<id>/resume` endpoint |
| Connection drain | 1 | Conn drain signal | Connections | Yes | New `POST /console/api/connections/<id>/drain` endpoint |
| Server lame-duck | 1 | Server signal `ldm` | Server | Restart required | New `POST /console/api/server/lameduck` endpoint |
| Stream snapshot (backup) | 1 | JS snapshot → object store | Stream detail | N/A (read-safe) | New `POST /console/api/streams/<name>/snapshot` endpoint |
| Consumer redeliver | 2 | `Redeliver(pending)` | Consumers | Yes | New `POST /console/api/consumers/<stream>/<name>/redeliver` endpoint |
| Consumer reset-ack-floor | 2 | Advance floor (can replay) | Consumers | Partial | New `POST /console/api/consumers/<stream>/<name>/reset-ack-floor` endpoint |
| Stream purge | 2 | `PurgeStream()` filtered | Stream detail | No / auto-backup | Wrap `dagnats clean --type=stream --stream=X --force` |
| KV purge | 2 | `Purge()` (operator buckets) | KV | No / auto-backup | Wrap `dagnats clean --type=kv --bucket=X --force` |
| Worker decommission | 2 | Drain + deregister | Worker detail | Re-provision | New `POST /console/api/workers/<id>/decommission` endpoint |
| Stream restore | 3 | JS restore overwrites | Server | No | New `POST /console/api/streams/<name>/restore` endpoint; requires uploaded backup file or object-store reference |

### 6. Audit vocabulary additions

New audit event types emitted to `console_audit` stream:

- `worker.drain`
- `worker.resume`
- `worker.decommission`
- `conn.drain`
- `server.lameduck`
- `stream.backup`
- `stream.purge`
- `stream.restore`
- `kv.purge`
- `consumer.redeliver`
- `consumer.reset`

## Alternatives considered

- **A. Per-user allowlist first.** Deferred: the binary "is this console allowed
  to destroy anything" is the question that matters first; per-user gating only
  matters once that's yes AND multiple trust levels exist.
- **B. Full RBAC.** Rejected as premature subsystem; composes later on top of
  flags + allowlist.
- **C. Pure observe, no infrastructure mutations.** (The ADR-021 extreme)
  Insufficient: operators genuinely need drain, lame-duck, purge, and DR.
- **D. Expose all NATS admin ops uniformly.** Rejected: engine-owned resources
  would become footguns.
- **E. Per-view bespoke confirmations.** Rejected in favor of one unified
  `DangerAction` / `ConfirmModal` pattern.

## Consequences

### Positive

- Bounded, auditable, reversible-first destructive surface.
- Safety decoupled from authorization (guards apply to everyone equally).
- Engine-owned boundary prevents corruption by construction.
- Composes with future RBAC (flags/allowlist become the default policy).
- Auto-backup enables safe recovery.
- Audit trail preserves operator actions for governance.

### Negative

- Most actions require new console endpoints (only tier-0 mutations exist
  today).
- Deployment-wide flag means no per-user gating until allowlist lands.
- Auto-backup needs object-store wiring (new dependency on
  `DAGNATS_OBJECT_STORE` or similar config).
- Stream restore remains genuinely dangerous even gated (can overwrite live
  data). Documented as a last-resort DR action.
- `DAGNATS_CONSOLE_ALLOW_DESTRUCTIVE` is a sharp tool; a careless deployment
  could leave it on. Mitigated by explicit default-off and startup log warnings.

## Rollout

### Phase 1 — Tier 1 (reversible, low-risk)

- Worker drain/resume + connection drain + server lame-duck.
- Stream snapshot (backup) — safe to expose because it only reads.
- Timeframe: alongside ADR-014 polish (PR 6–8).

### Phase 2 — Tier 2 (destructive but bounded)

- Stream purge (wrap `dagnats clean`), KV purge (wrap `dagnats clean`).
- Consumer redeliver + reset-ack-floor.
- Worker decommission.
- `DAGNATS_CONSOLE_ALLOW_DESTRUCTIVE` flag + dry-run previews + typed confirms.
- Auto-backup wiring for object-store.
- Timeframe: after Phase 1 ships and operator feedback arrives.

### Phase 3 — Tier 3 (disaster recovery)

- Stream restore (overwrites).
- Requires object-store read/write and file upload.
- Timeframe: 1–2 releases after Phase 2.

## Open questions

1. **Object-store configuration:** How does dagnats back up streams? Should
   snapshots go to a local tarball, an object-store bucket (S3-compatible, GCS,
   MinIO), or both? Does this require a new config section or env vars?
2. **Snapshot retention:** How long should auto-backups before purge/restore be
   kept? Default TTL?
3. **Allowlist schema:** When per-user gating lands (Phase 1+), should the
   allowlist be env var + file, or KV bucket?

## Cross-references

- ADR-014 (Control plane UI) — the console that surfaces these actions.
- ADR-021 (Ad-hoc agent runtimes) — observe-first / rotate-not-CRUD philosophy.
- `docs/architecture/console-understandability-plan.md` — broader console DX.
