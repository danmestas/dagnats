# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## Unreleased

## [0.0.10] - 2026-07-17

Reliability release addressing **#523** — a bloated `workflow_runs` KV bucket
could stall *all* workflow execution, and there was no bounded way to recover
it. The orchestrator scans pending run snapshots on every completion, and the
reconciler scans the whole run set periodically; both used an all-or-nothing
batch read, so one slow key past the deadline turned into an engine-wide stall.

### Fixed

- **Reconciler survives a bloated run bucket (#523).** Pending-run scans are now
  bounded (cap 1000 keys) and *best-effort*: a slow or missing key is skipped,
  not fatal. A skipped or capped scan emits a `WARN` plus a
  `workflow.runs.scan_degraded` metric so the degradation is observable, and the
  engine keeps making progress while the bucket is oversized.

### Added

- **`clean --keep=N` / `--before-seq=N` bulk prune (#523).** An O(1) server-side
  stream-sequence purge to recover a bucket that has already cliffed past what
  the per-key `--older-than` loop can drain within the deadline. Both are
  force-gated (unless `--dry-run`), mutually exclusive with each other and with
  `--older-than`, and **skip work-queue streams** (`TASK_QUEUES`) so queued
  un-acked tasks are never silently discarded. `--dry-run` never prompts or
  mutates and reads backing-stream sizes so the estimate reflects tombstones the
  KV key count hides. These purge by *sequence, not age* — a blunt recovery
  tool; `--older-than` remains the safe default.

## [0.0.9] - 2026-07-17

Bug-fix release addressing **#521** — unbounded run storage and two
`dagnats clean` failures.

### Fixed

- **Bounded run retention by default (#521).** Terminal run snapshots in the
  `workflow_runs` KV now age out after 30 days by default (matching
  `WORKFLOW_HISTORY`), so run storage stays bounded without operator
  intervention. The pruner is terminal-only — in-flight runs are never touched,
  preserving snapshot-first recovery. Configure with `DAGNATS_RUNS_MAX_AGE`: a
  window (`720h`, `30d`, `2w`) or `0`/`off` to disable.
- **`clean` no longer errors on work-queue streams (#521).** `clean
  --older-than` now skips `TASK_QUEUES` (a work queue holds live pending work,
  not history) instead of failing with a pull-consumer ack-policy error
  (`10084`). A full `clean` still drains it.
- **`clean` no longer times out deleting run keys (#521).** Full bucket clears
  use an O(1) backing-stream purge; the `--older-than` path bounds each delete
  with its own timeout and stays cancellable.

### Changed

- **Run pruner is now on by default (30d).** An upgraded server begins deleting
  terminal runs older than 30 days, so they disappear from the console
  (consistent with the 30d history window). Set `DAGNATS_RUNS_MAX_AGE=off` to
  keep the previous keep-forever behavior.

## [0.0.8] - 2026-07-16

Hardens run-dispatch reliability, eliminates the CI test-harness flakes, and
brings the full dependency stack current — clearing all outstanding security
advisories.

### Fixed

- **WORKFLOW_HISTORY poison-loop backstop (#508):** the orchestrator history
  consumer now has a bounded `MaxDeliver` with a redelivery schedule and
  dead-letters exhausted handler-error events to `DEAD_LETTERS` instead of
  NAK-looping forever; a dead-letter-publish failure NAKs rather than silently
  dropping the event.
- **Fan-out span differentiation (#513):** map-instance (fan-out) enqueue spans
  now carry the real `workflow_name` without regranting control-plane
  capabilities — the capability-grant key and telemetry label are decoupled (the
  #380 deny-by-default invariant is preserved and regression-tested).
- **Cleaner shutdown:** `Orchestrator` / `Correlator` / `SleepTimer` `Stop()`
  now wait (bounded) for their JetStream consume loop to fully quiesce instead
  of only signaling it, closing a lifecycle race where a goroutine could still
  be mid-fetch after `Stop()` returned.
- **CI flakes fixed at the root** — both OS ephemeral-port / temp-dir races in
  shared test helpers: `StartTestCluster` cross-call port collision (#516) and
  `StartTestServer` `StoreDir` cleanup race (#520). No more rerun-to-green.

### Security

- **All 14 Dependabot alerts cleared** (7 critical / 2 high / 5 moderate):
  `golang.org/x/crypto` v0.49.0 → v0.54.0 (critical SSH auth-bypass /
  key-constraint / FIDO-U2F) and `golang.org/x/net` v0.52.0 → v0.57.0.

### Changed

- **OpenTelemetry** → v1.44.0 / v0.20.0 (coordinated across the tree).
- **NATS** server → v2.14.3, client (`nats.go`) → v1.52.0,
  `orbit.go/jetstreamext` → v0.3.1 — changelog-reviewed to confirm the JetStream
  consumer/advisory semantics this release depends on are unchanged.

## [0.0.7] - 2026-07-15

Closes the observability gaps blocking downstream freshness/trace tooling and
hardens run-dispatch durability. All six changes since v0.0.6.

### Added

- **Per-trigger `last_fired` / `next_fire` metrics (#501)** — the scheduler
  exposes `trigger_last_fired_seconds` and `trigger_next_fire_seconds` gauges so
  a consumer can detect a *missed* cron fire without hardcoding each schedule.
- **Differentiated task/enqueue span names + `workflow_name` (#503)** —
  enqueue/execute spans are named per task/type and carry the workflow name, so
  traces are no longer collapsed into one undifferentiated wall.
- **Trigger-fire spans + trace propagation (#504)** — cron fires start a
  `trigger.fire` span and propagate context to the engine, giving trigger
  identity and a proper trigger→engine parent hop.
- **Guaranteed W3C propagator in workers (#505)** — `NewWorker` installs a W3C
  TraceContext+Baggage propagator when none is configured, so incoming
  `traceparent` headers are no longer silently dropped.

### Fixed

- **Durable visibility for singleton-skipped runs (#502)** — a run skipped by
  singleton admission now persists a terminal record instead of silently
  vanishing (acked but never dispatched/findable); a snapshot-write failure NAKs
  for redelivery rather than dropping the run.
- **Deterministic supercluster test ports (#509)** — fixed a port-collision
  flake that surfaced as ~500s CI readiness timeouts.

## [0.0.6] - 2026-07-10

### Fixed

- **`worker.Stop()` now drains in-flight handlers before returning (#498 /
  #499)** — fixes the shutdown `sql: database is closed` race.

## [0.0.5] - 2026-06-25

A feature release — 14 PRs since `v0.0.4`. Two headlines: **runtime-generated
workflows** (agent runtimes, ADR-021 Phase A) — gated task handlers can author
and launch brand-new DAGs at runtime, bounded on every axis — and **nats-micro
service discovery** for the internal control plane. Also lands honest run
listing, opt-in run retention, and the `dagnats-ci` add-on scaffold.

### Added

Agent runtimes (ADR-021 Phase A) — runtime-generated workflows:

- **`ControlPlane` handle** on gated task handlers: `RegisterWorkflow` authors an ephemeral workflow def at runtime and `StartRun` launches a child run of it, so an LLM planner can compose known tools into a *novel* DAG and execute it durably — crash-recoverable like any other run. Opt-in and deny-by-default (#459, #376).
- **Generation-tree lineage**: every spawned run is namespaced under its root run (`agent.<root>.*`); a bounded, idempotent reaper GCs ephemeral defs after the root run is terminal, plus promotion wiring for durable (`promoted.*`) defs (#460, #377).
- **Per-runtime safety bounds**: per-tree quotas (max active runs, max ephemeral defs), a generation-depth cap (≤ the engine nesting ceiling), a register rate limit, and a `Budget()` method so a handler can self-throttle *before* hitting a quota (#461, #378).
- **Capability-grant security model**: deny-by-default `policy.control_plane.grant` / `promote` lists, a per-dispatch **nonce** binding each request to the run the worker is actually executing, promotion authorization, and an audit record for every grant decision and control-plane mutation (#462, #380).
- **Console → Agent runtimes** view: per-tree generation lineage, per-runtime budget consumption, and run **provenance** (a "runtime" tag on agent-spawned runs), live over SSE (#463, #379).

Service discovery (nats-micro):

- The internal control plane now runs as discoverable **`micro.Service`s** — `dagnats-api` (#456) and `dagnats-trigger` (#458) — answering the reserved `$SRV.PING` / `$SRV.INFO` / `$SRV.STATS` protocol, with a live **Console → Services** page sourced from `$SRV` discovery rather than a static registry (#457). All under the `#449` umbrella.

Other:

- **Run retention**: opt-in, drop-only retention sweeper (`runs_max_age` / `DAGNATS_RUNS_MAX_AGE`, disabled by default) (#455, #453).
- **`dagnats-ci` add-on module**: scaffold with a `ci.yml` compiler + webhook core, as its own nested Go module (#451).
- **Public `dagnatsext` worker seam** + per-step task metadata (#450).
- **Docs**: runtime-generated-workflows + service-discovery guides, README + configuration refresh, and ADR-021 Phase A implementation status (#464); auto-generated SDK reference docs refreshed with a CI drift guard that pins gomarkdoc's source-link ref so output is reproducible on CI's detached-HEAD checkout (#465).

### Changed

- The control-plane request/reply **subjects are unchanged** by the nats-micro adoption — every existing caller keeps working. The `micro.Service` wrapper only adds the discovery + per-endpoint statistics surface, and fan-out is preserved (no queue group, matching the prior plain-subscribe behavior) (#456, #457, #458).

### Fixed

- **CLI `run list` is now honest**: a globally time-ordered listing across all workflows, plus a run count and a `--since` filter, instead of the previous per-workflow truncated view (#454, #452).

## [0.0.4] - 2026-06-18

A bug-fix + console-completion release — 9 PRs since `v0.0.3`. Resolves two
firestorm production bugs (unbounded JetStream/heap growth; cron fire-record
"zombie" duration) and completes the engine-gated console telemetry/trace
surfaces (p50, Trace-ID/duration, waterfall).

### Added

Console / web UI:

- **Trace detail**: a span **waterfall** (per-span offset/width geometry from the existing span data) + a clickable **span-detail KV panel** (span/parent/workflow/step/task/run id + status), with a disclosure caret (#444).
- **Traces list**: real **Trace ID** + **Duration** columns (#443).
- **Triggers list**: inline **enable/disable toggle** (reusing the existing toggle route, read-only-gated) (#439).
- **Audit page**: **outcome filter** (success/denied/failed), a **denied-attempts callout**, and an **identity/auth-mode banner** (#439).

### Changed

- **Run completion is now honest across every terminal path.** `dag.WorkflowRun` gains `TraceParent` + `CompletedAt`; all 8 terminal transitions (complete/fail/cancel/loop-fail/map-fail/schema-fail/failed-start/compensated) funnel through a single `markTerminal` helper that stamps `CompletedAt` — no path can forget it (#443).
- **JetStream streams are now bounded.** History streams get `max_age` (WORKFLOW_HISTORY 30d, EVENTS 14d, DEAD_LETTERS 30d) + proportional `max_bytes` ceilings (a fraction of `JetStreamMaxStore`, so they scale from a 2 GiB host to 10 GiB+); `TASK_QUEUES`/`SLEEP_TIMERS` keep **no `max_age`** (they hold live/pending work) (#441, #446, #447).
- Embedded NATS server now sets `JetStreamMaxMemory` + a soft `GOMEMLIMIT` (`debug.SetMemoryLimit`) via a new `MaxMemoryBytes` config / `DAGNATS_MAX_MEMORY_BYTES` env, so the Go heap returns to the OS (#446).
- The snapshot-p50 tile is **neutral-colored** (snapshot-save isn't a run-latency SLO) (#442).
- The full test suite runs at bounded package parallelism (`-p 4`) so it's deterministic on high-core machines (#437).

### Fixed

- **JetStream store + heap grew unbounded under workqueue churn** (firestorm #441): the real cause was *unbounded history streams* (not the workqueue, which deletes on ack). Now bounded by `max_age` + proportional `max_bytes` + a memory limit. Verified recovery-safe (the orchestrator is snapshot-first; the `workflow_runs` KV is authoritative).
- **Cron fire-record duration ticked up forever** after a run completed / engine restart (firestorm #440): `enrichFireStatus` computed `time.Since(CreatedAt)`; now frozen at `CompletedAt − CreatedAt`.
- **Snapshot p50 never reached the console**: the metrics aggregator typed OTel `Temporality` as `int`, but the SDK serializes it as a string — so every histogram/sum record failed to decode and was silently dropped. Fixed the decoder (#442).

### Documentation

- Preserved the console design mockup (the deleted MagicPath source) in-repo under `docs/design/mockup/` so it's version-controlled and backed up (#438).

## [0.0.3] - 2026-06-16

A console (web UI) release — ~60 commits since `v0.0.2`, bringing the operator
console into fidelity with the design mockup and fixing two observability
correctness/resilience bugs.

### Added

Console / web UI:

- New pages: **Server** (NATS identity + JetStream account, live Varz/Jsz), **Connections** (`/connz`), **Consumers** (work-queue health), **Concurrency** (admission-control: slot pools, singleton locks, rate-limit + debounce gates), **Services** roster, **Traces** (cross-run + deep-linkable) with a per-run Trace tab, read-only **Worker detail** and **Function detail**, **KV** catalog + value inspector, **Config** self-portrait (access posture + engine invariants).
- **Dashboard** reshaped to the mockup: two-row layout (status tiles + telemetry sparkcards for throughput / p50 / error rate), recent-failures table, datatype sparklines, nav badges.
- **Workflow detail**: numbered step-DAG (type pills + `depends_on` edges) and a **Run workflow** action.
- **Logs**: dedicated Trace-ID column linking to traces.
- **Triggers** Add / Edit / Delete and per-run **Signal / Cancel** actions (existing API, read-only gated).
- Design foundation: teal / IoskeleyMono / borderless cards, Lucide SVG nav icons + collapse-to-icon rail, `dagnats://` wordmark, muted status palette matched to the mockup, IA grouped into Inventory / Activity / System.
- `dagnats demo seed --keep-alive`: rich demo mode for populating a console for review (#425).

### Changed

- **Metric export is now cumulative temporality** (was delta) with a 10s NATS reader interval, so the console's rate/sparkline/chart math (which assumes monotonic counters) renders correctly (#434).
- Real ldflags build-stamp surfaced in the console footer (#420).
- Nav IA reorganised; the Leases page and the Ops hub were removed/consolidated.

### Fixed

- **Metrics pump now uses an ephemeral consumer** — a durable consumer with an immutable start-time previously failed (`nats 10012`) on every engine restart and silently disabled all console metrics; restarts now keep the observability surface alive, and the legacy durable is cleaned up on upgrade (#435).
- Active-runs tile no longer shows a negative count (sourced from real run state) (#427); dashboard throughput no longer renders `-0.0`; the broken/garbled metrics throughput chart now draws correctly (#426, #429).
- Readable detail-page values (no longer near-black on dark) (#428); transparent table headers + consistent hover/focus/active states (#405); run-detail underline tabs (#423, #424).
- `observe`: `buildResource` honors `OTEL_RESOURCE_ATTRIBUTES` / `OTEL_SERVICE_NAME` via `resource.New` + `WithFromEnv()` (`cfg.Resource` still wins); `LogExporter` derives `service.name` from the record resource (#367, #368).
- `dag`: `sub_workflow` treated as a no-task step type (#371); `serve` fail-fast flag + loopback-preserving port fallback (#372); retired `/ui` stub redirects to `/console/` (301) (#366); HTTP-bridge workers propagate `AttemptNumber` so retry backoff + dead-lettering work (#384).

### Honesty discipline

- Mockup features lacking backing data were **omitted, not fabricated** (e.g. per-entity stat tiles, Services instance/version columns, snapshot-p50). Traces-list trace-id/service/duration and trace-detail waterfall geometry remain engine-gated and intentionally unbuilt.

## [0.0.2] - 2026-06-03

A large console (web UI) and engine release — ~77 commits since `v0.0.1`.

### Added

Console / web UI:

- Logs page with trace-ID search; Task Types registry page; Configuration self-portrait page.
- Workers / KV / Streams promoted to top-level navigation; collapse-to-icons rail with footer strip.
- Fire-now trigger button backed by a `FireTrigger` HTTP endpoint; inline Run button on workflow rows.
- Page-header partials with tile counters, empty-state partials, drill chevrons, build/identity footers.
- IBM Plex Sans/Mono typography (bundled OFL-1.1).
- NATS WebSocket listener for browser clients (live UI updates).

Engine / triggers / workers:

- `dagnats.yaml` configuration file with hot-reload.
- Trigger-type system: external trigger variant + schema validation, trigger-type versioning, `trigger_types` KV bucket + `TriggerTypeDef`, `RegisterTriggerType` / `WatchTriggers` SDK, `trigger-type list/describe` CLI, and `ExternalRegistrar`.
- Services registry: `services` KV + `RegisterService` SDK.
- `WorkerRegistration` enriched with identity + heartbeat fields.
- filewatcher external-trigger example.

### Changed

- Observability: raw publishes routed through `TracingPublisher`; handler-extractor wrapper.
- `TriggerRegistrar` interface + table-driven trigger dispatch.

### Fixed

- Numerous console fixes: dashboard tile rendering on empty metrics, run-detail SSE patches, connection-pill state, CSP fixture gating, print CSS, and empty-bucket workflow listing.

## [0.0.1] - 2026-05-03

Initial public release of `dagnats`, a workflow orchestration engine combining
DAG-based task graphs with NATS-backed coordination. Single-binary deployable
with embedded NATS server and webhook/cron triggers. Supersedes internal
pre-release tags `v0.1.0` and `v0.1.1`, which were never published.

### Added

- DAG-based workflow definition and validation engine.
- Embedded NATS JetStream server (no external broker required).
- Worker, server, CLI, sidecar, bridge, and SDK packages.
- Webhook and cron trigger sources with `backfill` semantics.
- Lazy orchestrator subsystem initialization (ADR-004).
- Apache-2.0 LICENSE.
- Auto-sync of landing-page version from the latest git tag.
- Step lifecycle events (`step.queued`, `step.started`, `step.completed`, `step.failed`) with deterministic ordering and an `AttemptNumber` semantic ([#137](https://github.com/danmestas/dagnats/issues/137)).
- Engine-side retry-backoff scheduler honouring per-policy delays ([#147](https://github.com/danmestas/dagnats/issues/147)).
- Step-level timeout watchdog with staleness checks ([#140](https://github.com/danmestas/dagnats/issues/140)).
- Per-task `WithAckWait` handler-registration option ([#144](https://github.com/danmestas/dagnats/issues/144)).
- Cross-process consumer name collision detection ([#145](https://github.com/danmestas/dagnats/issues/145), ADR-010).
- Worker durable consumers on `TASK_QUEUES` with orphan ephemeral migration ([#136](https://github.com/danmestas/dagnats/issues/136), ADR-006).
- Multi-stage `Dockerfile` and cross-platform release binaries via `make release`.

### Changed

- Engine is now the sole retry authority. Workers report failures via `step.failed`; the engine schedules backoff and dispatches the next attempt (ADR-011).
- Generic worker handler errors now publish `step.failed` (retriable) and Ack the message instead of NAKing with a hardcoded 5s delay ([#141](https://github.com/danmestas/dagnats/issues/141)).
- Removed the experimental `ActorOrchestrator` / `WorkflowActor` (ADR-009) — single orchestrator path going forward.

### Fixed

- Step state correctly transitions to `Running` when a worker pulls the task ([#137](https://github.com/danmestas/dagnats/issues/137)).
- Retriable `step.failed` now schedules a retry instead of leaving the run wedged at `attempts: 1/N` ([#147](https://github.com/danmestas/dagnats/issues/147)).
- Step `Timeout` now fires a watchdog instead of being a silent no-op ([#140](https://github.com/danmestas/dagnats/issues/140)).
- Fast-failing worker handlers no longer leave runs in `running, attempts: 0/N` ([#141](https://github.com/danmestas/dagnats/issues/141)).
- `Worker.Stop()` logs directory deregistration failures instead of swallowing them.
- Cron triggers with `backfill: false` no longer fire on registration ([#139](https://github.com/danmestas/dagnats/issues/139)).
- Workflow run input correctly forwards to root steps.

### Documentation

- ADR-006: durable task-queue consumers.
- ADR-009: remove experimental actor orchestrator.
- ADR-010: cross-process consumer name collision detection.
- ADR-011: engine as sole retry authority.

### Tests

- Regression guards for multi-task-type concurrency ([#138](https://github.com/danmestas/dagnats/issues/138)), `dagnats run start --json` ([#143](https://github.com/danmestas/dagnats/issues/143)), and `ListRunEvents` step-event inclusion ([#142](https://github.com/danmestas/dagnats/issues/142)).
- End-to-end test suites for retry-backoff, fail-fast, step-timeout, and `publishStarted` NAK-recovery paths.
