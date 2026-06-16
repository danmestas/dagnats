# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## Unreleased

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
