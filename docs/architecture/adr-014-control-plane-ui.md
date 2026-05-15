# ADR-014: Embedded Control Plane UI

**Status:** Accepted
**Deciders:** Dan Mestas
**Companion to:** ADR-013 (HTTP trigger + respond step + Scalar `/docs`)

## Context

dagnats today exposes its operational surface through (a) a CLI, (b) a REST API,
(c) raw `nats` CLI access to the underlying streams, and as of ADR-013 (d) a
Scalar-rendered OpenAPI explorer at `/docs`. The fourth surface — Scalar —
answers a specific question for a specific user (an API consumer wanting to
understand the HTTP-trigger contract). It doesn't answer the operator's
questions: *is the system healthy, what ran today, what's failing, what's in
flight, what's stuck in DLQ?*

A control plane UI would close that gap. This ADR scores the current operator
experience using the dx-audit framework, ranks the highest-leverage
improvements, and proposes a UI architecture that is **deliberately
lightweight** — no build step contributors touch, no npm in CI, no SPA
framework — because dagnats's value proposition is the single-binary
single-dependency posture, and the control plane UI must not break it.

Prior art exists at both ends of the weight spectrum. Hatchet and Inngest ship
React-based dashboards that require build pipelines and ship hundreds of
kilobytes (sometimes megabytes) of JavaScript. Faktory and Sidekiq, at the
other extreme, ship a few hundred lines of server-rendered HTML with
sprinkle-on JavaScript and have served high-traffic production systems for over
a decade. dagnats lives ideologically at the Faktory end — and the technical
state of Datastar + SSE + Basecoat has moved enough since Sidekiq's era that we
can land "Hatchet UI fidelity at Faktory's footprint" without compromising
either.

### DX audit of the current operator surface

Following the dx-audit skill methodology: enumerate workflows, score them,
identify leverage. Audit is from the perspective of a production operator
running dagnats (not from a workflow author writing tests — that's a different
audit covered in `dx-tooling.md`).

#### Operator workflows, enumerated by frequency

| # | Workflow | Frequency | Current path |
|---|---|---|---|
| 1 | Check system health | Daily | `dagnats status` |
| 2 | Debug a failed run | Daily | `dagnats run list --status=failed`, then `run inspect <id>`, then `run events <id> --full` |
| 3 | Inspect run output | Daily | `dagnats run output <id>` |
| 4 | Trigger a manual run with custom input | Daily | `dagnats run start <wf> '<json>' --watch` |
| 5 | Monitor active runs | Daily | `dagnats run list --status=running`, re-run periodically |
| 6 | Register a new workflow | Weekly | Write JSON, `dagnats workflow register file.json` |
| 7 | Add or modify a trigger | Weekly | For cron: CLI. For others: edit workflow JSON, re-register |
| 8 | Replay DLQ entries after a fix | Weekly | `dagnats dlq list`, `dagnats dlq replay <seq>` per entry |
| 9 | Review trigger fire history | Weekly | `nats stream view TRIGGER_HISTORY` directly |
| 10 | Monitor throughput / metrics | Continuous | None (no first-class tooling) |
| 11 | Watch for consumer lag / capacity | Continuous | `nats consumer info TASK_QUEUES <name>` per consumer |
| 12 | Inspect cross-workflow signals | Continuous | `nats kv get signals` manually |
| 13 | Deploy or upgrade dagnats | Rare | Stop, replace binary, start |
| 14 | Diff workflow versions | Rare | None (no tooling) |
| 15 | Backup / restore NATS state | Rare | Standard NATS JetStream procedures |

#### Workflow scoring

| # | Workflow | Frequency | Score | Biggest gap |
|---|---|---|---|---|
| 1 | Check system health | Daily | 8/10 | Snapshot only; no live view. Re-running `status` every few seconds is the operator's "live dashboard." |
| 2 | Debug a failed run | Daily | **5/10** | Three CLI hops. `events --full` is wall-of-text. No DAG visualization. No fast "show me the failed step's input/output" view. |
| 3 | Inspect run output | Daily | 9/10 | Solid. Friction is in knowing the run_id; the command itself is fine. |
| 4 | Trigger a manual run | Daily | 7/10 | JSON-on-command-line is awkward for non-trivial inputs. `--watch` is line-oriented. |
| 5 | Monitor active runs | Daily | **4/10** | No live updates. Polling by re-running `run list`. Small terminals truncate columns. |
| 6 | Register a new workflow | Weekly | 7/10 | Solid once you know the schema. First-time experience requires reading the schema. Validator warnings surface in CLI output. |
| 7 | Add / modify a trigger | Weekly | **5/10** | Two distinct mechanisms (CLI for cron, JSON for others). Drift between the workflow definition and the live KV state is invisible. |
| 8 | Replay DLQ after fix | Weekly | 6/10 | No bulk select. No "replay all from last 24h." No preview of what will be replayed. |
| 9 | Review trigger fire history | Weekly | **3/10** | No first-class tooling. Operator drops to `nats stream view`. Output is raw JSON. |
| 10 | Monitor throughput / metrics | Continuous | **2/10** | No dashboard. Operator builds ad-hoc scripts against the TELEMETRY stream. |
| 11 | Watch for consumer lag | Continuous | **3/10** | Per-consumer; no aggregation; no visualization; no alert. |
| 12 | Inspect cross-workflow signals | Continuous | 4/10 | Direct KV access. No "which run is waiting on signal X" view. |
| 13 | Deploy / upgrade | Rare | 7/10 | Single binary, swap-and-restart. Solid for single-host. No rolling-upgrade story for clustered. |
| 14 | Diff workflow versions | Rare | 4/10 | No tooling — operators do file diffs on JSON manually. |
| 15 | Backup / restore | Rare | 5/10 | NATS tools; not dagnats-specific. |

Weighted overall DX score: **5.2 / 10**. The CLI is solid for "I know exactly
what to do" workflows. It collapses for "I want to see what's happening" and "I
want to act on what I see." The continuous workflows score worst because the
CLI fundamentally can't render live state.

#### Highest-leverage improvements

Ranked descending by frequency × severity × feasibility:

1. **Live active-runs view** — workflow #5, daily, 4→9 fix via SSE + DOM swap.
2. **Failed-run debugger** — workflow #2, daily, 5→8 fix via DAG visualization + grouped events + step-input/output inline.
3. **Metrics dashboard** — workflow #10, continuous, 2→7 fix via TELEMETRY stream rendered as time-series.
4. **DLQ replay with preview** — workflow #8, weekly, 6→8 via bulk select + previewed-impact-before-replay.
5. **Capacity / consumer-lag visualization** — workflow #11, continuous, 3→7 via aggregated stream/consumer stats.
6. **Trigger fire history** — workflow #9, weekly, 3→8 via first-class view.

These six fix workflows that together represent **80% of operator-time spent**.
The CLI continues to be the right tool for scripting and CI; the UI is the
right tool for sustained operational attention.

### Prior art surveyed

- **Hatchet.run.** React + Next.js dashboard. Looks polished. Ships as a
  separate ~200 MB Docker image. Build pipeline includes Tailwind compile,
  webpack bundle, multi-stage Docker build. Operators run it as a peer service
  to their Postgres + Redis stack. dagnats wants the IA (Workflows / Runs /
  Workers / Events as distinct views) but explicitly rejects the weight.
- **Inngest.** Ships a Dev Server (local development experience) bundling a
  Next.js app within the binary, plus a Cloud dashboard (hosted service). Great
  example of "ship a React app inside a Go binary" — but the build complexity
  is non-trivial, and the binary picks up the React build size. dagnats wants
  the spirit ("dev experience includes a UI") but rejects the build step.
- **Dagger.io.** Cloud UI is React-based; CLI ships a TUI for local pipeline
  runs. The TUI's ASCII-art-DAG inspires the failed-run debugger view's DAG
  rendering — rendering as SVG instead of ASCII is the only real change.
- **Faktory / Sidekiq (closest spiritual match).** Server-rendered HTML,
  ERB-style templates, sprinkle JS, ships with the Ruby gem. About 200-300
  lines of HTML. Decade-plus production track record. dagnats wants the
  architecture; can improve on visual polish, live-update story, and
  DAG-specific visualizations.

**Synthesis:** Faktory's architecture + Hatchet's IA + Dagger's DAG rendering +
shadcn aesthetics (via Basecoat) + Datastar for reactivity and live updates.

## Decision

Ship an embedded control plane UI under `/console/` that follows
server-rendered HTML + SSE + minimal vendored JS, mirrors the existing
Scalar-mount pattern, and is delivered in 8 independently mergeable PRs.

### 1. Tech stack (locked)

| Layer | Choice | Size | Why |
|---|---|---|---|
| HTML rendering | Go `html/template` | n/a (compile-time) | Standard library; no dep; perfect for embedded |
| Static assets | `embed.FS` | n/a | Same pattern as Scalar (`internal/openapi/scalar.html`) |
| CSS / components | Basecoat (pre-built Tailwind output) | ~35 KB gzipped | shadcn aesthetics on plain HTML; vendored CSS file, no runtime Tailwind |
| Component JS | Basecoat's full vanilla JS | ~10-14 KB | Accessible primitives (dialogs, dropdowns, tabs, tooltips, toasts, popovers, command palette, accordions) shipped with the framework |
| Interactivity + live updates | Datastar | ~12 KB | Single library covers what HTMX + Alpine.js + HTMX-SSE-extension would together; SSE is a primary content type; first-class Go SDK |
| Charts | µPlot | ~30 KB gzipped | Single-file ES module; renders sparklines fast; no framework deps |

Total embedded asset size: **~90 KB gzipped** after bundling and minification.
Compare to Hatchet's React build (~500 KB minified) or Sidekiq's ~150 KB. The
design rejects the "no JavaScript at all" purity posture explicitly: where
Basecoat's component JS earns its weight, we use it.

### 2. Local-first asset policy — no CDN, no external fetches

Every asset the dagnats console serves is **vendored locally and shipped inside
the binary**. The browser makes zero outbound requests to any third-party CDN,
fontserver, analytics endpoint, or icon service. This is a load-bearing
operational requirement for several real deployment scenarios:

- **Air-gapped environments.** Some operators run dagnats with no internet
  egress at all.
- **Reproducibility.** A vendored asset is identical across builds, identical
  across deployments, identical forever.
- **Supply-chain surface.** A CDN compromise becomes malicious JavaScript
  running in the operator's browser. Local-only assets eliminate that vector.
- **First-load latency.** CDN round trips on first visit add hundreds of
  milliseconds.
- **Privacy.** Every CDN request is a third-party hit observable to middleboxes
  sitting between the operator and the CDN.

Concrete rules this policy enforces:

1. **No `<script src="https://...">` or `<link href="https://...">` anywhere
   in any template.** A test in PR 1 parses every rendered page and fails if
   any external URL appears in a `src`, `href`, or `@import`.
2. **Fonts are system fonts.** CSS uses `font-family: system-ui, -apple-system,
   BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif`. No Inter, no Google
   Fonts, no vendored .woff2 files.
3. **Icons are inline SVG** in the templates. ~30-40 unique icons total ~10 KB
   distributed across templates. No icon font, no Lucide import, no SVG
   sprite-sheet fetch.
4. **µPlot, Datastar, Basecoat — all vendored.** Upstream version pins live in
   `assets/README.md`. The release-time bundling reads from vendored sources;
   no `npm install` reaching out at deploy time either.

### 3. Build posture: deploy-time bundling, never-in-CI build tools

| Stage | Input | Tool | Output | Runs when |
|---|---|---|---|---|
| CSS compile | Basecoat sources + console HTML templates | Tailwind standalone | `basecoat.css.gz` (~35 KB gzipped) | Maintainer regenerates at release |
| JS bundle | Datastar + Basecoat component JS + `app.js` | esbuild standalone | `console.js.gz` (~20 KB gzipped) | Maintainer regenerates at release |
| Chart vendor | µPlot release | (none — copy file) | `uplot.min.js.gz` (~30 KB gzipped) | Bumped on µPlot version refresh |

Both build tools are single self-contained binaries with no dependencies. The
build invocations run at deploy time only. CI stays Node-free. `go build`
produces the entire artifact end-to-end. Refresh procedures are documented in
`internal/console/assets/README.md`.

### 4. Information architecture

Mount everything under `/console/`. The name "console" over "admin" is
deliberate: most operations are inspection (low privilege), not administration
(high privilege).

```
/console/                       Dashboard (health + active runs + recent activity)
/console/workflows              Workflow list
/console/workflows/<name>       Workflow detail
/console/runs                   Run list with filters
/console/runs/<id>              Run detail with DAG + timeline + actions
/console/triggers               Trigger list
/console/triggers/<id>          Trigger detail with fire history
/console/dlq                    DLQ with bulk select + preview
/console/ops/streams            NATS stream stats
/console/ops/workers            Connected workers + consumer lag
/console/ops/health             Health probes
/console/ops/signals            Cross-workflow signal inspector
/console/ops/audit              Action audit log (operator mutations)
/console/help                   Quick reference

# Asset paths
/console/assets/console.js
/console/assets/basecoat.css
/console/assets/uplot.min.js
/console/assets/app.css

# SSE streams
/console/sse/heartbeat                  Heartbeat tick (PR 1)
/console/sse/runs                       Active-runs delta stream
/console/sse/runs/<id>                  Per-run event stream
/console/sse/metrics                    System metrics stream
/console/sse/dlq                        DLQ append stream
/console/sse/audit                      Audit-log append stream

# Action endpoints (Datastar @post targets, return HTML fragments)
/console/api/runs/start
/console/api/runs/<id>/cancel
/console/api/runs/<id>/signal/<name>
/console/api/triggers/<id>/enable
/console/api/triggers/<id>/disable
/console/api/triggers/<id>/delete
/console/api/dlq/replay
```

### 5. Authentication — loopback-trust by default

The console doesn't implement authentication of its own at default settings.
Instead it relies on the same OS- and transport-level access control that
Postgres, Redis, MinIO console, and most modern self-hosted dev tools rely on:
**the listener is bound to loopback by default, so the only callers that can
reach the console are processes on the same machine.**

Three rules describe the entire policy:

1. **Default bind for the listener is `127.0.0.1`.** Out of the box, `dagnats
   serve` exposes `/console/*` only to processes on the host. A fresh install
   requires zero auth configuration and works immediately. This is a breaking
   change from the prior `:8080` (all-interfaces) default; operators with
   remote-access deployments must set `DAGNATS_HTTP_ADDR=0.0.0.0:8080` or
   similar.

2. **If the operator binds to a non-loopback interface and has not configured
   an auth mode, dagnats refuses to mount `/console/*`.** Startup logs a loud
   message naming both env vars; the route returns 503 with the same
   information in the body.

3. **Two auth modes are available when remote exposure is required:**
   - **Forward-auth** (`DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH=true`). dagnats
     trusts the `X-Forwarded-User` and `X-Forwarded-Email` headers set by an
     upstream auth proxy (Cloudflare Access, oauth2-proxy, Pomerium, Caddy,
     nginx, etc.).
   - **HTTP Basic Auth** (`DAGNATS_CONSOLE_PASSWORD=...`). Single shared
     password, browser-native auth dialog. Constant-time comparison.

Deployment matrix:

| Operator's situation | Recipe |
|---|---|
| Local development | Nothing. Run `dagnats serve`, open `http://localhost:8080/console/`. |
| Remote inspection from your laptop | `ssh -L 8080:localhost:8080 vps`, open the console in your local browser. |
| Team access with SSO | Run dagnats with `DAGNATS_HTTP_ADDR=0.0.0.0:8080` and `DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH=true`. |
| Homelab, no SSO, LAN exposure | Run dagnats with `DAGNATS_HTTP_ADDR=0.0.0.0:8080` and `DAGNATS_CONSOLE_PASSWORD=...`. |

No login form. No session storage. No per-user accounts inside dagnats. Total
auth-related code in PR 1 is approximately 50 LOC.

### 6. Action audit log

Every operator mutation emits a structured audit entry to a dedicated NATS
stream, `CONSOLE_ACTIONS`. The stream is the source of truth — the
`/console/ops/audit` view, per-entity affordances on detail pages, and the SSE
live stream all read from it.

Schema (one entry per mutation, published to `console.action.<action>.<target>`):

```go
type AuditEntry struct {
    ID             string          // ULID for sortability
    Timestamp      time.Time
    Actor          string          // "alice@example.com" / "console" / "loopback"
    ActorSource    string          // "forward-auth" | "basic-auth" | "loopback"
    RequestID      string          // correlation key for cross-referencing logs
    Action         string          // "run.cancel" | "trigger.disable" | "dlq.replay"
    Target         string          // run_id | trigger_id | etc.
    Before         json.RawMessage `json:",omitempty"`
    After          json.RawMessage `json:",omitempty"`
    PayloadOmitted bool            `json:",omitempty"`
    Outcome        string          // "succeeded" | "failed" | "denied"
    ErrorMsg       string          `json:",omitempty"`
}
```

Stream config:

- Name: `CONSOLE_ACTIONS`
- Subject filter: `console.action.>`
- Retention: 14 days by default; configurable via `DAGNATS_CONSOLE_AUDIT_RETENTION`
- Storage: file
- Replicas: matches the cluster's `workflow_history` replica count
- Deduplication: keyed on `ID` (ULID)

UI surfacing:

1. **`/console/ops/audit`** — canonical view; filterable table; rows expand to
   show before/after diff with red/green key-level highlighting.
2. **Per-entity affordances** — on `/console/runs/<id>`,
   `/console/triggers/<id>`, `/console/workflows/<name>`, a collapsible
   "Operator actions" panel surfaces audit entries whose `Target` matches.
3. **Dashboard activity feed** — interleaves the last few audit entries with
   `run.completed`/`run.failed` engine events.

Raw access via `nats stream view CONSOLE_ACTIONS` remains.

### 7. Read-only mode

Single env var `DAGNATS_CONSOLE_MODE`. Default unset (full access). Setting
`DAGNATS_CONSOLE_MODE=readonly` enables inspection-only deployments: all
`POST`/`DELETE` endpoints under `/console/api/*` return 403 with body
`{"error":"console_readonly","message":"this console is configured read-only;
mutations are disabled"}`. Action buttons in the UI hide entirely. Denied
mutation attempts still emit audit entries with `Outcome: "denied"`. The env
var is a string (not a bool) so future modes are additive.

### 8. Live-update transport

Server-Sent Events only for v1. dagnats's live-update workload is
overwhelmingly one-way (run-state transitions, audit appends, DLQ entries,
metric ticks). Browser-native `EventSource` reconnection handles transient
drops via `Last-Event-ID`. WebSockets remain a known-good extension path: if a
future view genuinely needs bidirectional flow, it becomes a separate ADR.

Implementation note: browsers cap concurrent SSE connections per origin at 6;
each page subscribes only to the streams its view needs.

The SSE pattern (uniform across views):

```
Browser ←── SSE ──── /console/sse/<view>          (a goroutine in console handler)
                          │
                          └─── NATS subscribe ──── stream/subject
```

Handler steps:

1. Sets standard SSE headers (`Content-Type: text/event-stream`, `Cache-Control: no-cache`).
2. Subscribes to the relevant NATS stream/subject.
3. On each NATS message, renders an HTML fragment for the affected DOM region.
4. Writes the rendered HTML as a Datastar `PatchElements` SSE event via the official Go SDK.
5. On client disconnect (detected via `r.Context().Done()`), unsubscribes from NATS and returns.

Datastar's Go SDK API:

```go
sse, _ := datastar.NewSSE(w, r)
for event := range natsEvents {
    runRow := renderRunRow(event.RunID, event.Status)
    sse.PatchElements(runRow)
}
```

### 9. DAG visualization

Hand-rolled server-rendered SVG with manual layered (topological-depth) layout.
No graph library — no d3, no Cytoscape, no Mermaid. Design language follows
GitHub Actions (top-down boxes-and-arrows, status icons, click-for-detail in a
side panel) and Dagger.io's trace view. Status changes update SVG attributes in
place; positions never move. A static-SVG export endpoint
(`/console/runs/<id>/dag.svg`) reuses the same rendering code.

A second view — Dagger-style timeline flamegraph — is reserved behind a
disabled "Timeline" toggle on the run detail page; the toggle scaffold ships in
v1 but the implementation lands in a future PR.

### 10. Metrics retention

Charts read directly from the `TELEMETRY` stream (7-day retention, 1 GB cap;
unchanged from current dagnats). Time-range options on the dashboard: 1h / 24h
/ 7d. Anything beyond 7d is out of scope — dagnats ships a small
Prometheus-format `/metrics` endpoint (~50 LOC) so operators with longer-range
requirements pipe to their existing TSDB. Server-side downsampling before
rendering: 1h = 60 points, 24h = 144 points, 7d = 168 points.

### 11. Multi-cluster awareness

Out of scope for v1. One dagnats binary, one NATS cluster, one console. The
browser's tab/bookmark/URL bar is the cluster picker. A future ADR can address
federated multi-cluster consoles for orgs running 10+ dagnats deployments.

### 12. Theming and aesthetic

E-ink editorial direction — warm cream backgrounds, warm graphite text,
low-saturation accents, no pure white or pure black. Inspired by Kindle
Paperwhite, iA Writer, Bump.sh, and the [Craft Design Group](https://craftdesign.group/)
site's restrained editorial feel. Light mode default; dark mode is an explicit
toggle. Single env var `DAGNATS_CONSOLE_THEME=eink` (default) selects the
palette; future themes are additive.

Anchor palette (starting values):

| Role | Light | Dark |
|---|---|---|
| Background | `#F8F5EF` (warm cream) | `#1A1814` (warm near-black) |
| Surface | `#FFFCF6` (whisper-warm white) | `#252220` (raised paper) |
| Border | `#D6CFC0` (paper edge) | `#3A3530` (faded charcoal) |
| Text primary | `#1F1B16` (warm graphite) | `#E8E2D6` (warm cream) |
| Text secondary | `#5A554C` (faded ink) | `#9B968B` (faded warm gray) |
| Accent | `#3F5363` (paper indigo) | `#7A8E9D` (lifted paper indigo) |

Status palette (muted but icon-paired for legibility):

| State | Light | Dark | Icon |
|---|---|---|---|
| Completed | `#6B7A56` (muted sage) | `#9CAA82` | filled circle `✓` |
| Running | `#A88950` (muted ochre) | `#CDB178` | pulsing dot `●` |
| Failed | `#9A6052` (muted terracotta) | `#C18876` | filled triangle `✗` |
| Skipped | `#807A6E` (warm gray) | `#B5B0A4` | outline circle `⊘` |
| Pending | `#5A6B7D` (muted slate) | `#8A9BAD` | empty circle `○` |

Implementation: the palette lives as CSS custom properties on `<body>`
(`--bg`, `--surface`, `--border`, `--text-primary`, `--text-secondary`,
`--accent`, `--status-completed`, etc.). The dark-mode toggle flips the entire
palette via a `data-theme="dark"` attribute on `<body>`.

### Implementation plan: 8 layered PRs

The plan splits along the dx-audit leverage ranking. Each PR closes a leverage
tier and ships a usable, testable slice.

#### PR 1 — Foundation (skeleton + auth + assets) — ~500 LOC

- `internal/console/` package created
- Mount at `/console/` in `server/server.go` next to OpenAPI/Scalar mount
- Default bind for the listener is `127.0.0.1`; opt-in via `DAGNATS_HTTP_ADDR`
- Loopback-trust auth gate per the auth section above
- `X-Forwarded-User`/`X-Forwarded-Email` threaded into request-scoped actor
- Empty dashboard page rendering layout, top nav, static "system overview" tile
- Vendored bundle: `console.js.gz`, `basecoat.css.gz`, `uplot.min.js.gz`, `app.css`
- `assets/README.md` documenting the Tailwind + esbuild deploy-time procedure
- `/console/sse/heartbeat` — trivial SSE stream proving the Datastar SSE plumbing works
- Tests: loopback mount, non-loopback refuse, forward-auth context, Basic Auth,
  asset serving, layout rendering, heartbeat SSE lifecycle, `TestNoExternalURLs`

#### PR 2 — Workflows + Runs (read-only views) — ~700 LOC

- `/console/workflows` and `/console/workflows/<name>`
- `/console/runs` with filter form, `/console/runs/<id>` with event history
- Pagination via Datastar `@get` + URL params

#### PR 3 — Live updates via Datastar SSE — ~350 LOC

- `/console/sse/runs` and `/console/sse/runs/<id>`
- Runs list and detail pages subscribe via `data-on-load`

#### PR 4 — Triggers + DLQ + audit emitter + read-only middleware — ~700 LOC

- `internal/audit/` package; `natsutil.SetupAll` provisions `CONSOLE_ACTIONS` stream
- Read-only middleware wrapping `/console/api/*`
- Triggers, DLQ with bulk-select and preview-before-replay

#### PR 5 — Run actions (with audit emission) — ~450 LOC

- Start-a-run modal, cancel button, send-signal form, bulk run start

#### PR 6 — Operations views + DAG viz + audit UI — ~850 LOC

- `/console/ops/{streams,workers,health,signals,audit}`
- `/console/sse/audit` and per-entity audit panels
- DAG SVG rendering on run detail

#### PR 7 — Metrics dashboard with µPlot + Prometheus exporter — ~480 LOC

- Dashboard sparklines with time-range toggle
- `/console/sse/metrics` and per-workflow metrics
- `/metrics` Prometheus exporter at the engine listener

#### PR 8 — Polish + docs — ~300 LOC

- Keyboard navigation, dark mode toggle, help page, accessibility audit

### Expected outcome

Re-applying the dx-audit to the proposed UI:

| # | Workflow | Current | After UI |
|---|---|---|---|
| 1 | Check system health | 8 | 9 (live tiles) |
| 2 | Debug a failed run | 5 | **9** (DAG + grouped events + inline I/O) |
| 5 | Monitor active runs | 4 | **9** (SSE-driven live table) |
| 8 | Replay DLQ after fix | 6 | **8** (bulk select + preview) |
| 9 | Review trigger fire history | 3 | **8** (first-class view) |
| 10 | Monitor throughput | 2 | **8** (charts) |
| 11 | Watch capacity issues | 3 | **8** (aggregated stream/consumer view) |

Weighted overall: **~7.8 / 10** — a 2.6-point improvement on a 10-point scale,
concentrated in the daily and continuous workflows where the leverage is
highest.

The CLI doesn't get worse — it stays as the scripting/CI surface — but the
operator now has a UI for the "I want to see what's happening" half of the job.

Total scope: ~3,850 LOC including tests, ~77 KB of embedded vendored assets, 8
PRs each independently mergeable, no new external runtime dependencies, one new
release-time toolchain dep (Tailwind + esbuild standalones).

## Alternatives Considered

### Why not React/Vue/Svelte SPA?

Rejected for the same reason HTMX-style stacks are popular: no build step on
the client, no virtual DOM, the server owns rendering. A React dashboard at
dagnats's scale is hundreds of kilobytes of JavaScript and a webpack pipeline.
Datastar at 12 KB total covers the same interaction patterns dagnats actually
needs. The Hatchet/Inngest dashboards demonstrate the path — and the
operational weight — explicitly.

### Why not HTMX + Alpine.js + htmx-ext-sse?

Datastar replaces three coordinated libraries with one. Smaller total bundle;
SSE is first-class rather than bolted on; one attribute namespace rather than
two (`hx-*` and `x-*`). The HTMX ecosystem is larger and longer-tenured, which
is the main argument for the HTMX stack. For dagnats specifically, where SSE
is the primary live-update mechanism (not an exception), Datastar's first-class
SSE handling matters more than HTMX's larger ecosystem.

Maturity caveat: Datastar is newer than HTMX. The mitigation is structural —
the server-side stays unchanged. Templates render Go types; SSE handlers
subscribe to NATS and write HTML. If Datastar ever needs to be swapped, the
change is contained in `data-*` attributes and the SSE write helper.

### Why not Phoenix LiveView?

Same architectural model (server-driven reactivity) on a different ecosystem
(Elixir/BEAM). Not adopting Elixir.

### Why not Pico CSS (original design)?

Pico is class-less and dependency-free, but visually it reads as "classic web
admin," not "modern operator dashboard." For a tool operators look at every
day, the visual gap matters. Basecoat lands the shadcn aesthetic operators
recognize from the rest of the modern dev-tools ecosystem.

### Why not DaisyUI / Flowbite / Preline?

Other Tailwind component layers. Basecoat is the smallest, most opinionated,
and most explicitly designed for the "no React" case. The component vocabulary
matches shadcn closely enough that operators familiar with that ecosystem find
dagnats's UI immediately legible.

### Why not hand-written CSS mimicking shadcn?

We could write the components from scratch in custom CSS, but the maintenance
load is real and the accessibility patterns Basecoat ships (focus management,
ARIA, keyboard navigation) are non-trivial to reproduce. The vendored-CSS
posture gives us Basecoat's surface area for the maintenance cost of one CSS
file we refresh on release.

### Why not WebSockets for live updates?

dagnats's live-update workload is overwhelmingly one-way. SSE composes with
existing proxies/auth/observability with zero special-case config. Browser
reconnection is built in. If a future view genuinely needs bidirectional flow
during a long stream, it becomes a separate ADR scoped to that view, not a
wholesale transport switch.

### Why not a graph library (d3, Cytoscape, Mermaid client-side) for the DAG?

For the workflow sizes dagnats handles (typically <50 steps), manual layered
layout is fine. The pathological case wraps within its column. Edge crossings
are not minimized in v1 (no Sugiyama algorithm); workflows with cross-connected
dependencies render with some edge crossings, which is a known acceptable
limitation. Adding d3 or Cytoscape would double the JS bundle size for a
solved-by-150-lines-of-Go problem.

### Why not a CDN for the UI assets?

See the "Local-first asset policy" section above. The cost of this policy is
the few extra KB of binary size for asset vendoring. The benefit is that a
dagnats binary built today, archived, and unpacked five years from now still
serves a working console — no rotted CDN dependencies, no expired domain
registrations, no upstream package yanks.

### Why not let CI build the JS/CSS bundles?

CI staying Node-free is a deliberate choice: contributors editing dagnats can
`go build` end-to-end with one toolchain. The Tailwind compile and esbuild
bundle exist as release maintenance, not per-commit infrastructure. We are
explicit that this is the cost we accept for shadcn-grade polish and
accessible interactive primitives — not "no build step ever" but "no build
step contributors have to deal with on a day-to-day basis."

### Why not require auth always (no loopback-trust)?

Auth always means a login form, session storage, CSRF tokens, password reset
flow, OAuth integration — a substantial code surface for the local-development
case where every other modern dev tool (Postgres, Redis, MinIO console, Vault
dev mode, Grafana dev mode) trusts the loopback boundary. The
loopback-trust-by-default approach lands at zero auth configuration for the
overwhelmingly common case while making the dangerous configuration (non-loopback
+ no auth) impossible by accident.

## Consequences

### Positive

- Operator DX jumps from **5.2 / 10 to ~7.8 / 10**, concentrated in the daily
  and continuous workflows where the leverage is highest.
- Single binary preserved — `go build` produces the entire artifact end-to-end.
- Air-gapped deployments work out of the box.
- CI stays Node-free.
- The CLI remains the right tool for scripting/CI; the UI is the right tool for
  sustained operational attention. Both surfaces coexist; neither gets worse.
- The audit log is a NATS stream operators can also `nats stream view` directly.
- The auth model rules out the dangerous configuration by construction.

### Negative

- **Breaking change:** the listener default flips from `:8080` (all interfaces)
  to `127.0.0.1:8080`. Operators with remote-access deployments must
  explicitly set `DAGNATS_HTTP_ADDR=0.0.0.0:8080`. This affects every HTTP
  surface dagnats exposes, not just `/console/*`: webhooks at `/hooks/`, the
  REST API, OpenAPI/Scalar at `/docs`, HTTP triggers at `/api/`.
- ~77 KB of vendored asset surface area to maintain across upstream version
  bumps. Mitigated by version-pinned refresh procedures in
  `internal/console/assets/README.md`.
- Datastar is newer than HTMX. Production lore and Stack Overflow answers are
  thinner. Mitigated structurally — the server side is the source of truth and
  the swap-out cost is bounded.
- Release-time Tailwind + esbuild invocations are one more thing for the
  maintainer to do at release time. Mitigated by single Makefile target and
  documented procedures.

### Explicitly deferred

- **Per-operator permissions.** All-or-nothing in v1.
- **Multi-tenancy / multi-workspace.** Single dagnats = single workspace.
- **Workflow editor / IDE-style writer.** Authors still write JSON. The UI is
  for operators, not authors.
- **Visual workflow builder.** Same reasoning; not in scope.
- **Workflow version diff tool.** Low-leverage rare workflow; ship if time
  permits, otherwise defer.
- **Alerting / notifications.** Operators set up alerts on the TELEMETRY stream
  via their existing observability stack.
- **Audit log analyzer.** The `CONSOLE_ACTIONS` stream is a write target;
  building tools to analyze it is a future story.
- **i18n.** Not in v1. Templates are written so adding a translation layer
  later is mechanical.

Companion documents:
- `adr-013-http-trigger-respond-step.md` — the synchronous HTTP API foundation
  this UI sits on top of
- `dx-tooling.md` — DX for workflow authors (test harness, dev mode) —
  orthogonal concern
- `serve-command-design.md` — how `dagnats serve` wires up the embedded process
