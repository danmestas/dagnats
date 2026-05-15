# Control Plane UI — Design Notes

**Status:** Design complete — ready for ADR-014 promotion
**Author:** Dan Mestas
**Methodology:** dx-audit applied to current operator surface, then layered design proposal, then 12-question grill against the proposal
**Companion to:** ADR-013 (HTTP trigger + respond step + Scalar `/docs`)

---

## Why this document

dagnats today exposes its operational surface through (a) a CLI, (b) a REST API, (c) raw `nats` CLI access to the underlying streams, and as of PR #231 (d) a Scalar-rendered OpenAPI explorer at `/docs`. The fourth surface — Scalar — answers a specific question for a specific user (an API consumer wanting to understand the HTTP-trigger contract). It doesn't answer the operator's questions: _is the system healthy, what ran today, what's failing, what's in flight, what's stuck in DLQ?_

A control plane UI would close that gap. This document scores the current operator experience using the dx-audit framework, ranks the highest-leverage improvements, and proposes a UI architecture that is **deliberately lightweight** — no build step, no npm, no bundler, no SPA framework — because dagnats's value proposition is the single-binary single-dependency posture, and the control plane UI must not break it.

Prior art exists at both ends of the weight spectrum. Hatchet and Inngest ship React-based dashboards that require build pipelines and ship hundreds of kilobytes (sometimes megabytes) of JavaScript. Faktory and Sidekiq, at the other extreme, ship a few hundred lines of server-rendered HTML with sprinkle-on JavaScript and have served high-traffic production systems for over a decade. dagnats lives ideologically at the Faktory end — and the technical state of HTMX + SSE + standalone Preact has moved enough since Sidekiq's era that we can land "Hatchet UI fidelity at Faktory's footprint" without compromising either.

---

## DX audit (current state)

Following the dx-audit skill methodology: enumerate workflows, score them, identify leverage. Audit is from the perspective of a production operator running dagnats (not from a workflow author writing tests — that's a different audit and is covered in `dx-tooling.md`).

### Operator workflows, enumerated by frequency

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

### Workflow scoring

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

### Overall score

Weighted by frequency (daily=4, weekly=2, continuous=3, rare=1):

- Daily (1-5): (8+5+9+7+4) / 5 = 6.6
- Weekly (6-9): (7+5+6+3) / 4 = 5.25
- Continuous (10-12): (2+3+4) / 3 = 3.0
- Rare (13-15): (7+4+5) / 3 = 5.33

Weighted: `(6.6×4 + 5.25×2 + 3.0×3 + 5.33×1) / 10 = 5.16`

**Overall DX score: 5.2 / 10**

The CLI is solid for "I know exactly what to do" workflows. It collapses for "I want to see what's happening" and "I want to act on what I see." The continuous workflows score worst because the CLI fundamentally can't render live state — `dagnats status` is a snapshot, not a stream.

### Highest-leverage improvements (frequency × severity × feasibility)

Ranked descending:

1. **Live active-runs view** — workflow #5, daily, 4→9 fix via SSE + DOM swap. Single highest-leverage item.
2. **Failed-run debugger** — workflow #2, daily, 5→8 fix via DAG visualization + grouped events + step-input/output inline. Second-highest.
3. **Metrics dashboard** — workflow #10, continuous, 2→7 fix via TELEMETRY stream rendered as time-series. Third.
4. **DLQ replay with preview** — workflow #8, weekly, 6→8 via bulk select + previewed-impact-before-replay.
5. **Capacity / consumer-lag visualization** — workflow #11, continuous, 3→7 via aggregated stream/consumer stats.
6. **Trigger fire history** — workflow #9, weekly, 3→8 via first-class view.

These six fix workflows that together represent **80% of operator-time spent**. The CLI continues to be the right tool for scripting and CI; the UI is the right tool for sustained operational attention.

---

## Prior art

Worth being honest about what dagnats _doesn't_ want from each comparable.

### Hatchet.run

React + Next.js dashboard. Looks polished. Ships as a separate service (a Docker image around 200 MB). Build pipeline includes Tailwind compile, webpack bundle, multi-stage Docker build. Hatchet's investment in the dashboard is roughly equal to their engine investment, and operators run it as a peer service to their Postgres + Redis stack.

**What dagnats wants:** the IA — Hatchet's separation of Workflows / Runs / Workers / Events into distinct views is correct.

**What dagnats explicitly rejects:** the weight. Tailwind + webpack + a separate Docker image is the architectural opposite of "embedded single binary."

### Inngest

Ships a "Dev Server" (the local development experience) and a "Cloud" dashboard (the production one). The Dev Server bundles a Next.js app within the binary; the Cloud is a hosted service. Inngest's local-binary UI is impressive — it's actually a great example of "ship a React app inside a Go binary" — but the build complexity is non-trivial, and the binary picks up the React build size.

**What dagnats wants:** the spirit of "the dev experience includes a UI by default."

**What dagnats rejects:** the build step. Inngest's binary is built with the React app pre-compiled; touching the UI requires a JavaScript toolchain. dagnats wants to be editable end-to-end with `go build`.

### Dagger.io

Their cloud UI is React-based. Their CLI ships a TUI for local pipeline runs. The TUI is interesting — terminal-rendered, lightweight, ASCII-art-DAG — but it's a different surface from a web UI. The cloud UI is a separate hosted service.

**What dagnats wants:** the TUI inspiration for the failed-run debugger view's DAG rendering. Dagger's ASCII trees are exactly the level of visualization we need; rendering them as SVG instead of ASCII is the only real change.

### Faktory / Sidekiq (the closest spiritual match)

Server-rendered HTML, ERB-style templates, sprinkle JS where needed, ships with the Ruby gem. About 200-300 lines of HTML. Has served high-traffic production systems for over a decade. The UI does jobs / retries / dead / scheduled / busy / dashboard / metrics — and that's it. It is opinionated, terse, beautiful in a 1990s-server-admin way, and **operators love it.**

**What dagnats wants:** the architecture. Server-rendered HTML, minimal JS, no build step, ships in the binary.

**What dagnats can improve:** the visual polish (we have CSS in 2026 that Sidekiq circa 2012 didn't), the live-update story (SSE wasn't widely available then), and the DAG-specific visualizations (Sidekiq is queue-shaped, not graph-shaped).

### Synthesis

The right move: **Faktory's architecture + Hatchet's IA + Dagger's DAG rendering + shadcn aesthetics (via Basecoat) + Datastar for reactivity and live updates.**

---

## Proposed architecture

### Tech stack

| Layer | Choice | Size | Why |
|---|---|---|---|
| HTML rendering | Go `html/template` | n/a (compile-time) | Standard library; no dep; perfect for embedded |
| Static assets | `embed.FS` | n/a | Same pattern as Scalar (`internal/openapi/scalar.html`) |
| CSS / components | Basecoat (pre-built Tailwind output) | ~35 KB gzipped | shadcn aesthetics on plain HTML; vendored CSS file, no runtime Tailwind |
| Component JS | Basecoat's full vanilla JS (dialogs, dropdowns, tabs, tooltips, toasts, popovers, command palette, accordions, etc.) | ~10-14 KB (bundled, treeshaken where possible) | Accessible primitives shipped with the framework; no scope-limiting on which components are allowed; bundle accepts modest dead code in exchange for not maintaining a "which components do we use" checklist |
| Interactivity + live updates | Datastar | ~12 KB | Single library covers what HTMX + Alpine.js + HTMX-SSE-extension would together; SSE is a primary content type, not a bolt-on; first-class Go SDK |
| Charts | µPlot | ~30 KB gzipped | Single-file ES module; renders sparklines fast; no framework deps |

**Total embedded asset size: ~90 KB gzipped after bundling and minification.** Compare to Hatchet's React build (~500 KB minified) or Sidekiq's ~150 KB. We're still firmly at the small end of the spectrum, and the bundle ships Basecoat's full set of accessible-by-default primitives — dialogs, dropdowns, tabs, tooltips, toasts, popovers, command palette, accordions — that we'd otherwise spend a thousand-plus lines of hand-rolled JS to reproduce poorly.

The design rejects the "no JavaScript at all" purity posture explicitly: where Basecoat's component JS earns its weight, we use it. The runtime budget is what we manage; the deploy-time tooling is what we accept.

### Local-first asset policy — no CDN, no external fetches

Every asset the dagnats console serves is **vendored locally and shipped inside the binary**. The browser makes zero outbound requests to any third-party CDN, fontserver, analytics endpoint, or icon service. This isn't a stylistic preference; it's a load-bearing operational requirement for several real deployment scenarios:

- **Air-gapped environments.** Some operators run dagnats in environments with no internet egress at all. A control plane that depends on `cdn.jsdelivr.net` or `esm.sh` is dead in those environments.
- **Reproducibility.** A CDN URL can change, get hijacked, or disappear. A vendored asset is identical across builds, identical across deployments, identical forever.
- **Supply-chain surface.** A CDN compromise becomes malicious JavaScript running in the operator's browser with whatever permissions the console has. Local-only assets eliminate that vector.
- **First-load latency.** CDN round trips on first visit add hundreds of milliseconds. Local assets are served from the same listener as the page itself.
- **Privacy.** Every CDN request is a third-party hit observable to whatever middleboxes sit between the operator and the CDN. Vendored assets stay inside the trust boundary.

Concrete rules this policy enforces:

1. **No `<script src="https://...">` or `<link href="https://...">` anywhere in any template.** A test in PR 1 parses every rendered page and fails if any external URL appears in a `src`, `href`, or `@import`.
2. **Fonts are system fonts.** The CSS uses `font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif`. No Inter, no Google Fonts, no vendored .woff2 files. The Basecoat aesthetic relies on spacing/color/component design, not a particular typeface.
3. **Icons are inline SVG** in the templates. ~30-40 unique icons across the whole UI, each ~200 bytes inline, total ~10 KB distributed across templates. No icon font, no Lucide import, no SVG sprite-sheet fetch.
4. **µPlot, Datastar, Basecoat — all vendored.** Upstream version pins live in `assets/sources/README.md`. The release-time bundling reads from vendored sources; no `npm install` reaching out to the public registry at deploy time either (an `npm ci` against a pinned `package-lock.json` is acceptable; an `npm install` against ranges is not).

The cost of this policy is the few extra KB of binary size for asset vendoring. The benefit is that a dagnats binary built today, archived, and unpacked five years from now still serves a working console — no rotted CDN dependencies, no expired domain registrations, no upstream package yanks.

### Build posture: deploy-time bundling, never-in-CI build tools

dagnats has two release-time toolchain dependencies, both of which produce small artifacts that ship vendored in the binary and are never invoked during per-commit CI:

| Stage | Input | Tool | Output | Runs when |
|---|---|---|---|---|
| CSS compile | Basecoat sources + console HTML templates | Tailwind CLI | `basecoat.css` (~35 KB gzipped) | Maintainer regenerates at release |
| JS bundle | Datastar + Basecoat component JS + `app.js` | esbuild (`--bundle --minify`) | `console.js` (~20 KB gzipped) | Maintainer regenerates at release |
| Chart vendor | µPlot release | (none — copy file) | `uplot.min.js` (~30 KB gzipped) | Bumped on µPlot version refresh |

**Why esbuild for the JS bundle:** single self-contained binary, runs in milliseconds, native ES module support, free tree-shaking. The same Node environment that ran Tailwind can run esbuild via `npx`; or both can be invoked through their standalone binaries with zero npm involvement. Either path is documented; both produce identical artifacts.

The resolution mirrors the existing Scalar pattern at every step:

- **All bundled artifacts are vendored** in `internal/console/assets/` — same shape as `internal/openapi/scalar/standalone.js.gz`.
- **The build invocations run at deploy time only** — when cutting a release, a maintainer regenerates the artifacts and commits the refreshed files. Day-to-day CI (PR validation, every-commit checks) never invokes Tailwind or esbuild.
- **The refresh procedures are documented** in each asset directory's `README.md`, pinning every upstream version and the exact commands. Identical pattern to `internal/openapi/scalar/README.md`.
- **Contributors editing the UI** can use any utility class or component already in the bundled output. If they add markup that needs a class or component outside the existing set, the test that exercises the new view will visibly fail — at which point they either pick a class/component that's already present or request a refresh from a maintainer.

The net effect: dagnats's CI stays Node-free. The `go build` command produces the entire artifact end-to-end as before. The Tailwind compile and the esbuild bundle exist as release maintenance, not per-commit infrastructure. We are explicit that this is the cost we accept for shadcn-grade polish and accessible interactive primitives — not "no build step ever" but "no build step contributors have to deal with on a day-to-day basis."

### Information architecture

Mount everything under `/console/`. The choice of "console" over "admin" is deliberate: most operations are inspection (low privilege), not administration (high privilege). The name communicates "this is the operational view."

```
/console/                       Dashboard (health + active runs + recent activity)
/console/workflows              Workflow list
/console/workflows/<name>       Workflow detail (definition, triggers, recent runs)
/console/runs                   Run list with filters (status, workflow, time)
/console/runs/<id>              Run detail with DAG + timeline + actions
/console/triggers               Trigger list
/console/triggers/<id>          Trigger detail with fire history
/console/dlq                    DLQ with bulk select + preview
/console/ops/streams            NATS stream stats
/console/ops/workers            Connected workers + consumer lag
/console/ops/health             Health probes
/console/ops/signals            Cross-workflow signal inspector
/console/ops/audit              Action audit log (operator mutations)
/console/help                   Quick reference + link to /docs

# Asset paths
/console/assets/htmx.min.js
/console/assets/pico.min.css
/console/assets/app.css
/console/assets/app.js
/console/assets/uplot.min.js
/console/assets/preact.standalone.js   # only loaded on /console/runs/<id>

# SSE streams
/console/sse/runs                       Active-runs delta stream
/console/sse/runs/<id>                  Per-run event stream
/console/sse/metrics                    System metrics stream
/console/sse/dlq                        DLQ append stream

# Action endpoints (Datastar @post targets, return HTML fragments)
/console/api/runs/start                 POST: start a new run
/console/api/runs/<id>/cancel           POST: cancel
/console/api/runs/<id>/signal/<name>    POST: send signal
/console/api/triggers/<id>/enable       POST
/console/api/triggers/<id>/disable      POST
/console/api/triggers/<id>/delete       DELETE
/console/api/dlq/replay                 POST: bulk replay
```

The split between SSE streams (`/console/sse/*`) and HTMX fragment endpoints (`/console/api/*`) keeps the surface clear: anything ending in `.sse` is a long-lived stream; anything under `/api/` is a request/response action.

Additional SSE stream not yet listed:

```
/console/sse/audit                      Audit-log append stream — new entries as PatchElements
```

### Action audit log

Every operator mutation through the console emits a structured audit entry to a dedicated NATS stream, `CONSOLE_ACTIONS`. The stream is the source of truth — the `/console/ops/audit` view, the per-entity affordances on workflow/run/trigger detail pages, and the SSE live stream all read from it. Per the NATS-native rule: no separate database, no separate audit log file, just a stream operators can also tail with the `nats` CLI.

**Schema** (one entry per mutation, published to `console.action.<action>.<target>`):

```go
type AuditEntry struct {
    ID             string          // ULID for sortability
    Timestamp      time.Time
    Actor          string          // "alice@example.com" (forward-auth), "console" (basic auth / loopback)
    ActorSource    string          // "forward-auth" | "basic-auth" | "loopback"
    RequestID      string          // correlation key for cross-referencing logs
    Action         string          // "run.cancel" | "trigger.disable" | "dlq.replay" | etc.
    Target         string          // run_id | trigger_id | etc.
    Before         json.RawMessage `json:",omitempty"`  // prior state, when feasible
    After          json.RawMessage `json:",omitempty"`  // new state, when feasible
    PayloadOmitted bool            `json:",omitempty"`  // true when before/after would be expensive
    Outcome        string          // "succeeded" | "failed"
    ErrorMsg       string          `json:",omitempty"`  // present only when Outcome == "failed"
}
```

**Stream config:**

- Name: `CONSOLE_ACTIONS`
- Subject filter: `console.action.>`
- Retention: 14 days by default; operator-configurable via `DAGNATS_CONSOLE_AUDIT_RETENTION` (e.g. `30d`, `90d`, `365d`)
- Storage: file
- Replicas: matches the cluster's `workflow_history` replica count
- Deduplication: keyed on `ID` (ULID) so a retried publish is collapsed

**Actor identity sourcing:**

- Forward-auth mode: `X-Forwarded-User` (and optionally `X-Forwarded-Email`) become the actor. The upstream auth proxy is the trust source.
- Basic Auth mode: actor is the literal string `"console"`. Future enhancement: per-action `DAGNATS_CONSOLE_OPERATOR_NAME` env var for operator-supplied attribution.
- Loopback mode: actor is `"loopback"`. Single-machine workflows where attribution is implicit.

**Payload omission policy:**

- Cheap mutations (cancel a run, disable a trigger): full before/after.
- Bulk operations (replay 50 DLQ entries, bulk-disable triggers): `PayloadOmitted: true` plus a summary count in the `After` field. The audit entry still captures *who*, *what kind*, *when*, *how many*, just not the per-item data — that's recoverable from the run/DLQ streams themselves.
- Failed mutations: the `Before` is captured (so operators can see what state the failed attempt would have changed); `After` is omitted; `ErrorMsg` carries the failure reason.

**UI surfacing:**

The audit log appears in three places in the console:

1. **`/console/ops/audit`** — the canonical view. Filterable table by actor, action type, target, outcome, and time range. Each row expandable to show the full `Before`/`After` diff (rendered as JSON with red/green highlighting for changed keys). Pagination by ULID for stable ordering across new appends. Live updates via `/console/sse/audit` — new entries patch into the table head.

2. **Per-entity affordances** — on `/console/runs/<id>`, a collapsible "Operator actions on this run" panel surfaces audit entries whose `Target == run_id`. Same shape on `/console/triggers/<id>` and `/console/workflows/<name>`. Empty state ("no operator actions recorded") when no entries match. The panel reuses the same row template from the main audit page.

3. **Dashboard activity feed** — the dashboard's "Recent activity" tile interleaves the last few audit entries with the last few `run.completed`/`run.failed` engine events, giving the operator a unified "what happened recently" view. The audit entries are visually distinct (slightly different chrome) so the operator-vs-engine attribution stays clear.

The "raw" path remains: `nats stream view CONSOLE_ACTIONS` works for operators who want the unfiltered firehose. The UI is a convenience over the underlying stream, not a wall around it.

### Authentication — loopback-trust by default

The console doesn't implement authentication of its own at default settings. Instead it relies on the same OS- and transport-level access control that Postgres, Redis, MinIO console, and most modern self-hosted dev tools rely on: **the listener is bound to loopback by default, so the only callers that can reach the console are processes on the same machine.** The OS already enforces that boundary; dagnats doesn't need to reimplement it.

Three rules describe the entire policy:

1. **Default bind for the console is `127.0.0.1`.** Out of the box, `dagnats serve` exposes `/console/*` only to processes on the host. A fresh install requires zero auth configuration and works immediately.

2. **If the operator binds to a non-loopback interface and has not configured an auth mode, dagnats refuses to mount `/console/*`.** Startup logs a loud message: `console disabled: listener is bound to 0.0.0.0 but no auth mode configured. Set DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH=true if you're running behind an auth proxy, or DAGNATS_CONSOLE_PASSWORD=... for HTTP Basic Auth.` The dangerous configuration — console exposed to a network without auth — is impossible to achieve by accident.

3. **Two auth modes are available when remote exposure is required:**
   - **Forward-auth** (`DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH=true`). dagnats trusts the `X-Forwarded-User` and `X-Forwarded-Email` headers set by an upstream auth proxy (Cloudflare Access, oauth2-proxy, Pomerium, Caddy with a forward_auth directive, nginx with auth_request, etc.). The operator is responsible for ensuring the dagnats listener is unreachable except through that proxy. This is the production-correct path for any operator with existing SSO.
   - **HTTP Basic Auth** (`DAGNATS_CONSOLE_PASSWORD=...`). Single shared password, browser-native auth dialog. The "no SSO available, simple shared-password gate" fallback for homelabs and small teams.

The deployment matrix in practice:

| Operator's situation | Recipe |
|---|---|
| Local development | Nothing. Run `dagnats serve`, open `http://localhost:8080/console/`. |
| Remote inspection from your laptop | `ssh -L 8080:localhost:8080 vps`, open the console in your local browser. No auth needed because the loopback binding plus the SSH tunnel together provide access control. |
| Team access with SSO | Run dagnats with `DAGNATS_LISTEN_ADDR=0.0.0.0` and `DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH=true`. Put Cloudflare Access, oauth2-proxy, or your existing auth proxy in front. dagnats reads `X-Forwarded-User` from each request and uses it for the action audit log. |
| Homelab, no SSO, want to expose on LAN | Run dagnats with `DAGNATS_LISTEN_ADDR=0.0.0.0` and `DAGNATS_CONSOLE_PASSWORD=...`. |

What this approach explicitly does **not** do:

- No login form. Both production paths (forward-auth, SSH-tunnel-to-localhost) handle authentication outside dagnats; Basic Auth uses the browser-native dialog.
- No session storage. No cookies, no CSRF tokens, no "remember me," no "forgot password."
- No per-user accounts inside dagnats. Identity, if it exists, comes from the upstream auth layer via forward-auth headers.

The total auth-related code in PR 1 is approximately 50 LOC: the loopback-detection at startup, the refuse-to-mount branch, the forward-auth header reader, and the Basic Auth middleware. Future expansions (cookie sessions, OAuth/OIDC integration, per-action permissions, role-based access) become an independent ADR scoped against real user demand — the architecture this v1 design proposes leaves all of those additive rather than rip-and-replace.

### Live updates: how the SSE pipeline works

The SSE pattern is uniform across the live-update views:

```
Browser ←── SSE ──── /console/sse/<view>          (a goroutine in console handler)
                          │
                          └─── NATS subscribe ──── stream/subject
```

The handler:

1. Sets the standard SSE headers (`Content-Type: text/event-stream`, `Cache-Control: no-cache`, `Connection: keep-alive`).
2. Subscribes to the relevant NATS stream/subject (`WORKFLOW_HISTORY` for runs, `TELEMETRY` for metrics, `DEAD_LETTERS` for DLQ).
3. On each NATS message, renders an HTML fragment for the affected DOM region using the same Go template the initial page render used.
4. Writes the rendered HTML as a Datastar `PatchElements` SSE event via the official Go SDK.
5. On client disconnect (detected via `r.Context().Done()`), unsubscribes from NATS and returns.

The Go SDK's API:

```go
sse, _ := datastar.NewSSE(w, r)
for event := range natsEvents {
    runRow := renderRunRow(event.RunID, event.Status)
    sse.PatchElements(runRow)  // single round trip: SSE event carries the HTML
}
```

On the browser, the page subscribes via a `data-on-load="@get('/console/sse/runs', {openWhenHidden: true})"` attribute. Datastar's runtime opens the EventSource, receives `PatchElements` events, and updates the DOM by ID — no client-side JavaScript handlers required. Same pattern for the per-run page (`/console/sse/runs/<id>`) and the metrics stream.

The `EventSource` API has built-in reconnection — if the connection drops, the browser automatically reconnects with the `Last-Event-ID` header, and the handler resumes the NATS subscription from that sequence number.

This is a meaningful simplification over the HTMX+SSE-extension pattern that an earlier draft of this design considered: there, the SSE event would carry only a notification, and a follow-up HTMX fetch would retrieve the actual HTML fragment — two round trips per update. With Datastar, the SSE event carries the rendered HTML directly. One round trip, less browser-side coordination, no fragment-endpoint plumbing needed on the live-update path.

### Why Datastar

Datastar is a single ~12 KB library that covers the three layers an HTMX-based stack would otherwise need:

- **AJAX via attributes** (what HTMX is famous for): `data-on:click="@post('/console/api/runs/abc/cancel')"` replaces `hx-post="..."`.
- **Reactive client signals** (what Alpine.js handles in an HTMX-based stack): `data-show="$detailsOpen"` toggles a panel without a server round trip; signals exist purely in the browser for transient UI state.
- **SSE-first content type** (what `htmx-ext-sse` adds on top of HTMX): SSE is a primary content type, not an extension. The Go SDK ships `PatchElements`, `MergeFragments`, etc. as first-class operations.

The mental model is uniform: every interactive behavior is a `data-*` attribute, and the attribute namespace is HTML's reserved app-specific space. Contributors learn one library, not three.

Datastar over the alternatives:

- **Versus React/Vue/Svelte SPAs.** Same argument as HTMX: no build step on the client, no virtual DOM, the server owns rendering. Datastar is at the lighter end of "no SPA" frameworks at 12 KB total.
- **Versus HTMX + Alpine.js + htmx-ext-sse.** Single library replacing three coordinated libraries; smaller total bundle; SSE is first-class rather than bolted on; one attribute namespace rather than two (`hx-*` and `x-*`). The HTMX ecosystem is larger and longer-tenured, which is the main argument for the HTMX stack. For dagnats specifically, where SSE is the primary live-update mechanism (not an exception), Datastar's first-class SSE handling matters more than HTMX's larger ecosystem.
- **Versus Phoenix LiveView.** Same architectural model (server-driven reactivity) on a different ecosystem (Elixir/BEAM). Not adopting Elixir.

Maturity caveat: Datastar is newer than HTMX. Production deployments and Stack Overflow lore are thinner. The mitigation is structural — the server-side stays unchanged. Templates render Go types; SSE handlers subscribe to NATS and write HTML. If Datastar ever needs to be swapped for HTMX in the future, the change is contained in `data-*` attributes and the SSE write helper. The server logic, templates, and NATS plumbing don't move.

### Why Basecoat

Basecoat brings shadcn/ui's component patterns and aesthetic to plain HTML without React. The output is utility-class HTML (`class="btn btn-primary"`, `class="card"`, `class="dialog"`) compiled from Tailwind. The Basecoat layer adds the component shortcuts on top.

Why Basecoat over alternatives:

- **Versus Pico CSS** (the original draft of this design): Pico is class-less and dependency-free, but visually it reads as "classic web admin," not "modern operator dashboard." For a tool operators look at every day, the visual gap matters. Basecoat lands the shadcn aesthetic operators will recognize from the rest of the modern dev-tools ecosystem.
- **Versus DaisyUI, Flowbite, Preline.** Other Tailwind component layers. Basecoat is the smallest, most opinionated, and most explicitly designed for the "no React" case. The component vocabulary matches shadcn closely enough that operators familiar with that ecosystem find dagnats's UI immediately legible.
- **Versus hand-written CSS** that mimics shadcn. We could write the components from scratch in custom CSS, but the maintenance load is real and the accessibility patterns Basecoat ships (focus management, ARIA, keyboard navigation) are non-trivial to reproduce. The vendored-CSS posture gives us Basecoat's surface area for the maintenance cost of one CSS file we refresh on release.

The Tailwind build step lives at deploy/release time, not in per-commit CI — see the "Build posture" section above. Day-to-day contributor experience is editing HTML templates with the existing class vocabulary; refreshing the CSS bundle is a maintainer task documented in the Basecoat asset's `README.md`.

### The DAG visualization

The single most visually complex view is the run detail page's DAG visualization. Two complementary views answer different operator questions; v1 ships the first one, the second is scaffolded behind a toggle for a later PR.

**Design references:** GitHub Actions' workflow visualization (top-down boxes-and-arrows, status icons, click-for-detail in side panel) and Dagger.io's trace/DAG view (timeline-style bars, indented hierarchy, hand-rolled SVG rendering). Both share the same design language: static layout that never reflows on state changes, status as the primary visual signal, hand-rolled SVG with no graph library. No d3, no Cytoscape, no Mermaid client-side renderer.

#### View 1 — DAG view (ships v1)

Answers: *what's the shape of this workflow, and where did it succeed/fail?* This is the default view operators land on when they open a run detail page.

1. **Server computes the layered layout.** Steps are grouped by topological depth (root steps at layer 0, their immediate dependents at layer 1, etc.). Within a layer, alphabetical or input-order. This handles the 95% case of dagnats workflows (which are predominantly tree-shaped or linear, not heavily forked).

2. **Server renders SVG directly into the page template.** Each step is a `<g>` element containing a `<rect>` (rounded), a step-id text node (monospace, top-left), a task-type subtitle, a status icon + label, and a duration label (bottom-right). Edges are simple straight or single-curve `<path>` elements between layer columns with small arrowheads. CSS classes encode status (`step--completed`, `step--failed`, `step--running`, `step--skipped`, `step--pending`); status colors come from the e-ink palette locked in the theming decision. Skipped steps render with a dashed border and muted body.

3. **Hover and click states are Datastar signals.** Hover sets a `$hoveredStep` signal that faintly highlights the step and its incident edges. Click triggers `@get('/console/api/fragments/step?run=X&step=Y')` and patches a side panel containing the full step detail (input, output, errors, retry history). No custom JavaScript handlers.

4. **Live updates change attributes, never positions.** As step events arrive on `/console/sse/runs/<id>`, the SSE handler swaps the CSS class on the appropriate `<g>` element. No re-layout; SVG attributes update in place. The DAG that an operator was looking at one second ago is the DAG they're looking at now — only the colors change.

5. **No layout library.** For the workflow sizes dagnats handles (typically <50 steps), the manual layered layout is fine. The pathological case (large fan-out from a `map` step) wraps within its column. Edge crossings are not minimized in v1 (no Sugiyama algorithm); workflows with cross-connected dependencies render with some edge crossings, which is a known acceptable limitation.

6. **The DAG also exports as a static SVG file.** A single `GET /console/runs/<id>/dag.svg` returns the same SVG rendered as a downloadable image — no headless browser needed, no PNG conversion. Operators can share "here's why this run failed" as a self-contained file.

#### View 2 — Timeline view (deferred to a future PR)

Answers: *when did each step run and for how long?* Useful once the operator is oriented to the workflow's shape and wants to investigate timing — a long-running step that should have been fast, or a cluster of steps that all retried at once.

Visual model: Dagger.io-style trace flamegraph. Horizontal time axis, one row per step, bars colored by status and sized by duration. Map-step children nest under their parent bar; sub-workflow steps link to the child run. Live updates extend in-progress bars to "now" each tick.

The run detail page ships in v1 with a two-state toggle ("DAG" / "Timeline") at the top of the visualization area. The Timeline option is disabled with a tooltip "coming in a future release"; the toggle scaffold reserves the affordance and the URL space (`/console/runs/<id>?view=timeline`) without committing the implementation. When the timeline lands in a future PR, the toggle becomes live.

Deferring timeline to a later PR is deliberate. Timeline rendering needs careful attention to zoom, time scrubbing, dense long-run rendering, and the "step is still in progress" continuous-update case — none of which are technically hard but all of which deserve their own design pass. DAG view is what operators reach for first; timeline is the natural follow-up.

### Asset embedding pattern

Mirror the Scalar approach. New package `internal/console/`:

```
internal/console/
├── handler.go              // mux mounting, auth gate, route table
├── pages.go                // page handlers (dashboard, runs, etc.)
├── fragments.go            // fragment endpoints (Datastar @get targets)
├── sse.go                  // SSE handlers per stream (Datastar Go SDK)
├── actions.go              // mutation endpoints (cancel, replay, etc.)
├── templates/              // Go html/template files
│   ├── layout.html
│   ├── dashboard.html
│   ├── runs-list.html
│   ├── run-detail.html
│   └── fragments/
│       ├── run-row.html
│       ├── step.html
│       └── dlq-entry.html
├── assets/
│   ├── README.md           // refresh procedure for all bundled artifacts
│   ├── console.js.gz       // bundled+minified (Datastar + Basecoat JS + app.js)
│   ├── basecoat.css.gz     // pre-built Tailwind output (regenerated at release)
│   ├── uplot.min.js.gz     // vendored upstream µPlot
│   ├── app.css             // custom dagnats styles on top of basecoat
│   └── sources/            // unbundled inputs the deploy-time tools consume
│       ├── app.js          // custom JS — Datastar signal wiring, chart init, etc.
│       ├── tailwind.config.js
│       └── README.md       // upstream versions pinned for Datastar, Basecoat, µPlot
└── embed.go                // //go:embed assets/*.gz assets/*.css templates
```

All bundled artifacts ship gzipped on disk and served with `Content-Encoding: gzip`. The split between `assets/` (compiled outputs that the binary embeds) and `assets/sources/` (inputs the deploy-time tools consume) makes the dependency direction explicit: the binary's behavior is determined by the contents of `assets/*`, never `assets/sources/*`. A contributor inspecting how the UI works reads the compiled outputs (or the templates); a maintainer regenerating bundles reads `sources/` and the `README.md`.

The release procedure is two commands (or a single Makefile target invoking both):

```bash
# CSS — compile Tailwind against the current template inventory
npx tailwindcss -i sources/basecoat-entry.css -o assets/basecoat.css \
                --content "templates/**/*.html"
gzip -9 -f assets/basecoat.css

# JS — bundle Datastar + Basecoat component JS + app.js
npx esbuild sources/app.js --bundle --minify \
            --target=es2020 --format=esm \
            --outfile=assets/console.js
gzip -9 -f assets/console.js
```

`uplot.min.js.gz` is bumped by hand on µPlot releases — there's nothing to compile, just a checksum to verify and a file to drop in.

---

## Implementation plan: layered PRs

The plan splits along the dx-audit leverage ranking. Each PR closes a leverage tier and ships a usable, testable slice.

### PR 1 — Foundation (skeleton + auth + assets) — ~500 LOC

- `internal/console/` package created
- Mount at `/console/` in `server/server.go` (next to existing OpenAPI/Scalar mount). Default bind for the listener is `127.0.0.1`; changing this is an opt-in via `DAGNATS_LISTEN_ADDR`.
- Loopback-trust auth gate: if the bound address is loopback, `/console/*` mounts unconditionally. If the bound address is non-loopback, mounting requires either `DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH=true` or `DAGNATS_CONSOLE_PASSWORD=...`; without either, the console refuses to mount and the startup log emits a loud message naming both env vars.
- When forward-auth is enabled, `X-Forwarded-User` and `X-Forwarded-Email` are read from each request and threaded into the action audit log entries.
- Empty dashboard page rendering layout, nav, and a static "system overview" tile.
- Vendored bundle: `console.js.gz` (Datastar + full Basecoat JS + app.js), `basecoat.css.gz`, `uplot.min.js.gz`, `app.css`.
- `internal/console/assets/sources/README.md` documenting the Tailwind + esbuild deploy-time procedure with pinned upstream versions.
- `/console/sse/heartbeat` — a trivial SSE stream that emits a `PatchElements` tick every 5s — to prove the Datastar SSE plumbing works.
- Tests:
  - Loopback binding mounts without env vars; non-loopback binding without auth env vars refuses to mount and logs the expected message.
  - Forward-auth headers are read and surfaced as operator identity on a request-scoped context.
  - Basic Auth path: 401 without credentials when password is set; 200 with valid credentials.
  - Asset serving, layout rendering, heartbeat SSE lifecycle.
  - **`TestNoExternalURLs`** — render every public page, parse the HTML, fail if any `src`/`href`/`@import` points outside the dagnats process. Enforces the local-first asset policy at the test layer so the rule survives contributor edits.

### PR 2 — Workflows + Runs (read-only views) — ~700 LOC

- `/console/workflows` — list, with definition counts and per-workflow last-run timestamp
- `/console/workflows/<name>` — definition view (JSON pretty-printed), triggers attached, recent runs
- `/console/runs` — table with filter form (status, workflow, time range)
- `/console/runs/<id>` — detail with event history (timeline format), step status grid, run input/output
- Pagination via `data-on:click="@get(...)"` + URL params
- Tests: pages render, filter logic, fragment endpoints

### PR 3 — Live updates via Datastar SSE — ~350 LOC

- `/console/sse/runs` — runs delta stream (driven by `WORKFLOW_HISTORY` subscription with subject filter); emits `PatchElements` events using the Datastar Go SDK
- `/console/sse/runs/<id>` — per-run event stream
- Runs list page declares `data-on-load="@get('/console/sse/runs', {openWhenHidden: true})"`; new rows arrive as Datastar element patches
- Run detail page does the same for the per-run stream; step status nodes patch in place
- Tests: SSE lifecycle, NATS subscription cleanup on disconnect, reconnect behavior with Last-Event-ID

### PR 4 — Triggers + DLQ + audit emitter + read-only middleware — ~700 LOC

- `internal/audit/` package: `AuditEntry` type, `Emit(ctx, action, target, before, after, outcome, err) error`. Subscribes to no streams; publishes only. Centralized so every mutation endpoint goes through the same surface.
- `natsutil.SetupAll` provisions the new `CONSOLE_ACTIONS` stream (subject filter `console.action.>`, configurable retention via `DAGNATS_CONSOLE_AUDIT_RETENTION`, defaults to 14d).
- Read-only middleware: wraps `/console/api/*` mutation endpoints. When `DAGNATS_CONSOLE_MODE=readonly`, returns 403 with the `console_readonly` error body and emits an audit entry with `Outcome: "denied"`. Otherwise passes through. Single function on the action-mux side, single check inside templates to hide action buttons.
- `/console/triggers` — list across all four kinds, with enable/disable toggles. Each toggle calls `audit.Emit` with the prior `TriggerDef` as `Before` and the new state as `After`. Toggles hidden in read-only mode.
- `/console/triggers/<id>` — detail with fire history (last N fires from `TRIGGER_HISTORY` stream).
- `/console/dlq` — list with bulk select checkboxes and replay action. Bulk action hidden in read-only mode.
- Preview-before-replay flow: `@post('/console/api/dlq/preview')` returns "replaying these N entries" confirmation patch. The actual replay emits a single audit entry with `PayloadOmitted: true` and a count summary.
- Tests:
  - Trigger toggle flow, DLQ bulk-replay interaction.
  - **Audit entries published with correct actor identity** sourced from forward-auth header / Basic Auth literal / loopback literal.
  - **Read-only middleware**: with `DAGNATS_CONSOLE_MODE=readonly`, POST/DELETE return 403, audit entries record `Outcome: "denied"`, action buttons absent from rendered HTML.

### PR 5 — Run actions (with audit emission) — ~450 LOC

- "Start a run" modal on workflows list (input JSON editor, submit posts to existing `/runs` endpoint). Emits `run.start` audit entry with the input payload.
- Cancel button on run detail. Emits `run.cancel` audit entry with the prior run status.
- Send-signal form on waiting runs (signals derived from workflow definition's wait-for-event steps). Emits `run.signal` audit entry with the signal name and payload.
- Bulk run start (CSV/JSONL paste-in textarea). Emits a single `run.bulk-start` audit entry with `PayloadOmitted: true` and a count summary.
- Tests: action endpoints, validation of input JSON shape (matches workflow `input_schema`), **audit entries emitted on every mutation including failure cases**.

### PR 6 — Operations views + DAG viz + audit UI — ~850 LOC

- `/console/ops/streams` — table of streams with message counts, consumer lag aggregated per stream.
- `/console/ops/workers` — connected workers with their consumer subject + pending-ack count.
- `/console/ops/health` — health probes (NATS connectivity, JetStream available, KV bucket health).
- `/console/ops/signals` — signals KV inspector.
- `/console/ops/audit` — canonical audit log view. Filterable table (actor, action type, target, outcome, time range). Rows expand to show `Before`/`After` diff with red/green key-level highlighting.
- `/console/sse/audit` — append stream for live audit entries; new rows patch into the table head on the audit page.
- Per-entity audit affordances: "Operator actions" panels on `/console/runs/<id>`, `/console/triggers/<id>`, `/console/workflows/<name>` — same row template, filtered by `Target`.
- Dashboard "Recent activity" tile interleaves audit entries with engine events.
- DAG SVG rendering on run detail (server-computed layered layout); hover/click driven by Datastar signals.
- Tests: NATS stat queries, SVG output, signal-driven hover behavior, **audit-log view filtering + per-entity panel rendering + SSE append lifecycle**.

### PR 7 — Metrics dashboard with µPlot + Prometheus exporter — ~480 LOC

- Dashboard top-tile: 60-second sparklines for run throughput, task throughput, DLQ rate.
- Time-range toggle on the dashboard: **1h / 24h / 7d**. Each range maps to a server-side downsampling rate (60 / 144 / 168 points respectively); the chart endpoint reads from `TELEMETRY` and emits the prepared time-series.
- `/console/sse/metrics` — live stream that pushes new time-series points as `TELEMETRY` updates arrive. The SSE event carries a `PatchElements` that updates the chart's data; µPlot redraws via its incremental data-update API (no full re-render).
- Per-workflow metrics on workflow detail page (same shape, scoped to that workflow's runs).
- **`/metrics` Prometheus-format exporter** at the engine listener (not under `/console/`). Standard text format; exposes run.throughput, task.throughput, dlq.rate, plus per-stream and per-consumer JetStream stats. Operators with TSDBs (Victoria Metrics, Prometheus + remote write, ClickHouse) scrape this for longer-range storage independently of the console.
- Tests: time-series downsampling correctness, chart rendering (DOM-level assertions on µPlot output), Prometheus exporter format compliance (parse the output, assert metrics are present, units are correct).

### PR 8 — Polish + docs — ~300 LOC

- Keyboard navigation (`/` to focus search, `g` then `r` for runs, etc.) via Datastar `data-on:keydown`
- Dark mode toggle (Basecoat respects Tailwind's dark mode out of the box)
- Help page at `/console/help`
- Documentation page at `docs/site/content/docs/console/`
- Final accessibility audit (semantic HTML + ARIA where needed; Basecoat ships accessible primitives, but custom views need their own audit)

### Total scope estimate

- ~3,850 LOC including tests (modestly less than the HTMX plan because Datastar collapses the fragment-fetch-on-SSE pattern into a single round trip)
- ~77 KB of embedded vendored assets
- 8 PRs, each independently mergeable
- No new external runtime dependencies beyond what's vendored. One new release-time toolchain dep (Node.js + Tailwind), exercised only when refreshing the Basecoat CSS
- Estimated 2-3 weeks of focused contributor time

---

## Comparing the proposed outcome to the current state

Re-applying the dx-audit to the proposed UI (estimated scores):

| # | Workflow | Current | After UI |
|---|---|---|---|
| 1 | Check system health | 8 | 9 (live tiles) |
| 2 | Debug a failed run | 5 | **9** (DAG + grouped events + inline I/O) |
| 3 | Inspect run output | 9 | 9 |
| 4 | Trigger a manual run | 7 | 8 (JSON editor in modal) |
| 5 | Monitor active runs | 4 | **9** (SSE-driven live table) |
| 6 | Register a new workflow | 7 | 8 (definition view + warnings inline) |
| 7 | Add / modify a trigger | 5 | 7 (one place to manage all four kinds) |
| 8 | Replay DLQ after fix | 6 | **8** (bulk select + preview) |
| 9 | Review trigger fire history | 3 | **8** (first-class view) |
| 10 | Monitor throughput | 2 | **8** (charts) |
| 11 | Watch capacity issues | 3 | **8** (aggregated stream/consumer view) |
| 12 | Inspect cross-workflow signals | 4 | 7 (signals inspector) |
| 13 | Deploy / upgrade | 7 | 7 (unchanged) |
| 14 | Diff workflow versions | 4 | 5 (definition-view side-by-side is a stretch but plausible) |
| 15 | Backup / restore | 5 | 5 (unchanged) |

Weighted overall: **~7.8 / 10** — a 2.6-point improvement on a 10-point scale, concentrated in the daily and continuous workflows where the leverage is highest.

The CLI doesn't get worse — it stays as the scripting/CI surface — but the operator now has a UI for the "I want to see what's happening" half of the job.

---

## Resolved design questions

- **Mount path.** `/console/`. Communicates "operational view" rather than "privileged admin."

- **Auth model.** Loopback-trust by default — the listener binds to `127.0.0.1` and no auth is required because the OS provides the boundary. When the listener is bound to a non-loopback interface, `/console/*` refuses to mount unless either forward-auth (`DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH=true`) or HTTP Basic Auth (`DAGNATS_CONSOLE_PASSWORD=...`) is configured. The dangerous configuration is impossible by accident. See the "Authentication" section above for the full deployment matrix.

- **Console-disabled behavior.** When the console refuses to mount (non-loopback binding without auth), the startup log emits a loud message naming both env vars; the route returns 503 with the same message in the body for operators who hit the URL before reading the log.

- **Action audit log.** New `CONSOLE_ACTIONS` JetStream stream with subject filter `console.action.>` and 14-day default retention (configurable via `DAGNATS_CONSOLE_AUDIT_RETENTION`). Every mutation endpoint emits an `AuditEntry` capturing actor (from forward-auth header / Basic Auth / loopback), action kind, target, before/after snapshots where feasible, and outcome. UI surfaces audit data in three places: a canonical `/console/ops/audit` view with filters and live updates, per-entity panels on workflow/run/trigger detail pages, and an interleaved "Recent activity" tile on the dashboard. Raw access via `nats stream view CONSOLE_ACTIONS` remains the same. See the "Action audit log" section above for the schema and publication pattern.

- **Read-only mode.** Single env var `DAGNATS_CONSOLE_MODE`. Default is unset (full access — operators can cancel runs, replay DLQ entries, toggle triggers, etc.). Setting `DAGNATS_CONSOLE_MODE=readonly` enables an inspection-only deployment: all `POST`/`DELETE` endpoints under `/console/api/*` return 403 with body `{"error":"console_readonly","message":"this console is configured read-only; mutations are disabled"}`. Action buttons in the UI (cancel, replay, enable/disable, signal) hide entirely when read-only is in effect — operators see no UI affordance for what they cannot do. Denied mutation attempts still emit audit entries with `Outcome: "denied"` so misconfigured access attempts are forensically visible. Single-line middleware at the `/console/api/*` mux. The env var is a string (not a bool) so future modes (`approval-required`, custom policies) become additive without breaking the v1 surface.

- **Live-update transport.** Server-Sent Events only for v1. dagnats's live-update workload is overwhelmingly one-way (run-state transitions, audit appends, DLQ entries, metric ticks) so the simpler primitive applies cleanly. Browser-native `EventSource` reconnection handles transient drops via `Last-Event-ID`. SSE composes with existing proxies/auth/observability with zero special-case config. Datastar's Go SDK is SSE-native, reinforcing the choice. WebSockets remain a known-good extension path: if a future view genuinely needs bidirectional flow during a long stream (e.g., a pause-and-resume log viewer), it becomes a separate ADR scoped to that view, not a wholesale transport switch. Implementation note for PR 3: browsers cap concurrent SSE connections per origin at 6; each page subscribes only to the streams its view needs (audit page doesn't open the metrics stream, etc.); the test in PR 3 verifies subscription budgets on multi-tab scenarios.

- **DAG visualization.** Hand-rolled server-rendered SVG with manual layered (topological-depth) layout. No graph library — no d3, no Cytoscape, no Mermaid. Design language follows GitHub Actions (top-down boxes-and-arrows, status icons, click-for-detail in a side panel) and Dagger.io's trace view (hand-rolled SVG, hierarchical, static layout that never reflows). Status changes update SVG attributes in place; positions never move. A static-SVG export endpoint (`/console/runs/<id>/dag.svg`) reuses the same rendering code. A second view — Dagger-style timeline flamegraph — is reserved behind a disabled "Timeline" toggle on the run detail page; the toggle scaffold ships in v1 but the implementation lands in a future PR.

- **Metrics retention for charts.** Charts read directly from the `TELEMETRY` stream (7-day retention, 1 GB cap; unchanged from current dagnats). Time-range options on the dashboard: 1h / 24h / 7d. Anything beyond 7d is explicitly out of scope — dagnats ships a small Prometheus-format `/metrics` endpoint alongside (~50 LOC) so operators with longer-range requirements pipe to their existing TSDB (Victoria Metrics, ClickHouse, Prometheus + remote write). Server-side downsampling before rendering: 1h range = 60 points (1/minute), 24h = 144 (1/10min), 7d = 168 (1/hour); downsampling happens in Go, the browser receives the prepared time-series. The Prometheus exporter and the in-console charts are independent — operators who only want the dashboard view don't need to configure anything; operators who want longer history don't need to configure dagnats's storage cap.

- **Multi-cluster awareness.** Out of scope for v1. One dagnats binary, one NATS cluster, one console. Operators running multiple environments (staging, prod, dev) run multiple dagnats binaries on different hosts/ports and bookmark each environment's URL. The browser's tab/bookmark/URL bar is the cluster picker. A future ADR can address federated multi-cluster consoles for orgs running 10+ dagnats deployments, but the auth-boundary and trust-context implications are substantial enough to warrant their own design pass.

- **Mobile responsiveness.** Kept responsive. Basecoat is responsive by default; cost is zero. Operators on call sometimes need to check status from a phone.

- **i18n.** Not in v1. The help page documents the assumption: "console is English-only; the underlying engine is locale-agnostic." Templates are written so that adding a translation layer later is mechanical (no embedded English in HTML attributes that would require markup changes, only in text nodes). Revisit if a real user request arrives.

- **Theming and aesthetic.** E-ink editorial direction — warm cream backgrounds, warm graphite text, low-saturation accents, no pure white or pure black. Inspired by Kindle Paperwhite, iA Writer, Bump.sh, and the [Craft Design Group](https://craftdesign.group/) site's restrained editorial feel. Light mode is the default; dark mode is an explicit toggle via Basecoat's dark-mode primitives. Single env var `DAGNATS_CONSOLE_THEME=eink` (default) selects the palette; future themes are additive.

  Anchor palette (starting values; the implementation PR can refine):

  | Role | Light | Dark |
  |---|---|---|
  | Background | `#F8F5EF` (warm cream) | `#1A1814` (warm near-black) |
  | Surface | `#FFFCF6` (whisper-warm white) | `#252220` (raised paper) |
  | Border | `#D6CFC0` (paper edge) | `#3A3530` (faded charcoal) |
  | Text primary | `#1F1B16` (warm graphite) | `#E8E2D6` (warm cream) |
  | Text secondary | `#5A554C` (faded ink) | `#9B968B` (faded warm gray) |
  | Accent | `#3F5363` (paper indigo) | `#7A8E9D` (lifted paper indigo) |

  Status palette (muted but icon-paired for legibility — every status color is accompanied by a geometric badge so the signal is multi-modal and survives colorblindness):

  | State | Light | Dark | Icon |
  |---|---|---|---|
  | Completed | `#6B7A56` (muted sage) | `#9CAA82` | filled circle `✓` |
  | Running | `#A88950` (muted ochre) | `#CDB178` | pulsing dot `●` |
  | Failed | `#9A6052` (muted terracotta) | `#C18876` | filled triangle `✗` |
  | Skipped | `#807A6E` (warm gray) | `#B5B0A4` | outline circle `⊘` |
  | Pending | `#5A6B7D` (muted slate) | `#8A9BAD` | empty circle `○` |

  Implementation: the palette lives as CSS custom properties on `<body>` (`--bg`, `--surface`, `--border`, `--text-primary`, `--text-secondary`, `--accent`, `--status-completed`, etc.). Basecoat's shadcn theme tokens map to these custom properties via Tailwind config. The dark-mode toggle flips the entire palette via a `data-theme="dark"` attribute on `<body>`. No JavaScript is required for the palette itself; only the toggle persists the operator's preference via `localStorage`.

---

## What this design explicitly defers

- **Per-operator permissions.** All-or-nothing in v1.
- **Multi-tenancy / multi-workspace.** Single dagnats = single workspace.
- **Workflow editor / IDE-style writer.** Authors still write JSON in their editor of choice. The UI is for operators, not authors.
- **Visual workflow builder.** Same reasoning; not in scope.
- **Workflow version diff tool.** Listed as a low-leverage rare workflow; ship if time permits, otherwise defer.
- **Alerting / notifications.** Operators set up alerts on the TELEMETRY stream via their existing observability stack (Prometheus, etc.). The console renders; it doesn't notify.
- **Audit log analyzer.** The `CONSOLE_ACTIONS` stream is a write target; building tools to analyze it is a future story.

---

## Next step

Grill the open questions, lock the design, file as ADR-014 ("Embedded control plane UI"). The implementation can then proceed PR-by-PR following the layered plan.

Companion documents in this directory:
- `adr-013-http-trigger-respond-step.md` — the synchronous HTTP API foundation this UI sits on top of
- `dx-tooling.md` — DX for workflow authors (test harness, dev mode) — orthogonal concern
- `serve-command-design.md` — how `dagnats serve` wires up the embedded process
