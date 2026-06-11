# Console implementation plan — porting the redesigned console into dagnats

**Status:** Design note, 2026-06-11. References ADR-014 (audit/auth), ADR-021 (observe-first), ADR-022 (gated write actions), and `console-understandability-plan.md`. The UX source of truth is the MagicPath prototype (component `kindly-stream-5966`, 24 views). This note translates the design into a backend and frontend execution plan: which views exist, which are partial, which gaps to close, and the phased rollout that ties reskin to backend reads to new pages.

---

## 1. Situation — ground truth audit

The redesigned console exists today **only** as a MagicPath prototype (24 views). A ground-truth audit of `internal/console/` shows the real console is far more mature than prior assumption — this is a **redesign-and-extend**, not greenfield.

### Framework already in place
- **DataSource interface** (`internal/console/data_source.go`) — 20 read methods wired end-to-end; tests passing.
- **Auth scaffold** (`auth.go`, `actions.go`, `audit.go`) — read-only mode, permission checks, write-action audit logging all present.
- **Basecoat modal components** — `templates/components/dlq_action_modal.html`, `run_confirm_modal.html` ready for reuse.
- **"Add a page" recipe proven** — template `{{define "content"}}` + `pageContentFiles` map entry + `servePageX` handler + `routes()` registration + nav `<a>` in `layout.html`. Takes ~20 minutes per view.
- **Datastar SSE wiring** — all 14 existing views use it; bundle is `v1.0.0-RC.6` (JS) vs SDK `v1.2.1` (Go).

### Prototype view audit: 14 exist, 3 partial, 7 absent
- **EXISTS (14):** Dashboard, Workflows (list + detail), Runs (list + detail), Triggers (list), DLQ (list + detail), Workers (list), Streams (list), KV (list), Logs, Metrics, Audit, Config, Functions (list), Services (list).
- **PARTIAL (3):** Functions detail (list wired, detail absent); Workers detail (partial via KV + `AggregateTaskTypes`); Streams detail (stream snapshot exists, consumers absent); Server/Health (ops hub sketch, `Varz()/Jsz()/Connz()` unused).
- **ABSENT (7):** Concurrency (gate-state KV reads absent); Services detail; Connections; Traces detail (CLI-only, `cli/trace.go` unported); Consumers (standalone view); KV bucket config (Status().StreamInfo() detail); Effective config (flag/env/file/default with source).

### Two corrections to prior assumptions
1. **KV inspector already exists** — `ListKVKeys`/`GetKVEntry` wired. Only bucket **config** metadata (TTL/history) is missing, plus a "purge bucket" action for tier-2 writes.
2. **Metrics view already uses uPlot** (richer than sparklines for the dashboard graph). Datatype font (OpenType-ligature charts) is the chosen approach for **trend sparklines** in list cells; uPlot stays for the richer metrics charts regardless.

---

## 2. View-by-view gap map

| Prototype view | Real console status | Backend read | Work estimate |
|---|---|---|---|
| Dashboard | EXISTS | wired | CSS reskin only |
| Workflows (list + detail) | EXISTS | wired | CSS reskin only |
| Runs (list + detail) | EXISTS | wired | CSS reskin only; this is the **recommended first vertical slice** |
| Triggers (list + fire-now) | EXISTS | wired | CSS reskin only |
| DLQ (list + detail + redrive/discard) | EXISTS | wired | CSS reskin + add denied/failed outcomes + filters |
| Workers (list) | EXISTS | wired | CSS reskin only |
| **Workers detail** | PARTIAL | `AggregateTaskTypes` wired, process info absent | new view + small backend read (process info from KV or engine state) |
| Streams (list) | EXISTS | wired | CSS reskin only |
| **Streams detail** | ABSENT | `StreamSnapshot` exists, consumers absent | new view + `ListConsumers` backend read |
| KV (list) | EXISTS | wired | CSS reskin only |
| **KV detail (bucket config)** | PARTIAL | list wired, config metadata absent | reskin + `kv.Status().StreamInfo()` read |
| Logs | EXISTS | wired | CSS reskin only |
| Metrics | EXISTS | wired | CSS reskin only; already uses uPlot |
| Audit | EXISTS (`/console/ops/audit`) | wired | CSS reskin + add denied/failed outcomes + filters |
| Functions (list) | EXISTS | wired | CSS reskin only |
| **Functions detail** | PARTIAL | list wired, detail absent | small new view |
| **Concurrency** | ABSENT | gate-state KV reads absent | new view + read concurrency KVs |
| **Server/Health** | PARTIAL (ops hub sketch) | `s.ns` not passed to console | new view + pass embedded NATS server or fetch monitor port |
| **Connections** | ABSENT | `Connz()` unused | new view + `Connz()` backend read |
| **Traces detail** | ABSENT | CLI-only (`cli/trace.go`) | new view + extract `telemetry.spans.*` reader → `DataSource.WatchTraces` |
| Config (self-portrait) | EXISTS | wired | CSS reskin + add effective-config + engine-invariants panel |
| Services (list) | ABSENT | KV roster partial, `$SRV` absent | new view + services discovery |
| **Services detail** | ABSENT | same | new view detail |
| (Consumers standalone) | (power-user optional) | (would reuse `ListConsumers`) | *Phase 4 or deferred* |

---

## 3. Backend to build (the actual work)

The embedded NATS server (`s.ns`) is not currently passed to the console, and some reads are CLI-only. New `DataSource` methods to add (each listed with its NATS primitive and affected views):

1. **ListConsumers** — `js.Stream(name).ListConsumers()` → yields consumer name, state, delivered, pending, ack floor. *Streams-detail, optional Consumers standalone view.*

2. **Server health / Varz / Jsz / Connz** — pass `s.ns` to console (or fetch monitor port) → call `Connz()`, `Varz()`, `Jsz()`. *Server/Health view, Connections view.*

3. **WatchTraces** — extract `cli/trace.go`'s `telemetry.spans.*` KV reader (or direct OTEL collector hook if available). *Traces-detail view.*

4. **Concurrency gate-state** — read `concurrency_tasks`, `singleton_locks`, `rate_limits`, `debounce_state` KV buckets for inflight/queued. *Concurrency "what's blocking?" view.*

5. **KV bucket status & config** — call `kv.Status().StreamInfo()` to get TTL, retention policy, history limit. *KV-detail view.*

6. **Services discovery** — enumerate `services` KV bucket or probe `$SRV.PING` to list registered services and their workers. *Services list/detail views.*

7. **Effective config** — resolve `--flag`/`DAGNATS_*` env/config file/hardcoded default with source attribution (which source won the precedence). *Config view, new "effective config" panel.*

Plus **ADR-022 write actions** (tier-1 → tier-2 → tier-3, behind `CONSOLE_ALLOW_DESTRUCTIVE` flag, following the existing modal+read-only+audit pattern):
- **Tier-1:** Worker drain, connection drain, lame-duck mode, stream backup.
- **Tier-2:** Stream purge (export `cli/clean.go`'s `PurgeStream` logic), KV purge.
- **Tier-3:** Restore from backup, reconciliation ops.

---

## 4. Design-system decisions — token translation

### Tokens
Fold the prototype's borderless-dark `dn-*` CSS token system into the existing `internal/console/assets/app.css` token set (`--bg`, `--surface`, `--accent`, status colors; light + dark already present). **Extend, don't replace**, so the theme toggle survives and reskins land cleanly.

### Sparklines
**Datatype font** (OpenType-ligature charts, used throughout the prototype for trend sparklines in list cells) is the chosen approach. Escape hatch: **fall back to uPlot if Datatype clashes with Basecoat at console density** — validate during the first vertical slice (Runs view). The richer metrics charts continue to use uPlot regardless.

### Data-cell font
IoskeleyMono (or equivalent monospace with tabular numerals) is optional vs current IBM Plex Mono for data cells. Vendor woff2 if adopted; if not, Plex remains.

### Datastar version alignment
JS bundle is `v1.0.0-RC.6` while Go SDK is `v1.2.1`. **Verify compatibility early** so copied prototype patterns work in real code. If a pattern fails, update the bundle or the SDK — don't paper over the delta.

---

## 5. Phased rollout (design decisions made, then front-end, then back-end)

### Phase 0 — Foundation & IA (no backend, no new views)
- Token reconciliation: extend `app.css`, lock the Datatype/uPlot sparkline approach against a **test cell** (not yet in a view).
- Three-layer nav IA in `layout.html`: **Inventory** (Workflows, Functions, Workers, Triggers) / **Activity** (Dashboard, Runs, DLQ, Logs, Metrics) / **System** (Streams, KV, Consumers, Concurrency).
- Finalize font choices (Datatype for sparklines, IoskeleyMono if adoption, validated in Runs cells).
- **Datastar version check** — ensure prototype patterns are compatible.

### Phase 1 — Reskin the 14 existing views (CSS + copy only, zero backend)
- Each view: template HTML + classes → Basecoat classes, CSS → extend token set, copy → "Function" terminology.
- **Recommended first vertical slice:** Runs (list + detail + tabs + live SSE) — richest existing view, proves token translation, locks all design decisions.
- After Runs green, fan out the remaining 13 (Dashboard, Workflows, Triggers, etc.).

### Phase 2 — Cheap new views (small DataSource extensions)
- **Functions detail** — reuse `AggregateTaskTypes`, add detail template.
- **Workers detail** — add process-info read (KV or engine state).
- **Streams detail** — add `ListConsumers` read, new template.
- **KV detail (bucket config)** — add `Status().StreamInfo()` read, reskin.

### Phase 3 — System observability (backend-first per view)
- **Consumers standalone view** (optional; uses Phase 2's `ListConsumers`).
- **Server/Health view** — pass `s.ns` to console, add `Varz()/Jsz()/Connz()` reads.
- **Connections view** — uses `Connz()` from Phase 3's server backend.
- **Concurrency "what's blocking?" view** — read concurrency KVs.
- **Traces detail view** — extract `cli/trace.go` logic into `DataSource.WatchTraces`.
- **Services list/detail views** — services discovery reads.
- **Effective config panel** — resolve flag/env/file/default precedence.

### Phase 4 — ADR-022 write actions (gated, audited, read-only-respecting)
- **Tier-1 actions:** worker drain, connection drain, lame-duck, stream backup (modal + confirm, behind new `CONSOLE_ALLOW_DESTRUCTIVE` flag).
- **Tier-2 actions:** stream purge, KV purge (export `cli/clean.go` logic).
- **Tier-3 actions:** restore, reconciliation (deferred if complexity warrants).

---

## 6. Recommended first step — Runs vertical slice

Do **not** fan out Phase 0 or Phase 1 in parallel. Ship a **single vertical slice end-to-end: the Runs view (list + detail + live SSE)**.

**Why Runs:**
- Richest existing view (multiple tabs, live updates, events, detail panels) — a full proof.
- Proves the Datatype sparkline decision (trend charts in run list cells) against real Datastar bindings.
- Locks the font, token, and Basecoat class choices before reskinning 14 others.
- De-risks "copy the prototype patterns" — if a Datastar pattern fails, we fix it here, not mid-fan-out.
- Ship confidence: one view done well feeds credibility for the phased plan.

**Acceptance:** Runs list and detail render, SSE updates live, sparklines render correctly (Datatype or fallback to uPlot works), theme toggle respects tokens, glossary tooltips work, all in read-only mode (no tier-1 actions yet — those land in Phase 4).

After Runs is shipped and merged, the remaining Phase 1 reskins are independent, and Phase 2–4 can begin in parallel by view.

---

## 7. Relationship to other work

- **console-understandability-plan.md** — the design philosophy and three-layer IA; this note is the execution plan.
- **ADR-014** (audit/auth) — guards all write actions; already wired.
- **ADR-021** (observe-first) — design-note on observability-first architecture; KV watches and Datastar SSE align with it.
- **ADR-022** (gated write actions) — the framework for tier-1/2/3 actions, already partially implemented (run retry/discard via modal).
- **#274** (console UX overhaul) — detailed task list; this note reorders it around phases and execution strategy.
- **cli/trace.go, cli/clean.go** — source of logic for Traces view and Stream/KV purge actions; will be extracted into `DataSource` methods.
