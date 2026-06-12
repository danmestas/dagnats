# dagnats Console — Prototype-Fidelity Remediation List

Derived from per-page UI audits comparing the **real** dagnats console to the **MagicPath prototype** (target design). Organized so an engineer can work top-down: fix cross-cutting themes once (highest leverage), then structural/IA gaps, then per-page residuals.

---

## 1. Cross-cutting themes (highest leverage)

These patterns recur across many pages. Fixing each one in a shared component/token fixes N pages at once.

### T1 — Remove colored left-accent bars on cards; go borderless/flat
The prototype uses **borderless, 12px-radius filled panels** (bg `#13171d`, no border, no colored left-accent bar). The real console renders **radiused cards with colored left-accent bars** (teal/red/amber).
- **Pages hit:** Runs (teal error banner + red failed-step accent), Concurrency (colored left-accent on all section cards), Ops/IA landing (teal left-accent on Leases/Audit/Metrics cards), DLQ (bordered/outlined cards vs borderless filled), Dashboard (consistent already — no violation).
- **Fix shape:** Define one card token/component (borderless, `bg #13171d`, 12px radius, no `border-left` accent). Replace all per-section card variants with it. Remove the status-driven accent-bar color logic.

### T2 — Replace teal-filled section header bars with quiet uppercase labels
Real cards put a **solid teal-filled header band** atop section cards; prototype uses **borderless flat cards with quiet uppercase section labels** (and a right-aligned context tag).
- **Pages hit:** Workflows (Definition/Triggers/Recent-runs cards), Runs (Run input card, Event timeline card), Server (tinted teal header bar per section), Config (ALL-CAPS gray labels vs title-case + context tag).
- **Fix shape:** Drop the teal header-bar treatment from the card component. Render section titles as quiet uppercase mono labels with an optional right-aligned source/context tag (e.g. `console/auth.go`, `SLOT POOL · CONCURRENCYMANAGER`).

### T3 — Adopt IoskeleyMono + teal/amber/red data-cell typography
Prototype renders **all data cells in IoskeleyMono** with semantic coloring (teal accents, amber for warnings, red for failures). Real uses proportional sans with flat grey/blue and no per-value color semantics.
- **Pages hit:** TaskTypes, Workers, Streams, Consumers, Concurrency, Server, Connections, Logs, DLQ, KV, Ops/Metrics/Audit — essentially every data table.
- **Fix shape:** Apply IoskeleyMono to table/stat-tile data cells globally via a `.data-cell` class. Add semantic color tokens: teal (nominal/links), amber (warning/throttle), red (failure). Apply failure-tinting to fail-rate/lag/pending columns.

### T4 — Restore sidebar nav fidelity (labels, items, badges, glyphs, version)
The nav diverges on naming, membership, and decoration across every page.
- **Naming:** real `Task Types` → prototype **`Functions`**.
- **Missing top-level items:** `Traces`, `Metrics`, `Services`, `Audit log` (real buries Metrics/Audit under `Ops`, lacks Services/Traces entirely — see §2).
- **Missing decoration:** per-item **count badges** (Workflows 12, Functions 19, Workers 4, Triggers 5, Runs 240, DLQ 2, Services 6, Connections 8, Streams 8, Consumers 6, KV 18), **leading glyph icons** (◆/⌗/𝑓/⚙/⏱), inline **version badge `v0.1.0`** next to `dagnats`.
- **Pages hit:** every page (sidebar is global).
- **Fix shape:** Rename `Task Types`→`Functions`. Add glyph + count-badge to the nav-item component (counts sourced from the same aggregates the pages already fetch). Add version badge. Restructure groups per §2.

### T5 — Add global `Read-only: off` / `Destructive: off` posture toggles to the top bar
Prototype shows two mode pills top-right on **every** page. Real has none.
- **Pages hit:** Dashboard, Workflows, Runs, TaskTypes, Workers, Streams, Config, Server, Ops-area, KV, Connections — all.
- **Fix shape:** Add the two posture toggles to the shared top-bar chrome. Wire `Read-only` to `CONSOLE_READ_ONLY`; `Destructive` gates destructive actions (Purge, Delete, Decommission). One component, global mount.

### T6 — Build the missing detail / drill-in views
Real pages stop at a list; prototype rows drill into rich detail pages. This is the single largest functional gap.
- **Pages hit:**
  - **Workers** → `WorkerDetail` (stat tiles PROCESSED·1H / IN-FLIGHT / REDELIVERED / DEDUP HITS, REGISTERED FUNCTIONS table, IN-FLIGHT TASKS table, "HOW THIS WORKER RUNS TASKS" explainer, Drain/Decommission/Resume actions).
  - **Streams** → stream detail (Config card, State card, Consumers-on-this-stream table, Backup/Purge actions).
  - **TaskTypes/Functions** → FunctionDetail (Contract input/output schema cards, PROVIDERS table, RECENT INVOCATIONS table, healthy badge, View-runs/View-DLQ cross-links, Invoke modal).
  - **Triggers** → trigger detail (config + FIRE HISTORY table, Fire-now/Disable/Delete actions).
- **Fix shape:** Make list rows clickable (chevron affordance) and add a detail route per resource. Share the stat-tile + section-card components. Group with §2 since several detail views depend on currently-missing data wiring.

### T7 — Add the missing 4th stat tile on list pages (and color-code tiles)
Multiple list pages show 3 tiles where the prototype shows 4, and tiles lack semantic color.
- **Pages hit:** Workflows (missing `RUNS · 24h`), Runs (missing `P50 DURATION`), Triggers (missing `FIRES · 24H`), TaskTypes (missing `GROUPS` + `FAIL RATE 24H`), Workers (missing `GROUPS` + `STALE`), Streams (missing `MESSAGES`/`CONSUMERS`/`ON DISK`), Concurrency (missing `WORKER-STARVED`), Consumers (alarm tiles `LAG ALARM`/`TASK PENDING`), Connections (missing `Total · since boot` + `Slow consumers`), DLQ (missing `DEAD_LETTERS / STREAM`), Server (tile set mismatch), KV (missing `ROLES`/`TTL-BOUNDED`/`WATCHED`), Dashboard (missing the 3-card sparkline analytics row).
- **Fix shape:** Add the missing tiles per page, sourced from existing aggregates. Apply tile color tokens (teal nominal, amber alarm). This pairs with T3.

### T8 — Restore table column headers, prototype column sets, and badge cells
Real tables drop headers, rename columns, omit sparkline/badge/avg columns, and add real-only columns.
- **Pages hit:** Dashboard (Recent failures / operator actions render header-less stacked rows), Workflows (missing `Runs 24h`/`24h Trend` sparkline/`Avg`/`Trigger` badge), Runs (Events columns renamed), TaskTypes (missing `PENDING`/`ACTIONS`/`RATE 1H` sparkline), Workers (column set diverges), Streams (missing `RETENTION`/`STORAGE`/`SEQ`/`POLICY` pills), Triggers (column set + config/actions), DLQ (column set diverges), Logs (missing `TRACE ID`), Consumers (header casing). 
- **Fix shape:** Standardize on the prototype column sets and labels per page (UPPERCASE mono headers). Render type/status/retention/storage values as **colored pill badges**, not plain text. Add inline sparkline cells where the prototype has them (`24h Trend`, `RATE 1H`, `processed-1h`).

---

## 2. Structural / IA remediations (ordered by impact)

1. **Remove the `Ops` hub; promote its children to top-level nav.** Real nests Metrics, Audit log, Leases under `/console/ops`. Prototype promotes **Metrics** and **Audit log** to top-level. Delete the Ops landing (Leases/Audit/Metrics 3-card page); re-home each child as a top-level route.
2. **Add the top-level `Services` view (entirely missing).** Roster of 6 services / 7 instances: columns Service, Kind, Version, Commit, Instances, Status, Last seen, Note + 4 summary tiles. The prototype folds the lease/process roster into Services; reconcile the real `Leases` page into it.
3. **Add the top-level `Traces` view (entirely missing).** Tiles + table (Trace ID / Root operation / Service / Spans / Duration / Status / Started), row click → span tree. Today the real console only has a Trace tab inside run-detail.
4. **Re-grade nav grouping/order to the prototype 3-layer rail.** ACTIVITY = Runs / DLQ / Logs / Traces / Metrics; SYSTEM = Server / Services / Connections / Streams / Consumers / KV / Concurrency / Audit log / Config. (Real currently uses INVENTORY/ACTIVITY/SYSTEM with Ops+Config trailing.)
5. **Wire telemetry for stubbed pages.** Workers (`Worker telemetry is not yet wired`), Streams (`Stream metadata is not yet wired`, all metric cells `—`), Concurrency (all sections empty), Runs-list (Duration `n/a` every row). These block T6 detail views and T7/T8 columns — they are the data dependency behind several themes.
6. **Build CRUD/action surfaces that are entirely absent:**
   - **Triggers:** `+ Add trigger` modal, row `Edit`, `Fire now`, `Disable`/`Delete`, per-row `Enabled` toggle.
   - **Workers:** `Provision worker`, `Drain`/`Decommission`/`Resume`.
   - **Workflows:** `Run workflow`, `Edit definition`.
   - **Runs:** `Signal`, `Cancel`, `View trace`.
   - **DLQ:** `Soft-discard` (real has only Retry+Discard); per-row Retry disabled when not redrive-eligible.
   - **Functions:** `Invoke` modal.
   - **Streams:** `Backup (snapshot)`, `Purge…`.
7. **Replace the KV inspector with the KV catalog.** Real is a generic 3-pane bucket/key/value inspector over ~5 buckets. Prototype is a **role-grouped catalog** (7 sections: Definitions, Runtime State, Liveness, Idempotency/Dedup, Admission Control, Scheduling/Affinity, Approvals) over ~18 buckets with columns BUCKET / TTL / HISTORY / CHURN / PURPOSE / INSPECT IN (cross-links) / ACTIONS (Purge…), and tiles BUCKETS / ROLES / TTL-BOUNDED / WATCHED. This is a page-concept replacement, not a tweak.
8. **Build the Config governance panels (5 missing).** Add `Access posture`, `Build info`, `Effective config` (KEY/VALUE/SOURCE/ORIGIN with precedence chips), `Engine invariants` (13-row constants table), and `Worker groups` (GROUP/MEMBERS/LAST SEEN). Add `Export config as YAML`. Add Endpoints rows for Monitor / OTLP exporter / HTTP bridge.
9. **Build the Workflows visual DAG/steps view.** Replace the raw-JSON Definition block with a numbered STEPS list: per-step type badges (normal/map/agent/sleep/sub_workflow), `entry` marker, `depends_on <step>` edges, subtitle `5 steps · DAG`.
10. **Restore the Dashboard analytics row + conceptual model.** Add the 3-card sparkline row (THROUGHPUT 142/s, P50 LATENCY 1.2s, ERROR RATE 0.4%) and the conceptual-model sentence ("Workers register Functions · Triggers fire Workflows · …"). Remove the EXTRA `Welcome to the dagnats console` onboarding card (no prototype equivalent).
11. **Build the Runs Timeline (Gantt) tab + Output panel.** Add the `Timeline` tab (`Step timeline · 0.4s total`, horizontal teal duration bars). Reconcile tab set to `Events / IO / Timeline` (+ `View trace` button). Add the second `Output` card to the IO tab (real shows Input only).

---

## 3. Per-page remediation list

> Findings already fully covered by a cross-cutting theme (T1–T8) are dropped here and referenced. Page-specific residuals remain as checkboxes.

### Dashboard
- [ ] (high) Add the conceptual-model sentence ("Workers register Functions · Triggers fire Workflows · a Workflow is a DAG of steps that each call a Function · every firing is a Run…").
- [ ] (high) Add the 3-card sparkline analytics row (THROUGHPUT 142/s, P50 LATENCY 1.2s, ERROR RATE 0.4%) — see T7.
- [ ] (med) Remove the EXTRA `Welcome to the dagnats console` onboarding card + `Got it` button (no prototype equivalent).
- [ ] (med) Recent failures: render a real table with headers RUN ID / WORKFLOW / ERROR (real has header-less stacked rows) — see T8.
- [ ] (med) Recent operator actions: render TIME / ACTOR / ACTION columns; populate rows (e.g. `dlq.retry → dl-6689ab0b`) instead of bare empty state — see T8.
- [ ] (low) Recent-failures ERROR cell: render boxed/badged error strings, not plain red text — see T3/T8.
- [ ] (low) KPI tile labels: `FAILED RUNS · 1H` / `SUCCESS · 24H` (real uses `(1H)` parenthetical + wrong 1H window on success) — fix window + middot formatting.
- [ ] (low) Subtitle wording: `Is anything on fire?` (real over-elaborates).
- [ ] (low) Remove KPI `→` / "click to drill" caption and the teal sparkline/progress bar under each KPI value (prototype KPI tiles are numbers-only; sparklines live in the separate analytics row).
- [ ] (low) Footer status line structure differs (`ONLINE 8/8 streams · …embedded · commit 5824fe3` vs real's System-toggle/live pill) — align structure, drop the live/System pill.
- Covered by themes: mode pills (T5), sidebar labels/badges/glyphs/version (T4).

### Workflows
- [ ] (high) Replace raw-JSON Definition with visual STEPS view — see §2.9.
- [ ] (high) Add `Run workflow` and `Edit definition` action buttons (top-right of detail) — see §2.6.
- [ ] (high) Add per-step type badges + `entry` marker and `depends_on <step>` edges — see §2.9.
- [ ] (med) Detail subtitle `5 steps · DAG` (real shows only a `version 1` pill).
- [ ] (med) List: add `Runs 24h` count column, `24h Trend` sparkline, `Avg` duration column, `Trigger` badge column (real shows numeric Triggers count) — see T8. Add `RUNS · 24h` tile (T7).
- [ ] (med) Detail Recent-runs table: add the `Trigger` column.
- [ ] (med) List `Activity (24h)` column renders empty — populate or replace with sparkline+avg.
- [ ] (low) Remove real-only `Version` and `Status` list columns (not in prototype).
- [ ] (low) List header: `Workflow definitions` card title + `aggregated from WORKFLOW_HISTORY` caption.
- [ ] (low) Remove standalone `Triggers` ("no triggers attached") block from detail (prototype has no separate block).
- [ ] (low) Drop real-only Filter/Sort + prev/next pagination on the list (prototype is a single un-paginated table).
- Covered by themes: teal header bars (T2), sidebar (T4), mode toggles (T5).

### Runs
- [ ] (high) Reconcile tabs to `Events / IO / Timeline` (+ `View trace` button); real has `Steps / Events / Input/Output / Trace` — see §2.11.
- [ ] (high) Add the `Timeline` Gantt tab (`Step timeline · 0.4s total`, horizontal teal duration bars).
- [ ] (high) Add the second `Output` card to the IO tab (real shows only `Run input`).
- [ ] (high) Add `Signal`, `Cancel`, `View trace →` header actions; remove the single `Jump to step` button as the only action — see §2.6.
- [ ] (high) Remove the full `Run failed at step noop` error-banner section (prototype uses only the header status pill).
- [ ] (high) Add `P50 DURATION` (1.2s) stat tile — see T7.
- [ ] (med) Add `< Runs` breadcrumb back-link above the run id.
- [ ] (med) Events table headers → `TIMESTAMP / TYPE / STEP / MESSAGE` (real uses `Time / Event / Step / Data`).
- [ ] (med) Runs-list row action → `Run` (and `Fire now` for cron) + row chevron (real uses `Inspect`).
- [ ] (med) Combine the separate `Find run by id` + Workflow/Status/Range filter blocks into one compact filter row.
- [ ] (med) Remove the `page 1` prev/next pager (prototype shows `showing 1-8 of 240`, no pager).
- [ ] (low) Remove the real-only `Range` filter.
- [ ] (low) Populate Duration column (real shows `n/a` every row — telemetry wiring, §2.5).
- Covered by themes: left-accent banners/cards (T1), teal header bars on IO/Events cards (T2), status pills vs icon+text (T3/T8), sidebar (T4).

### Triggers
- [ ] (high) Add `+ Add trigger` modal (TYPE / TARGET WORKFLOW / CRON EXPRESSION / Enabled, Cancel/Save) — see §2.6.
- [ ] (high) Add row-level `Edit` action.
- [ ] (high) Add `Fire now` action (per row + detail header).
- [ ] (high) Add `Disable` / `Delete` actions (detail header).
- [ ] (high) Build per-trigger detail/drill-in view — see T6.
- [ ] (high) Add `FIRE HISTORY` table (TIME / RUN ID / RESULT with running/completed badges) to detail.
- [ ] (high) Add per-row `Enabled` toggle switches.
- [ ] (med) Add `FIRES · 24H` tile — see T7.
- [ ] (med) Reconcile columns to prototype: `TRIGGER ID / WORKFLOW / TYPE / CONFIG / ENABLED / ACTIONS` (drop real's `Target`/`Activity (24h)`, add `Config`/`Actions`) — see T8.
- [ ] (low) Remove real-only `Type` filter dropdown.
- [ ] (low) Remove real-only empty-state block (lightning glyph, "No triggers configured", "Read trigger docs") and info panel.
- [ ] (low) Panel header → `Configured triggers · evaluated by trigger-svc` (real: bare `Triggers` + `Configured trigger sources.`).
- [ ] (low) Render TYPE as colored pill badges (cron/subject/http/webhook) — see T3.
- [ ] (low) Drop the teal-mono `0 configured.` inline count line.

### TaskTypes (→ Functions)
- [ ] (high) Rename page/nav/title/breadcrumb to `Functions` — see T4.
- [ ] (high) Build FunctionDetail view (Contract input/output schema cards, PROVIDERS table WORKER ID/STATUS/IN-FLIGHT, RECENT INVOCATIONS table TIME/RUN ID/CALLER/STATUS/DURATION) — see T6.
- [ ] (high) Add `Invoke` action + modal (editable PAYLOAD JSON textarea, side-effect warning, Cancel/Invoke).
- [ ] (high) Make rows clickable with a `›` drill-in chevron.
- [ ] (med) Add `View runs →` and `View DLQ entries →` cross-link buttons in detail footer.
- [ ] (med) Add per-function `● healthy` green status badge next to the name.
- [ ] (med) Stat tiles → 4 (`FUNCTIONS / WORKERS / GROUPS / FAIL RATE 24H`); real has 2 (`TASK TYPES / SERVICES`) — see T7.
- [ ] (med) Reconcile columns to `SERVICE::NAME / OWNER WORKERS / PENDING / RATE 1H / AVG / FAIL % / ACTIONS` (add `PENDING`, `ACTIONS`) — see T8.
- [ ] (med) Add inline teal `RATE 1H` sparkline per row.
- [ ] (low) Column headers UPPERCASE mono (`SERVICE::NAME`, `OWNER WORKERS`) — see T3.
- [ ] (low) Add table sub-header caption `Registered functions across all workers` / `aggregated from workers KV`.
- [ ] (low) Subtitle → `Every registered task type across live workers`.
- [ ] (low) Color-code `FAIL %` (red for 100%, emphasized 2.1%) — see T3.
- Covered by themes: mode toggles (T5), IoskeleyMono cells (T3).

### Workers
- [ ] (high) Build WorkerDetail drill-in (header, stat tiles, registered-functions table, in-flight tasks, explainer) — see T6.
- [ ] (high) Add `Drain` / `Decommission` / `Resume` actions — see §2.6.
- [ ] (high) Add `Provision worker` button (top-right of list card).
- [ ] (high) Add detail stat tiles `PROCESSED·1H / IN-FLIGHT / REDELIVERED (NAK/TIMEOUT) / DEDUP HITS`.
- [ ] (high) Add `REGISTERED FUNCTIONS` table (function, pending, in-flight, processed-1h sparkline, avg, fail%).
- [ ] (high) Add `IN-FLIGHT TASKS` table (run id, function, started, ackwait remaining).
- [ ] (high) Wire worker telemetry — remove `Worker telemetry is not yet wired` callout + `no workers reporting` empty row; populate live rows + `workers self-register…` sub-line — see §2.5.
- [ ] (med) Add `HOW THIS WORKER RUNS TASKS` pull-based explainer panel.
- [ ] (med) Summary tiles → 4 (`WORKERS / GROUPS / ONLINE / STALE`); real has 3 (`WORKERS/ACTIVE/IDLE`) — see T7.
- [ ] (med) List columns → `WORKER ID / GROUP / STATUS / TASK TYPES / LAST SEEN / HOST` (add GROUP, TASK TYPES, HOST; drop Current lease/Uptime/Tasks) — see T8.
- [ ] (med) Add per-row chevron affordance (signals mutating controls wired).
- [ ] (low) Add `heartbeats via workers KV` caption beside list card header.
- [ ] (low) STATUS: green online dot + `online` label (real Status unpopulated) — see T3.

### Streams
- [ ] (high) Build stream detail drill-in (Config card + State card + Consumers-on-this-stream table) — see T6.
- [ ] (high) Add `Backup (snapshot)` and `Purge…` detail actions — see §2.6.
- [ ] (high) Detail Config rows: Subjects, Retention, Storage, Dedup/max-age, Replicas.
- [ ] (high) Detail State rows: Messages, Bytes, First-last seq, Deleted, Consumers.
- [ ] (high) Detail Consumers table: CONSUMER / FILTER / PENDING / ACK-PENDING / REDELIVERED.
- [ ] (high) Summary tiles → 4 (`STREAMS / MESSAGES / CONSUMERS / ON DISK`); real has 1 — see T7.
- [ ] (high) Add `RETENTION` (limits/work pill) and `STORAGE` (file/memory pill) columns — see T8.
- [ ] (high) Wire stream metadata — populate Messages/Bytes/Consumers (all `—` today); remove `Stream metadata is not yet wired` banner — see §2.5.
- [ ] (med) Add `SEQ` (range), `DELETED`, `POLICY` (dedup/maxAge/atomic-publish) columns.
- [ ] (med) Remove real-only `Purpose` column.
- [ ] (med) Add subtitle hint `live · click a stream → config, state & consumers` (right-aligned, live dot).
- [ ] (med) Wrap table in a rounded card with header label `JetStream streams` (real table is bare) — see T1/T2.
- [ ] (med) Render Retention/Storage/policy values as borderless rounded pills (teal accent for work).
- [ ] (low) Reconcile stream roster to the 8 prototype streams (WORKFLOW_HISTORY/TASK_QUEUES/EVENTS/DEAD_LETTERS/SLEEP_TIMERS/STICKY_TASKS/TELEMETRY/TRIGGER_HISTORY).
- Covered by themes: IoskeleyMono cells (T3), sidebar (T4), mode toggles (T5).

### Consumers
- [ ] (high) Add amber `no worker consuming` stalled callout (`task.image-pipeline.> — 4 pending, 0 waiting pulls` / "Backlog with no worker consuming. Check Workers →").
- [ ] (high) Add row-level stalled highlight (red `no workers pulling` badge + red pending/lag values on `wkr-image-pipeline`).
- [ ] (med) Tile #2 → `LAG ALARM` (amber); real shows `PENDING`. Tile #3 → `TASK PENDING` (amber); real shows `MAX LAG` — see T7.
- [ ] (med) Wrap table in a card titled `Durable consumers` with right-aligned `N bound` count badge.
- [ ] (low) Color-code tiles (CONSUMERS teal, alarms amber); real CONSUMERS is blue, others uncolored.
- [ ] (low) Keep the generic descriptive sentence as plain text beneath tiles AND add the separate alarm callout (real folds both into one teal banner).
- [ ] (low) Column headers UPPERCASE IoskeleyMono — see T3.
- [ ] (low) Color-code numeric data cells (red stalled, amber redelivered=12) — see T3.

### Concurrency (→ Admission control)
- [ ] (high) Add worker-starvation callout (red-accent narrative + `→ Connections` link, e.g. `image-pipeline::fetch-urls`).
- [ ] (high) Add `Blocked runs` / `Waiting on a gate` section (RUN/WORKFLOW/STEP/GATE/WAITING with gate pills like `global slot · retry-errors::fetch (2/2 full)`).
- [ ] (high) Add `WORKER-STARVED` tile; lead with `RUNS BLOCKED` (real leads with `LOCKS HELD`, has `TASKS IN-FLIGHT` instead) — reconcile to RUNS BLOCKED / LOCKS HELD / RATE-LIMITED / WORKER-STARVED — see T7.
- [ ] (high) Slot pool: add `UTILIZATION` column with colored meter dials (`unlimited` for ∞) — real has only Task type / In-flight.
- [ ] (high) Singleton locks: add `MODE` (Cancel/Queue/Reject badges), `HELD`, `QUEUED`, `REJECTED` columns.
- [ ] (high) Rate limits: add `RETRY AFTER` column (amber when throttling).
- [ ] (high) Debounce: add `SUBJECT`, `WINDOW`, `ABSORBED`, `FIRES IN` columns (real has only Trigger / Timer seq).
- [ ] (med) Page title → `Admission control` (real: `Concurrency`).
- [ ] (med) Slot pool: add `WAITING` column; combine `IN USE / LIMIT` (real bare `In-flight`).
- [ ] (med) Singleton locks: `Scope` as colored badges (workflow/keyed); `Held by` as teal-mono hash link.
- [ ] (med) Add section source-file captions (`SLOT POOL · CONCURRENCYMANAGER`, `SINGLETON LOCKS · ENGINE/ADMISSION.GO`, etc.) and per-card right-aligned meta annotations + sub-headers.
- [ ] (low) Add slot-pool footer counter `task.concurrency.acquired 312`.
- [ ] (low) Rate limits: combine into `TOKENS / LIMIT` (real splits Tokens/Limit).
- [ ] (low) Remove real-only info banner + empty-state footer note.
- Covered by themes: left-accent bars (T1), bare lowercase headers → quiet labels (T2), IoskeleyMono/teal-amber-red cells (T3).

### KV
- [ ] (high) Replace the 3-pane inspector with the role-grouped catalog — see §2.7 (the full page-concept replacement, all its high findings roll up here).
- [ ] (med) Page title → `KV` with catalog-framing subtitle (real: `KV inspector` / "Read-only inspection…").
- [ ] (med) Drop the VALUE drill-into-value pane + `rev N` indicator (no prototype equivalent) once the catalog lands.
- [ ] (med) Fix the KV nav item rendered as a `glo-tooltip-wrapper` (help-cursor + hover popover) — make it a plain `<a>` like other nav items (DLQ shares this bug).
- Covered by themes: mode toggles (T5), IoskeleyMono bucket-name cells + TTL/CHURN colored pills (T3).

### DLQ
- [ ] (high) Convert the right-side drawer to a full-page detail route (`DLQ entry`, `< DLQ` back link).
- [ ] (high) Add `Retry` / `Discard` / `Soft-discard` action buttons to detail (real drawer has none; `Soft-discard` missing entirely) — see §2.6.
- [ ] (high) Add side-by-side `Headers` and `Payload` cards (real has only `Error message` + `Original input`).
- [ ] (high) Show multi-line stack trace in the ERROR card (worker/exec.go, transport.go frames).
- [ ] (high) Reconcile table columns to `MSG ID / WORKFLOW / STEP / ERROR / AGE / ACTIONS` (real: Seq/Reason/Workflow/Run/Failed at/Attempts/Actions) — see T8.
- [ ] (med) Add `DEAD_LETTERS / STREAM` tile — see T7.
- [ ] (med) Key rows on `MSG ID` (`dl-xxxx`), not `Seq`.
- [ ] (med) Show relative `AGE` (6m, 21h) instead of absolute `Failed at` UTC.
- [ ] (med) Expose `STEP` as a top-level table column (real has it only in the drawer).
- [ ] (med) Disable per-row `Retry` when not redrive-eligible (real enables uniformly).
- [ ] (med) Replace `Inspect` button + chevron with prototype's Retry/Discard + chevron-to-detail.
- [ ] (low) Remove real-only `Reason class` filter dropdown.
- [ ] (low) Detail meta line → `dl-… · retry-errors · step fetch · delivery 5/5` (real: `DLQ entry #1` + field list).
- [ ] (low) Remove `Open in full page` drawer button (moot once detail is a full page).
- Covered by themes: borderless filled cards (T1), ERROR pill + IoskeleyMono/teal (T3), nav badges + Functions rename + glo-tooltip bug (T4/KV).

### Logs
- [ ] (high) Add the summary-tile header row (`4.2k LINES/1H`, `38 WARNINGS`, `6 ERRORS`, large `live TAIL` tile) — see T7.
- [ ] (high) Add the `TRACE ID` column (clickable teal trace-id link) — columns → `TIME / SEVERITY / SERVICE / TRACE ID / MESSAGE`.
- [ ] (high) Make trace-id clickable → span-tree drill-in (real only offers a `trace_id (32-hex)` filter input).
- [ ] (high) Render `run_id`/`step_id` chips in the MESSAGE cell (e.g. `run a662ac7e`, `step fetch`).
- [ ] (med) Column headers → `SEVERITY` (real `LEVEL`) and `SERVICE` (real `SOURCE`).
- [ ] (med) Add telemetry-stream descriptor line (`TELEMETRY STREAM · telemetry.logs.{service}.{severity} · 7-day · 1GB · showing last 500`).
- [ ] (med) Severity filter → solid Debug/Info/Warn/Error toggle pills (real uses numeric count chips).
- [ ] (med) Remove real-only extra controls (`Search messages…` box, `All severities` dropdown, `Apply`, `Clear`); prototype has a single `trace id / service / message` input.
- [ ] (low) Subtitle → prototype "Structured log tail across…" framing.
- [ ] (low) Add `click a trace id → span tree · 8 of 8` helper caption.
- [ ] (low) Move LIVE indicator to a large green `live TAIL` header tile.
- Covered by themes: IoskeleyMono + teal trace-id styling (T3).

### Server
- [ ] (high) Add the `HEALTHY` status-pill row (green pill + `nats-server 2.12.1` + `uptime … · healthz ok · 127.0.0.1:4222`).
- [ ] (high) Add the `Lame-duck mode` action button.
- [ ] (high) Add the JetStream storage donut/gauge (`STORAGE (19%)`, `1.9 GiB / 10 GiB`).
- [ ] (med) Top tiles → `UPTIME / SLOW CONSUMERS / JS STORAGE / API ERRORS` (real: STORAGE %/CONNECTIONS/SUBSCRIPTIONS/SLOW CONSUMERS) — see T7.
- [ ] (med) Render JetStream / Traffic / Host sections as tile grids (real renders label/value lists).
- [ ] (med) Reorder sections to Tiles → Status → TRAFFIC → HOST → JETSTREAM CAPACITY.
- [ ] (med) Remove the real-only `Identity` card (folded into the status pill in prototype) and the long prose blurb banner.
- [ ] (med) Add monitor-port context line `Embedded NATS monitor · :8222 · /varz · /jsz · /healthz`.
- [ ] (low) Add JetStream card meta labels (`Storage & API · /jsz`, `store file · /var/lib/dagnats/jetstream`) and the slow-consumers footer note.
- [ ] (low) Connections tile: include `· 41 TOTAL` secondary count.
- Covered by themes: teal header bars (T2), IoskeleyMono/teal numerals (T3), mode toggles (T5).

### Config (→ Configuration)
- [ ] (high) Add `Access posture` panel — see §2.8.
- [ ] (high) Add `Engine invariants` panel (13-row CONSTANT/VALUE/GOVERNS/SOURCE table).
- [ ] (high) Add `Effective config` panel (KEY/VALUE/SOURCE/ORIGIN with default/env/file/flag chips + precedence note).
- [ ] (high) Add `Build info` panel (version, ldflags, commit, built date, NATS server, Go version) — replace the plain footer line.
- [ ] (high) Add `Worker groups` table (GROUP/MEMBERS/LAST SEEN) — real shows an empty `WORKER POOLS` placeholder.
- [ ] (high) Add `Export config as YAML` button (the real `VIEW DEPLOYMENT YAML` expander doesn't open).
- [ ] (high) Replace two separate Console/NATS endpoint cards with a single `Endpoints` panel listing NATS/Console/Monitor/OTLP exporter/HTTP bridge.
- [ ] (high) Add source-attribution chips (default/env/file/flag/hardcoded) throughout.
- [ ] (med) Add right-aligned panel context tags (`console/auth.go`, `fixed contract · compile-time constants`, `preview · not yet wired`, `precedence: …`) — see T2.
- [ ] (med) Keep the real-only `JETSTREAM RESOURCES` + `REGISTERED TRIGGER TYPES` sections only if intentional (not in prototype Config — confirm scope).
- [ ] (low) Page title → `Configuration`; subtitle → prototype "Deployment…".
- [ ] (low) DLQ tile label → `DLQ ENTRIES`.
- Covered by themes: mode toggles (T5), teal mono value cells (T3), section header styling (T2).

### Ops / IA structure
- All findings roll up into §2 (items 1–4): remove `Ops` hub, add `Services`, add `Traces`, re-grade nav grouping/order. Page-specific residuals below.
- [ ] (high) Add Metrics anomaly callout (amber `snapshot.save.duration_ms p99/p50` spike + `View runs in this 90s window` drill button).
- [ ] (high) Add Audit summary tiles (`28 Events·24h / 2 Denied / 1 Failed / 25 Succeeded`) + `denied while read-only` anomaly callout.
- [ ] (med) Metrics tiles → `RUNS/MIN / RUNS ACTIVE / DLQ DEPTH / SNAPSHOT P99/P50`; add the 7 sparkline metric cards (real shows 2 charts + a Per-workflow table).
- [ ] (med) Metrics: add OTel/NATS source line + `Prometheus · GET /metrics` button.
- [ ] (med) Audit filters → chip groups (Outcome: All/Success/Denied/Failed; Actor: All/Dan/Maya/Ci-Bot) instead of free-text + Range + Filter button.
- [ ] (med) Audit columns: drop the real-only `Data` column (prototype is Time/Actor/Action/Target/Outcome) — confirm before removing.
- [ ] (low) Keep the real-only Metrics `Per-workflow` table only if intentional.
- Covered by themes: left-accent cards (T1), IoskeleyMono colored numerics (T3), nav badges/glyphs/Functions rename (T4), mode toggles (T5).

---

## 4. Priority recommendation (fix first)

The 5–8 highest prototype-fidelity wins, ordered:

1. **T1 + T2 — kill the colored left-accent bars and teal header bars; ship the borderless filled card.** One component swap repaints Runs, Concurrency, DLQ, Streams, Server, Config, Workflows, Ops at once. Cheapest broad visual-fidelity gain.
2. **T3 — IoskeleyMono + teal/amber/red data-cell typography.** Global table/tile restyle; the single most pervasive "feels like the prototype" lever.
3. **T4 + §2.1–2.4 — fix the nav: rename Task Types→Functions, add count badges/glyphs/version, remove the Ops hub, promote Metrics + Audit, add Services and Traces top-level, re-grade grouping.** IA is global chrome seen on every page; the Ops/Services/Traces gap is the biggest structural divergence.
4. **T5 — add the global `Read-only` / `Destructive` posture toggles.** Tiny, global, and they gate every destructive action the detail/CRUD work introduces.
5. **§2.5 — wire telemetry for Workers / Streams / Runs-duration / Concurrency.** This is the data dependency that unblocks T6 detail views, T7 tiles, and T8 columns on the most-stubbed pages; do it before/with the detail builds.
6. **T6 — build the detail/drill-in views (Workers, Streams, Functions, Triggers).** The largest functional gap; each shares the now-fixed card/tile/table components.
7. **T7 + T8 — add the missing 4th stat tiles and reconcile column sets/badges/sparklines per page.** Mechanical once the shared tile/badge/sparkline components exist.
8. **§2.6 — add the CRUD/action surfaces (Run/Edit/Signal/Cancel/Fire-now/Provision/Drain/Invoke/Purge/Soft-discard).** Turns read-only viewers into the prototype's actionable control plane; gated by T5's Destructive toggle.
