I'll write the audit report directly as my response. Let me organize the 21 reports into the requested structure.

# DagNats Operator Console — Page-by-Page Audit vs MagicPath Mockup

## 1. Executive Summary

**Verdict tally (21 audited surfaces):**

| Verdict | Count | Pages |
|---|---|---|
| MATCH | 0 | — |
| PARTIAL | 17 | shell, dashboard, workflows, functions, workers, triggers, runs, dlq, logs, metrics, server, connections, streams, consumers, concurrency, kv, audit, config (18 listed — see note) |
| MISSING_FROM_UI | 2 | traces, services |
| UI_ONLY | 1 | leases |
| BROKEN | 0 | — |

Note: 17 PARTIAL + 2 MISSING + 1 UI_ONLY = 20 page-views, plus the **shell** chrome (also PARTIAL) = 21 reports. No surface earned a clean MATCH (every live page diverges from the mockup in copy, columns, or actions) and none is outright BROKEN (one live-only chart on Metrics mis-renders under sparse data, but degrades cosmetically).

**Biggest gaps:**
1. **Two whole mockup pages unbuilt** — `Traces` (OTLP span waterfall + span-detail KV panel) and `Services` ($SRV roster + endpoint stats) both 404; neither has a nav entry.
2. **Destructive/operator-action layer is systematically absent.** The live console is read-only by design: no Read-only/Destructive posture toggles (shell), no Worker Drain/Resume/Decommission, no Stream Backup/Purge, no KV Purge, no Trigger Add/Edit/Delete, no Run Signal/Cancel, no Connection Drain, no Server lame-duck.
3. **Detail tiers missing for Functions and Workers** — `FunctionDetailView` (contract schemas, providers, invocations, Invoke modal) and `WorkerDetailView` (lifecycle, in-flight tasks) are entirely unbuilt.
4. **Telemetry visualizations dropped** — sparkcards/sparklines (Dashboard, Functions, Consumers Trend, Server connections/storage pie), the Metrics anomaly callout, and the Concurrency Blocked-runs table are absent.

**Strongest matches (faithful, live-wired):**
- **Runs** — superset of the mockup; all columns, four working detail tabs (Steps/Events/IO/Trace) on real engine data, plus live stat tiles and pagination.
- **Consumers** — 10 of 11 mockup columns verbatim source-line, real engine state, plus extra summary tiles.
- **Server** — all four health sections (Identity/JetStream/Traffic/Host) live-populated.
- **Logs** — fully-wired SSE live tail, server-side filters, export verified 200.
- **DLQ** — list + modal + full-page detail; live page exceeds the mockup detail spec.

---

## 2. Per-Page Sections (nav order)

### Shell chrome
- **Mockup:** `ConsoleRedesign.tsx` (dn-rail sidebar + dn-topbar + dn-footer)
- **Route:** `/console/` 200 (server-rendered per-route, not SPA) — **PARTIAL**

| Feature | In UI | Works | Note |
|---|---|---|---|
| Side rail + nav | yes | yes | `.console-nav-desktop`; routes work |
| Brand wordmark | yes | yes | `dagnats://` (intentional change, PR ef9a765) |
| Version string v0.1.0 | no | na | Absent; moved to footer "dagnats dev" |
| Nav groups Inventory/Activity/System | yes | yes | Plus standalone Dashboard |
| Nav items | partial | yes | Missing Traces & Services; adds Leases |
| Nav count badges | yes | live | Live counts (differ from hardcoded mockup) |
| Active highlight | yes | yes | `.is-active` |
| Topbar title + lede | yes | yes | |
| Read-only toggle pill | no | na | Absent entirely |
| Destructive toggle pill | no | na | Absent entirely |
| Read-only/Destructive banners | no | na | No toggles → no banners |
| Stat tiles row | partial | yes | In main content, not topbar |
| Footer ● ONLINE dot | partial | yes | Relocated to topbar SSE "live" pill |
| Footer N/N streams | yes | yes | "5/5 streams" |
| Footer nats:// url + copy | yes | wired | Copyable affordance |
| Footer commit hash | no | na | Not surfaced |
| Sidebar collapse / theme toggle / cmdk / loopback actor | yes | yes | Additive, not in mockup |

**Key gaps:** Read-only & Destructive posture toggles + banners; version string; commit hash.

---

### Dashboard
- **Mockup:** `ConsoleViewsObserve.tsx:DashboardView` — **PARTIAL**

| Feature | In UI | Works | Note |
|---|---|---|---|
| Explainer banner | yes | yes | Verbatim |
| Sparkcard: throughput 142/s | no | na | Replaced by live tiles |
| Sparkcard: p50 latency | no | na | Absent |
| Sparkcard: error rate | partial | yes | Reframed as Failed/Success tiles |
| Live tile: Failed runs (1h) | yes | yes | Deep-links `?status=failed&range=1h` |
| Live tile: DLQ depth | yes | yes | → /console/dlq |
| Live tile: In-flight runs | yes | yes | → ?status=running |
| Live tile: Success rate (1h) | yes | yes | → /console/metrics |
| Recent failures table | yes | yes | Real seeded row |
| Recent operator actions | yes | empty | "No operator actions recorded yet." |

**Key gaps:** 3 mockup sparkcards (fake telemetry) swapped for 4 live deep-linked counters — an honest improvement; operator-actions empty (no seeded activity).

---

### Inventory ▸ Workflows
- **Mockup:** `WorkflowsView` + `WorkflowDetailView` — **PARTIAL**

| Feature | In UI | Works | Note |
|---|---|---|---|
| List Name / Steps / Status pill | yes | yes | demo-noop, 1 step, ✓ completed |
| Runs 24h column | no | na | Absent |
| 24h trend sparkline | partial | empty | "Activity (24h)" cell empty (0 runs) |
| Avg column | no | na | Absent |
| Trigger type pill | partial | empty | Replaced by numeric Triggers count (0) |
| Run / Open actions | yes | na | Run button present (not clicked) |
| Filter / Sort / Version (extras) | yes | yes | Additive |
| Detail Steps DAG | partial | yes | Raw JSON, not numbered DAG w/ edges |
| Detail Run/Edit actions | no | na | Absent |
| Detail Recent runs table | partial | yes | Drops "Trigger" col; run links work |

**Key gaps:** trend sparkline, Runs-24h/Avg columns, DAG visualization (shown as JSON), detail Run/Edit buttons.

---

### Inventory ▸ Functions
- **Mockup:** `FunctionsView` + `FunctionDetailView` + `FunctionInvokeModal` — **PARTIAL** (served by renamed "Task Types" page)

| Feature | In UI | Works | Note |
|---|---|---|---|
| List service::name / Owners | yes | empty | 0 workers connected |
| Pending column | no | na | Absent |
| Rate-1h sparkline | partial | empty | Hard-coded -1 em-dash, no spark |
| Avg / Fail% columns | yes | empty | -1 sentinel placeholders |
| Per-row Invoke button | no | na | Absent |
| Row → function detail | partial | na | Links to /console/runs instead |
| Empty state + docs CTA | yes | yes | Verified |
| **Entire FunctionDetailView** | no | na | Health pill, stat tiles, contract schemas, providers, invocations all absent |
| **FunctionInvokeModal** | no | na | Absent entirely |
| Service-prefix grouping (extra) | yes | empty | Additive |

**Key gaps:** whole detail view, invoke modal, Pending column, sparklines, real metrics (placeholders pending per-function histogram).

---

### Inventory ▸ Workers
- **Mockup:** `WorkersView` + `WorkerDetailView` — **PARTIAL**

| Feature | In UI | Works | Note |
|---|---|---|---|
| Worker list table | yes | empty | "No workers currently registered." |
| Columns Worker/TaskTypes/Host/LastSeen/Status | yes | empty | Status is plain text, no dot |
| Group column | no | na | Absent |
| Provision worker button + modal | no | na | Absent |
| Self-register explainer copy | no | na | Absent |
| Clickable rows / chevron | no | na | rowClickable=false |
| Header stat strip (extra) | yes | yes | workers/active/stale tiles |
| **Worker detail view** | no | na | 404 — no template/handler |
| Detail stat tiles / fn table / in-flight table | no | na | Absent |
| Drain/Resume/Decommission lifecycle | no | na | Absent |

**Key gaps:** entire detail tier + all lifecycle operator actions; Provision modal; Group column.

---

### Inventory ▸ Triggers
- **Mockup:** `TriggersView` + `TriggerDetailView` + `TriggerModal` — **PARTIAL** (read-only adaptation)

| Feature | In UI | Works | Note |
|---|---|---|---|
| List type badge/id/workflow/target | yes | empty | 0 triggers seeded |
| Type filter | yes | empty | Real GET form |
| Stat tiles (extra) | yes | empty | TRIGGERS/ACTIVE/DISABLED |
| Activity sparkline (extra) | yes | empty | |
| Empty state + docs | yes | yes | Richer than mockup |
| + Add trigger button/modal | no | na | Dropped (read-only) |
| Detail config + raw JSON | yes | empty | |
| Detail Fire / Disable actions | yes | empty | Present, gated (not fired) |
| Detail Edit / Delete | no | na | Dropped (read-only) |
| Detail Fire history table | yes | empty | Run links wired |

**Key gaps:** all mutation/CRUD (Add/Edit/Delete + modal) — intentional read-only stance; 0 seeded data.

---

### Activity ▸ Runs
- **Mockup:** `RunsView` + `RunDetailView` — **PARTIAL** (superset of mockup)

| Feature | In UI | Works | Note |
|---|---|---|---|
| Workflow / Status filters | yes | yes | status=failed verified |
| Find-by-id input | yes | na | Present |
| All list columns (id/wf/status/trigger/started/duration) | yes | yes | Duration "—" for instant runs |
| Per-row action | partial | yes | "Inspect" vs mockup Fire/Run |
| Range filter / stat tiles / pagination (extras) | yes | yes | failed tile deep-links |
| Detail header | yes | yes | Richer than mockup |
| Detail tabs Steps/Events/IO/Trace | yes | yes | All populate real engine data |
| Signal action | no | na | Absent |
| Cancel action | no | na | Absent |
| Gantt Timeline tab | partial | empty | Replaced by plain Steps list (0ms) |

**Key gaps:** Signal + Cancel operator actions; gantt-style step timeline.

---

### Activity ▸ DLQ
- **Mockup:** `DlqView` + `DlqDetailView` — **PARTIAL** (live exceeds detail spec)

| Feature | In UI | Works | Note |
|---|---|---|---|
| List w/ real entry | yes | yes | 1 seeded dead-letter |
| Msg id column | partial | yes | Renamed to "Seq" |
| Step column | no | na | Dropped from list (in detail only) |
| Error column | partial | yes | Renamed "Reason" |
| Age column | partial | yes | Replaced by absolute "Failed at" |
| Retry / Discard actions | yes | na | Present (not fired) |
| Detail (modal + full-page route) | yes | yes | Cross-links wf+run |
| Soft-discard third action | no | na | Absent (Retry+Discard only) |
| Headers/Payload panes | partial | yes | Equivalent fields, different labels |
| Reason-class filter + stat tiles (extras) | yes | yes | |

**Key gaps:** naming/semantic divergence (Seq/Reason/Failed-at), Step list column, Soft-discard, literal NATS Headers pane.

---

### Activity ▸ Logs
- **Mockup:** `LogsView` — **PARTIAL** (fully wired, ring empty)

| Feature | In UI | Works | Note |
|---|---|---|---|
| Source/retention caption | partial | yes | Different retention model |
| Severity filter | partial | yes | Dropdown + read-only count chips (not multi-toggle) |
| Free-text search | yes | yes | Server-side |
| Pause / Export | yes | yes | Export verified 200 |
| Live tail SSE | partial | yes | Status dot "live" |
| Table Time/Level/Source/Message | partial | empty | 4 cols vs 5; ring empty |
| **Clickable trace ID → span tree** | no | na | Static `trace_id=...` text |
| Inline run/step/task chips | no | na | Absent |
| Top sources footer | yes | empty | |
| Trace-id filter + Clear (extras) | yes | yes | |

**Key gaps:** clickable-traceid → traces (the mockup's central interaction), inline log chips.

---

### Activity ▸ Metrics
- **Mockup:** `MetricsView` — **PARTIAL** (substantial redesign)

| Feature | In UI | Works | Note |
|---|---|---|---|
| Pipeline source-line | partial | yes | Drops OTel/SSE/Prometheus detail |
| Anomaly callout + "view runs in window" | no | na | Absent |
| SeriesCards (runs.completed/active/failed, dlq.depth) | partial | empty | Reframed as rate tiles, no raw metric names |
| OTel metric-name/labels mapping | no | na | Absent |
| Concurrency card (acquired=312) | no | na | Absent |
| Snapshot latency p50/p95/p99 | yes | empty | Tile + chart ("no data yet") |
| step.enqueue card | no | na | Absent |
| Prometheus GET /metrics button | no | na | Absent |
| Run throughput chart (extra) | yes | **no** | Mis-renders x-axis (Jun 2026/Dec 2027/Jun 2028) under sparse data |
| Per-workflow table (extra) | yes | yes | Real row + filter drill-in |

**Key gaps:** anomaly callout, OTel name mapping, concurrency/step.enqueue cards, dlq.depth tile, Prometheus button. **Cosmetic bug:** throughput chart time-axis under sparse data.

---

### System ▸ Server
- **Mockup:** `ServerHealthView` — **PARTIAL**

| Feature | In UI | Works | Note |
|---|---|---|---|
| Source line | partial | yes | Cites in-process :9180, not :8222 monitor |
| HEALTHY status pill | no | na | Absent |
| Identity (version/uptime/addr) | partial | yes | Adds server name + JS domain |
| Lame-duck action button | no | na | Absent (zero buttons on page) |
| Traffic section | yes | yes | All live |
| Connections sparkline | no | na | Plain number |
| Host (mem/cpu/cores) | yes | yes | Live |
| JetStream capacity + pie | partial | yes | Numeric pct, no pie |
| JetStream stats | partial | yes | Live |
| Top stat tiles (extra) | yes | yes | |
| Slow-consumer clients/routes/leafs footer | no | na | Absent |

**Key gaps:** lame-duck action, HEALTHY pill, storage pie + connections sparkline.

---

### System ▸ Services — **MISSING_FROM_UI**
- **Mockup:** `ServicesView` + `ServiceDetailView`
- **Route:** `/console/services` **404** (no nav entry)

| Feature | In UI | Works | Note |
|---|---|---|---|
| Services nav / route | no | no | 404; no Go route |
| Service roster card + count | no | na | Absent |
| Roster columns (Service/Kind/Version/Commit/Instances/Status/LastSeen) | no | na | Absent |
| kindPill / status pill | no | na | Absent |
| Row → ServiceDetail | no | na | Absent |
| ServiceDetail $SRV endpoints table | no | na | Absent |

**Note:** the `services` KV bucket *is* read live, but only to augment the Functions page (`attachServiceDescriptions`) — never surfaced as a roster. Mockup flags the $SRV endpoint detail as a "preview pending nats-micro adoption"; the **roster+health half is buildable today**.

---

### System ▸ Connections
- **Mockup:** `ConnectionsView` — **PARTIAL**

| Feature | In UI | Works | Note |
|---|---|---|---|
| Source-line explainer | partial | yes | Drops "idle =" clause |
| All table columns (CID/Client/Kind/Lang/RTT/Uptime/Idle/Subs/Pending/In-Out) | yes | yes | 1 real client (console's own) |
| Idle-warning callout | no | empty | Not implemented + no multi-client data |
| Header count "N · 0 slow consumers" | partial | yes | Replaced by stat tiles |
| **Actions column + per-row Drain** | no | no | Absent; read-only by design |
| Drain ConfirmModal | no | no | Absent |
| CONN_PENDING_NOTE footer | no | empty | Absent |
| Graceful degrade (extra) | yes | na | Live-only robustness |

**Key gaps:** Drain action + modal; explanatory callout/footer blocks (partly empty-demo-data with 1 client).

---

### System ▸ Streams
- **Mockup:** `StreamsView` + `StreamDetailView` — **PARTIAL**

| Feature | In UI | Works | Note |
|---|---|---|---|
| List Stream/Subjects/Messages/Bytes/Consumers | yes | yes | Real engine data, drill-in works |
| Retention / Storage pills | no | na | Dropped from list |
| Seq / Deleted / Policy columns | no | na | Dropped |
| Open column + stat tiles (extras) | yes | yes | |
| Detail Config + State + Consumers cards | partial | yes | Config omits Dedup/max-age; State omits Deleted |
| **Backup (snapshot) action** | no | na | Absent |
| **Purge… action (tier-2 typed confirm)** | no | na | Absent |
| Consumer table cols | partial | yes | Relabeled Ack-pending→Ack, Redelivered→Lag |

**Key gaps:** Backup + Purge destructive actions; 5 list columns + retention/storage pills.

---

### System ▸ Consumers
- **Mockup:** `ConsumersView` — **PARTIAL** (strong match)

| Feature | In UI | Works | Note |
|---|---|---|---|
| Source-line description | yes | yes | Verbatim |
| 10 of 11 columns | yes | yes/empty | Real engine state (2 consumers) |
| Danger callout banner | no | empty | No stalled consumer seeded |
| "Durable consumers / N bound" header | partial | yes | Replaced by CONSUMERS tile |
| **Trend sparkline (11th column)** | no | no | Entirely absent |
| Conditional styling (amber/red/ack-none) | — | empty | Templates support it; not exercised |
| Stat tiles (extra) | yes | yes | CONSUMERS/PENDING/MAX-LAG/STALLED |

**Key gaps:** per-row lag Trend sparkline; danger callout (conditional, unseeded).

---

### System ▸ Concurrency
- **Mockup:** `ConcurrencyView` — **PARTIAL** (faithful but thinner)

| Feature | In UI | Works | Note |
|---|---|---|---|
| Intro explainer | yes | yes | |
| Stat tiles (extra) | yes | empty | locks/in-flight/rate-limited |
| Worker-starvation danger callout | no | na | Absent (mockup's marquee teaching moment) |
| Slot pool table | partial | empty | Drops Limit/Utilization-spark/Waiting |
| Singleton locks table | partial | empty | 3 of 7 columns |
| Rate limits table | partial | empty | Drops Retry-after |
| Debounce table | partial | empty | 2 of 5 columns |
| **Blocked runs table** | no | na | Absent (most operationally actionable section) |
| MaxSteps honesty note | no | na | Absent |
| Lazy-empty KV explainer (extra) | yes | yes | Honest framing |

**Key gaps:** Blocked-runs table, worker-starvation callout, dropped columns. *Caveat: section-header labels render at near-invisible contrast on dark theme.*

---

### System ▸ KV
- **Mockup:** `KvView` (catalog) — **PARTIAL** (fundamentally divergent design)

| Feature | In UI | Works | Note |
|---|---|---|---|
| Catalog source-note ("no external KV read API") | no | na | Live contradicts it — IS an inspector |
| 7 role-grouped bucket cards | no | na | Flat list instead |
| TTL column (load-bearing contract) | no | na | Absent — mockup's key fact |
| History / Churn / Purpose columns | no/partial | na | Mostly absent |
| "Inspect in" cross-links | no | na | Absent |
| Per-bucket Purge action | no | na | Absent (read-only) |
| Full ~19-bucket catalog | partial | empty | Only 5 buckets surfaced |
| **3-pane BUCKETS/KEYS/VALUE inspector** | yes | yes | UI-only; reads real JSON values + rev badge |
| Stat tiles (extra) | yes | yes | 5 BUCKETS / 11 KEYS |

**Key gaps:** entire catalog taxonomy (TTL/Churn/Purpose/role-grouping/cross-links/purge) — but live ships a *working value inspector the mockup said didn't exist*. Different feature, not a thinner one.

---

### System ▸ Audit
- **Mockup:** `AuditView` — **PARTIAL**

| Feature | In UI | Works | Note |
|---|---|---|---|
| "Audit log" heading | yes | yes | |
| console_audit/90-day-TTL srcline | no | na | Absent |
| Forward-auth identity banner | no | na | Absent |
| Denied-count warning callout | no | na | Absent |
| Outcome filter chips | no | na | Replaced by Actor input + Action/Range dropdowns |
| Actor filter | partial | empty | Free-text vs chips |
| Action / Range filters | yes | empty | Real GET form |
| Table Time/Actor/Action/Target/Outcome/Data | yes | empty | console_audit = 0 keys |
| Filter apply | yes | yes | Verified re-renders |
| Empty state + cross-links (extra) | yes | yes | |

**Key gaps:** 3 explanatory blocks (provenance srcline, identity banner, denied callout); outcome-chip filter model. Table empty (no seeded actions).

---

### System ▸ Config
- **Mockup:** `ConfigView` — **PARTIAL** (re-interpretation)

| Feature | In UI | Works | Note |
|---|---|---|---|
| Header stat tiles | partial | yes | Real counts, adds DLQ |
| **Access posture card** (auth pills/read-only/→Audit) | no | na | Absent |
| Endpoints panel | partial | yes | 2 of 5 (no Monitor/OTLP/bridge rows) |
| Build info | partial | yes | No commit/built-date/ldflags |
| **Effective config table** | no | na | Absent (mockup flagged backend-pending) |
| **Engine invariants table (13 rows)** | no | na | Absent (distinctive mockup section) |
| Worker pools | yes | empty | Empty-state (no workers) |
| JetStream streams + KV tables (extra) | yes | yes | Real data |
| Registered trigger types (extra) | yes | yes | |
| Export config as YAML | yes | yes | **Ships working** (mockup had it disabled) |

**Key gaps:** Access-posture card, Effective-config table, Engine-invariants table (the mockup's three distinctive security/config-resolution surfaces).

---

### Leases — **UI_ONLY** (not in mockup)
- **Route:** `/console/leases` 200 (honest empty stub)

| Feature | In UI | Works | Note |
|---|---|---|---|
| Route + nav item | yes | yes | UI invention |
| "Not yet wired" info callout | yes | yes | Honest |
| 6-col table (Lease/Worker/Workflow/Step/Acquired/Expires) | yes | empty | Hard-coded nil — always empty |
| Near-expiry highlight | yes | na | Unverifiable (no data) |

See §4 for the argument.

---

## 3. MISSING FROM UI — Prioritized Backlog

| # | Item | Page | Scope | Buildable today? |
|---|---|---|---|---|
| **P1** | **Worker detail view** (lifecycle, stat tiles, registered-fns, in-flight tasks) + **Drain/Resume/Decommission** actions | Workers | Large | Yes — workers KV is live |
| **P1** | **Function detail view** (contract schemas, providers, recent invocations) + **Invoke modal** | Functions | Large | Partial — needs per-function metrics |
| **P2** | **Services page** (roster + kind/status pills + instance counts) | Services | Medium | Yes (roster half); $SRV detail needs nats-micro |
| **P2** | **Traces page** (TracesView table + TraceDetailView span waterfall + span-detail KV) | Traces | Large | Deferred — needs OTLP telemetry.spans.* pipeline |
| **P2** | **Clickable trace-id → span tree** + inline run/step/task log chips | Logs | Small/Medium | Gated on Traces page existing |
| **P3** | **Blocked-runs table** (which runs wait on which gate) | Concurrency | Medium | Yes — most operationally actionable |
| **P3** | Stream **Backup (snapshot)** + **Purge…** (typed-confirm) actions | Streams | Medium | Yes |
| **P3** | Run **Signal** + **Cancel** actions | Runs | Small | Yes |
| **P3** | Connection **Drain** action + ConfirmModal | Connections | Small | Yes |
| **P3** | Server **lame-duck mode** action + HEALTHY pill | Server | Small | Yes |
| **P3** | KV catalog surface (TTL/Churn/Purpose/role-grouping/cross-links) + per-bucket Purge | KV | Medium | Yes (alongside existing inspector) |
| **P3** | Trigger **Add/Edit/Delete** + modal | Triggers | Medium | Policy decision (read-only console) |
| **P4** | Telemetry sparkcards/sparklines: Dashboard throughput/p50, Functions Rate-1h, Consumers Trend, Server connections+storage-pie, Workflows 24h-trend | Many | Medium | Needs per-entity histograms |
| **P4** | Metrics: anomaly callout, OTel metric-name/labels mapping, concurrency + step.enqueue cards, dlq.depth tile, Prometheus button | Metrics | Medium | Mixed |
| **P4** | Config: Access-posture card, Effective-config table, Engine-invariants table | Config | Medium | Invariants buildable; effective-config backend-pending |
| **P4** | Shell: Read-only/Destructive posture toggles + banners; version string; commit hash | Shell | Small | Yes |
| **P5** | Dropped list columns: Workflows (Runs-24h/Avg), Streams (Retention/Storage pills, Seq/Deleted/Policy), Concurrency (per-table cols), Functions (Pending) | Many | Small each | Yes |

**Plus a fix (not missing, broken):** Metrics Run-throughput chart mis-renders its time axis under sparse data (`Jun 2026 / Dec 2027 / Jun 2028`) — domain/binning bug.

---

## 4. UI-ONLY (not in mockup) Rollup

| Element | Page | For | Against |
|---|---|---|---|
| **Leases page** | Leases | Honest stub, no fake data; signals a planned engine lease feed | Permanently empty (hard-coded nil); zero operational value today; **overlaps Concurrency**, which already owns the admission layer (singleton_locks, slot pool, rate limits) it gestures at; risks operator confusion as a peer of Concurrency in nav |
| **3-pane KV value inspector** | KV | Real working capability the mockup explicitly said dagnats lacked; reads live JSON + rev badges | Replaces (doesn't complement) the mockup's catalog/TTL-contract surface |
| Sidebar collapse / theme toggle / cmdk palette / loopback actor | Shell | Justified operator-console UX improvements; loopback actor is the honest replacement for posture pills | None material |
| Stat-tile strips | Workers, Triggers, DLQ, Runs, Consumers, Concurrency, Connections, Streams, Server | Operational at-a-glance summaries the mockup lacked | Minor — sometimes replace mockup card-header chrome |
| Range filter / pagination | Runs | Real operational need | None |
| Trace-id filter + Clear (CSRF) | Logs | Justified additions | None |
| Working YAML export | Config | Ships what the mockup left disabled | None |
| Version column, name filter, Sort | Workflows | Surfaces real versioning/usability | None |

**Recommendation on Leases:** fold eventual lease/lock telemetry into the **Concurrency** page (already has singleton_locks + slot-pool sections designed for exactly this), or keep Leases only once a real engine lease feed exists. As shipped it's a placeholder peer-of-Concurrency that's always empty.

---

## 5. Verified Working (confirmed live)

- **Shell:** per-route nav with `is-active` highlight; live count badges (Workflows 1, Runs 10, DLQ 1, Streams 5, etc.); footer "5/5 streams" + copyable `nats://127.0.0.1:9180`; SSE "live" health pill.
- **Dashboard:** explainer banner; 4 live deep-linked stat tiles; Recent failures row (run f8b7cc3bdc14, demo-noop, "demo noop: planned failure") with working links.
- **Runs:** status=failed filter narrows table; run-id → detail navigation; 4 detail tabs (Steps/Events/IO/Trace) populate real engine data (event timeline, JSON input/output, span trace); failed-tile deep-link.
- **DLQ:** 1 real entry; modal + full-page `/console/dlq/1` detail; cross-links to workflow + run; stat tiles (1 ENTRIES / 1 REDRIVE-ELIGIBLE / 0 EXPIRED).
- **Logs:** SSE live tail (`text/event-stream` 200, status "live"); server-side severity/search/trace filters; export verified 200 (text + JSON).
- **Workflows:** list → detail drill-in; Recent runs table with working run-id links; live Last-run timestamp + status pill.
- **Streams:** list → `/console/streams/{name}` detail; real Config/State/Consumers cards (50 msgs, 18.4 KiB, consumer "orchestrator").
- **Consumers:** 2 real consumers (orchestrator/WORKFLOW_HISTORY, workers-demo-noop/TASK_QUEUES) with live Filter/Waiting/AckWait values.
- **Server:** live in-process NATS data (version 2.12.6, uptime, 160 subs, 7139 API calls/265 errors, 503 KiB/21 GiB storage, 66 MiB RSS).
- **Connections:** real connz row (cid 5, RTT 148µs, uptime, 20781/26164 in/out msgs).
- **Metrics:** TELEMETRY-sourced — success-rate 90%, 1 failed, demo-noop 9 completed; per-workflow table + working row filter link.
- **KV:** 5 real buckets, clicking key → renders real JSON value with rev badge; bucket switch repopulates keys.
- **Audit:** filter GET form re-renders filtered table; cross-links to triggers/DLQ.
- **Config:** real counts strip, JetStream streams/KV tables (5 rows each), working inline YAML export.