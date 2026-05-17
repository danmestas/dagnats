# Console contributing guide

This is the DX guide for changing the dagnats console
(`internal/console/`). It assumes you've read
[docs/console.md](console.md) (the operator guide) and
[ADR-014](architecture/adr-014-control-plane-ui.md) (the decision
log for the arc).

## File layout

```
internal/console/
├── embed.go              # //go:embed roots for assets + templates
├── handler.go            # Mount(), routes(), template loading
├── auth.go               # AuthMode resolution + middleware
├── csrf.go               # CSRF token issue + verify
├── data_source.go        # DataSource interface + APIDataSource adapter
├── pages.go              # GET pages: dashboard, workflows, runs
├── extra_pages.go        # GET pages: triggers, dlq, audit
├── ops_pages.go          # GET pages: ops index, workers, leases, kv
├── metrics_page.go       # GET page: ops/metrics (dashboard view)
├── metrics_source.go     # Aggregator → MetricsSource adapter
├── metrics_anomaly.go    # Pure anomaly detector + threshold constant
├── metrics_api.go        # JSON endpoint for chart refresh (Datastar)
├── metrics_adapter.go    # AdaptAggregator: server → console binding
├── metrics_stream.go     # SSE stream: tile patches
├── metrics_anomaly_*.go  # Pure unit tests for the detector
├── actions.go            # POST mutation handlers (retry, toggle, …)
├── audit.go              # Audit emitter + KV bucket
├── audit_actions.go      # Action-name constants (single source of truth)
├── audit_metrics.go      # Audit-emitter Prometheus counters
├── dlq_tombstone.go      # Soft-discard tombstone store
├── event_bus_binding.go  # Mutation → SSE event bridge
├── streams.go            # SSE streams (runs, triggers, dlq, heartbeat)
├── streams_extra.go      # SSE streams: run detail, metrics
├── fragments.go          # Datastar fragment endpoints (filter dropdowns)
├── assets/               # Bundled JS/CSS + sources/ for unbundled JS
├── templates/            # Go html/template files (layout + per-page + fragments)
└── *_test.go             # Per-feature test files (red-green TDD)
```

The split between `pages.go`, `extra_pages.go`, `ops_pages.go`, and
`metrics_page.go` is purely a file-size discipline. Adding a new page
goes wherever it fits topically — there's no DI / framework gating
which file gets new code.

## Adding a new page route

Five touchpoints for a new `/console/foo` page:

1. **Template**: write `templates/foo.html` with a `{{define "content"}}`
   block. The shared `layout.html` calls `{{template "content" .}}`.
2. **Handler**: write `servePageFoo(w, r, ts, cfg)` in the right
   `*_pages.go`. Pattern:
   ```go
   func servePageFoo(
       w http.ResponseWriter, r *http.Request,
       ts *templateSet, cfg Config,
   ) {
       if w == nil { panic("servePageFoo: w is nil") }
       if r == nil { panic("servePageFoo: r is nil") }
       ds, ok := requireData(w, cfg, "foo")
       if !ok { return }
       view, err := buildFooView(r.Context(), ds, r.URL.Query())
       if err != nil { /* http.Error 500 */ }
       renderPage(w, r, ts, cfg, "foo", pageData{
           Title: "Foo", Section: "ops", Page: view,
       })
   }
   ```
3. **Test**: write `foo_test.go` opening with a methodology comment.
   Use `mountWithFake(t, fake)`; assert both the positive case (200,
   expected markup) and the negative case (empty data, 503-when-no-DS,
   …).
4. **Embed**: add `//go:embed templates/foo.html` to `embed.go`.
5. **Routes + template map**: register the handler in `routes()` in
   `handler.go`, and add the section key to the `pageContentFiles` map.

The TestEndOfArc smoke catches you if you forget any of these — it
GETs every nav-reachable path and fails on a 500.

## Adding a new fragment endpoint

Fragment endpoints power Datastar's `@get`-on-input pattern (filter
dropdowns, debounced search). They return a partial HTML envelope, not
a full layout.

1. **Template fragment**: add to `templates/fragments/foo.html` with a
   `{{define "foo-row"}}` (or similar) block.
2. **Handler**: write a small handler that executes the fragment via
   `ts.base.ExecuteTemplate(w, "foo-row", data)`. Set
   `Content-Type: text/html; charset=utf-8`.
3. **Signal binding**: in the page template, add a `data-bind:fooFilter`
   signal and a `data-on:input__debounce.200ms="@get('/console/api/fragments/foo')"`
   handler.
4. **Test**: assert the fragment renders without the layout wrapper —
   no `<html>` / `<body>` tags.

## Adding a new metric

The metrics dashboard reads from a `MetricsSource` interface. To plumb a
new metric all the way through:

1. **Producer**: emit a counter / histogram from your subsystem via the
   provider-agnostic `observe.Counter` / `observe.Histogram` interfaces.
2. **Aggregator**: confirm the metric name is registered in
   `internal/console/metrics_adapter.go`'s allowed-list. **Mind the
   cardinality**: avoid label explosion (per-run-id labels are not OK).
3. **Tile / chart**: extend `buildMetricsTiles` (one-shot tile) or
   `buildMetricsCharts` (a chart) in `metrics_page.go`. Each tile must
   render an `Empty=true` placeholder when its underlying metric has
   no samples — the dashboard's "no data yet" copy is part of the
   contract.
4. **JSON refresh**: if the metric backs a chart, extend
   `buildChartByID` in `metrics_api.go` so the SSE-triggered refresh
   path knows about it.
5. **Test**: add to `metrics_page_test.go`. Pattern is `fakeMetricsSource`
   + `exerciseMetrics`. Assert both the seeded case and the empty case.

## Adding a new audit action

Action names are constants in `audit_actions.go`. To add one:

1. Declare `const ActionFoo AuditAction = "foo.bar.baz"`.
2. Register in `KnownActions` so the audit-log filter dropdown shows it.
3. Emit from the mutation handler via the existing `audit.Emit(ctx, …)`
   call site.

The shape is intentional: the constants file is the single source of
truth, and the dropdown rendering reads from `KnownActions` so adding
a constant is enough to surface it in the UI.

## Adding a new DAG step style

DAG visualisation is in `internal/console/dagviz/`. Step shapes /
colours / labels live in one place; adding a new step kind is one
case in the layout switch + one `data-step-status` style in `app.css`.
See `dagviz/README.md` for the layout invariants.

## The four-verifier discipline

Every console PR in the arc was gated on four verifiers:

1. **`dx-audit` skill** — scored end-to-end operator workflows. Read
   `~/.claude/plugins/marketplaces/agent-skills/skills/dx-audit/SKILL.md`.
   Write `/tmp/dx-audit-<pr>.md` per iteration.

2. **`norman` audit skill** — Don Norman's design principles:
   conceptual model, signifiers, feedback, constraints, error recovery.
   Read `~/.claude/plugins/marketplaces/agent-skills/skills/norman/SKILL.md`.
   Write `/tmp/norman-audit-<pr>.md`.

3. **`frontend-design` skill compliance** — locked aesthetic
   (e-ink editorial, Fraunces + IBM Plex Sans, warm cream / warm
   near-black palette). Read
   `~/.claude/plugins/cache/claude-plugins-official/frontend-design/79caa0d824ac/skills/frontend-design/SKILL.md`.

4. **`agent-browser` per iteration** — drive a headless Chrome over
   the locally-running server (PR's binary on `127.0.0.1:8090` during
   development), screenshot into `/tmp/dagnats-pr<n>-screens/`. The
   smoke-test path is in `browser_smoke_test.go`.

Why all four: the dagnats Go tests pass even when the UI is inert
(PR 2 retro: Datastar bootstrap was missing; every Go test was green
while the live page was dead). The four-verifier rule keeps the
human-in-the-loop perspective in the loop.

## Coding rules (CLAUDE.md)

- Errors handled; no `_ = err`.
- Min 2 assertions per function (panics on programmer errors).
- No recursion. Iterative with explicit stack.
- All loops + queues bounded.
- Functions ≤70 lines. Push `if`s up, `for`s down.
- Variables declared close to use. Smallest scope.
- Comments say WHY, not what.
- 100-column hard line limit.
- Test files open with a methodology comment.
- Min 2 assertions per test (positive + negative space).
- Provider-agnostic observability: interfaces in-house, vendor
  adapters separate.
