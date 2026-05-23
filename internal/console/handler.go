package console

import (
	"bytes"
	"context"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/starfederation/datastar-go/datastar"
)

// devMode reflects DAGNATS_DEV=1 at process start. When true the asset
// handlers send no-cache headers so CSS/JS bundle changes show up on
// reload without manually busting the browser cache. Production keeps
// the long-immutable cache for performance — the env var is only meant
// for `go run ./cmd/dagnats` style dev workflows.
//
// This is a package-level var (read once at init) because evaluating
// the env var on every asset request is pointless overhead and the
// value cannot change at runtime — operators set it before starting
// the binary.
var devMode = os.Getenv("DAGNATS_DEV") == "1"

// assetCacheHeader returns the Cache-Control value used by every asset
// handler. Long+immutable in production, no-store + must-revalidate
// in dev mode so source edits surface on browser reload.
func assetCacheHeader() string {
	if devMode {
		return "no-store, must-revalidate"
	}
	return "public, max-age=31536000, immutable"
}

// Config carries the runtime state Mount needs to wire up the console.
//
// HTTPAddr is the listener's resolved address (used to decide whether
// auth must be configured). AuthMode is the result of ResolveAuthMode;
// callers must pass the value they obtained at startup. Password is
// only consulted when AuthMode == AuthBasic. Logger is required and
// must be configured — the package logs via slog so observability
// stays provider-agnostic per the project rules. Data is the read-only
// surface the console renders against; PR 1 left this optional so the
// foundation could land before the api.Service hook-up. PR 2 onward
// expects it set — pages that need data return 503 when Data is nil
// so the dashboard still renders if Data wasn't wired.
type Config struct {
	HTTPAddr string
	AuthMode AuthMode
	Password string
	Build    string
	Logger   *slog.Logger
	Data     DataSource
	// HeartbeatInterval, when zero, defaults to 5 * time.Second. Tests
	// override this so the lifecycle assertions complete quickly.
	HeartbeatInterval time.Duration
	// ReadOnly, when true, refuses every mutation under /console/*
	// with a 405 + JSON body. UI rendering pages also read this flag
	// to render mutation buttons as visible-but-disabled with a
	// tooltip explaining the env var that flipped them off.
	ReadOnly bool

	// DLQSoftDiscard, when true, routes DLQ discard through the
	// in-memory tombstone store: the JetStream entry stays in place
	// until the undo window expires. The Bus, when non-nil, is the
	// event channel mutation handlers publish to so SSE streams can
	// patch rows in/out without a refetch. Both are wired by
	// server.go at startup; tests opt in with helper builders.
	DLQSoftDiscard bool
	tomb           *dlqTombstoneStore
	bus            *eventBusBinding

	// Metrics, when non-nil, exposes the live metric aggregator to
	// the dashboard tiles, the metrics page, and the per-metric SSE
	// patcher. server.go wires this to a metrics.NewAggregator()
	// pumped from the TELEMETRY stream. Nil in tests that don't care
	// about metrics — the dashboard renders empty-state placeholders.
	Metrics MetricsSource

	// MetricsErrorReason is the operator-facing explanation surfaced
	// on /console/ops/metrics when the aggregator failed to start.
	// Empty string ⇒ no aggregator was ever requested (the dashboard
	// renders the neutral "not wired" copy). Non-empty ⇒ startup hit
	// an error; the page renders an alert banner so the operator
	// learns the metrics layer is broken instead of inferring that
	// it's a deferred feature. server.go sets this from the slog
	// warnings the startup pump emits.
	MetricsErrorReason string

	// LogRing, when non-nil, gives /console/logs a Snapshot() of
	// recent engine slog records and a Subscribe() live tail. The
	// production server wires this to a logring.Handler installed via
	// slog.SetDefault — so every engine log line flows through it.
	// Nil ⇒ the Logs page renders an "observability not wired"
	// empty-state instead of returning 503 (the operator may still
	// want the page to load to see the chrome and nav entry).
	LogRing LogTailSource
}

// LogTailSource is the narrow surface /console/logs depends on. It is
// satisfied by logring.Handler but expressed locally so the console
// package never imports observe types — tests pass a fake here without
// pulling slog records through the ring.
type LogTailSource interface {
	// Snapshot returns a freshly-allocated, time-ordered (oldest first)
	// copy of every record currently retained.
	Snapshot() []slog.Record
	// Subscribe returns a channel that receives every record handled
	// after the call. The cleanup func unsubscribes.
	Subscribe(ctx context.Context) (<-chan slog.Record, func())
}

// tombstones returns the configured tombstone store. Internal helper —
// not exported because external callers shouldn't reach into Config's
// lazily-allocated state. The lazy allocation pattern lets tests
// opt into soft-discard without rewriting their config builders.
func (c *Config) tombstones() *dlqTombstoneStore {
	if c == nil {
		return nil
	}
	return c.tomb
}

const defaultHeartbeatInterval = 5 * time.Second

// Mount returns a fully configured http.Handler that serves every
// route under /console/. Callers wire it into their mux:
//
//	mux.Handle("/console/", console.Mount(cfg))
//
// Mount panics on programmer errors (nil logger, basic-auth without a
// password) so misconfiguration fails loudly at startup, not at first
// request.
func Mount(cfg Config) http.Handler {
	if cfg.Logger == nil {
		panic("console.Mount: cfg.Logger is nil")
	}
	if cfg.HTTPAddr == "" {
		panic("console.Mount: cfg.HTTPAddr is empty")
	}
	if cfg.AuthMode == AuthBasic && cfg.Password == "" {
		panic("console.Mount: basic-auth selected but Password is empty")
	}
	ts, err := loadTemplates()
	if err != nil {
		panic(fmt.Sprintf("console.Mount: load templates: %v", err))
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}
	if devMode {
		cfg.Logger.Warn("console dev mode active — asset caching disabled (DAGNATS_DEV=1)")
	}

	mux := http.NewServeMux()
	routes(mux, ts, cfg)

	guarded := readOnlyMiddleware(cfg.ReadOnly, mux)
	csrfGuarded := csrfMiddleware(cfg.AuthMode, guarded)
	return authMiddleware(cfg.AuthMode, cfg.Password, csrfGuarded)
}

// routes wires every public path under /console/ into mux.
// Keeping this on a separate function makes the route inventory
// easy to scan.
func routes(mux *http.ServeMux, ts *templateSet, cfg Config) {
	if mux == nil {
		panic("routes: mux is nil")
	}
	if ts == nil {
		panic("routes: ts is nil")
	}
	mux.HandleFunc("/console/", func(w http.ResponseWriter, r *http.Request) {
		dispatchRoot(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/workflows", func(w http.ResponseWriter, r *http.Request) {
		servePageWorkflowsList(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/workflows/", func(w http.ResponseWriter, r *http.Request) {
		dispatchWorkflows(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/runs", func(w http.ResponseWriter, r *http.Request) {
		servePageRunsList(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/runs/lookup", func(w http.ResponseWriter, r *http.Request) {
		serveRunIDLookup(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/runs/", func(w http.ResponseWriter, r *http.Request) {
		servePageRunDetail(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/api/run/",
		func(w http.ResponseWriter, r *http.Request) {
			serveRunTabFragment(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/api/fragments/workflows-list",
		func(w http.ResponseWriter, r *http.Request) {
			serveFragmentWorkflowsList(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/api/fragments/runs-list",
		func(w http.ResponseWriter, r *http.Request) {
			serveFragmentRunsList(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/triggers",
		func(w http.ResponseWriter, r *http.Request) {
			servePageTriggersList(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/triggers/",
		func(w http.ResponseWriter, r *http.Request) {
			dispatchTriggers(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/dlq",
		func(w http.ResponseWriter, r *http.Request) {
			servePageDLQList(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/dlq/",
		func(w http.ResponseWriter, r *http.Request) {
			dispatchDLQ(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/api/dlq/",
		func(w http.ResponseWriter, r *http.Request) {
			dispatchDLQAPIFragment(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/api/runs/",
		func(w http.ResponseWriter, r *http.Request) {
			serveRunSheet(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/api/search",
		func(w http.ResponseWriter, r *http.Request) {
			serveSearch(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/workers",
		func(w http.ResponseWriter, r *http.Request) {
			servePageWorkers(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/kv",
		func(w http.ResponseWriter, r *http.Request) {
			servePageKVInspector(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/streams",
		func(w http.ResponseWriter, r *http.Request) {
			servePageStreams(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/logs",
		func(w http.ResponseWriter, r *http.Request) {
			servePageLogs(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/sse/logs",
		func(w http.ResponseWriter, r *http.Request) {
			serveSSELogs(w, r, cfg)
		})
	mux.HandleFunc("/console/config",
		func(w http.ResponseWriter, r *http.Request) {
			servePageConfiguration(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/task-types",
		func(w http.ResponseWriter, r *http.Request) {
			servePageTaskTypes(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/ops",
		func(w http.ResponseWriter, r *http.Request) {
			servePageOpsIndex(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/ops/workers",
		func(w http.ResponseWriter, r *http.Request) {
			serveOpsWorkersRedirect(w, r)
		})
	mux.HandleFunc("/console/ops/leases",
		func(w http.ResponseWriter, r *http.Request) {
			servePageLeases(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/ops/kv",
		func(w http.ResponseWriter, r *http.Request) {
			serveOpsKVRedirect(w, r)
		})
	mux.HandleFunc("/console/ops/audit",
		func(w http.ResponseWriter, r *http.Request) {
			servePageAuditLog(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/ops/metrics",
		func(w http.ResponseWriter, r *http.Request) {
			servePageMetrics(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/sse/metrics",
		func(w http.ResponseWriter, r *http.Request) {
			serveSSEMetrics(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/api/metrics/chart/",
		func(w http.ResponseWriter, r *http.Request) {
			serveAPIMetricsChart(w, r, cfg)
		})
	mux.HandleFunc("/console/assets/console.js", serveGzAsset("console.js.gz",
		"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/assets/basecoat.css", serveGzAsset("basecoat.css.gz",
		"text/css; charset=utf-8"))
	mux.HandleFunc("/console/assets/uplot.min.js", serveGzAsset("uplot.min.js.gz",
		"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/assets/app.css", servePlainAsset("app.css",
		"text/css; charset=utf-8"))
	mux.HandleFunc("/console/assets/connection-state.js",
		servePlainAssetAt("sources/connection-state.js",
			"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/assets/toast.js",
		servePlainAssetAt("sources/toast.js",
			"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/assets/count-chip.js",
		servePlainAssetAt("sources/count-chip.js",
			"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/assets/metrics.js",
		servePlainAssetAt("sources/metrics.js",
			"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/assets/onboarding.js",
		servePlainAssetAt("sources/onboarding.js",
			"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/assets/build-info-copy.js",
		servePlainAssetAt("sources/build-info-copy.js",
			"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/assets/sidebar-collapse.js",
		servePlainAssetAt("sources/sidebar-collapse.js",
			"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/assets/logs.js",
		servePlainAssetAt("sources/logs.js",
			"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/sse/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		serveHeartbeat(w, r, ts, cfg.HeartbeatInterval)
	})
	mux.HandleFunc("/console/sse/dashboard", func(w http.ResponseWriter, r *http.Request) {
		serveSSEDashboard(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/sse/runs", func(w http.ResponseWriter, r *http.Request) {
		serveSSERuns(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/sse/runs/", func(w http.ResponseWriter, r *http.Request) {
		serveSSERunDetail(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/sse/triggers", func(w http.ResponseWriter, r *http.Request) {
		serveSSETriggers(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/sse/dlq", func(w http.ResponseWriter, r *http.Request) {
		serveSSEDLQ(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/assets/fonts/", serveFontAsset())
	// Fixture pages for Phase 2 component vendoring smoke tests. Gated
	// by DAGNATS_FIXTURES=true so production never exposes them. Tests
	// flip the env var on before calling Mount.
	if fixturesEnabled() {
		mux.HandleFunc("/console/__fixtures__/",
			func(w http.ResponseWriter, r *http.Request) {
				serveBasecoatFixture(w, r)
			})
	}
}

// dispatchRoot picks between dashboard (/console/, /console) and 404.
// Previously serveDashboard owned both checks; splitting lets the
// dashboard handler stay focused on rendering, the dispatcher on
// routing. We can't bind /console/ exclusively because Go's mux
// makes that prefix-greedy — anything not matched elsewhere falls
// through here and we need to NotFound it. PR 4 wraps the 404 in
// the console layout so the operator keeps the chrome + a back link.
func dispatchRoot(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchRoot: w is nil")
	}
	if r == nil {
		panic("dispatchRoot: r is nil")
	}
	if r.URL.Path != "/console/" && r.URL.Path != "/console" {
		serveNotFound(w, r, ts, cfg)
		return
	}
	serveDashboard(w, r, ts, cfg)
}

// serveNotFound renders the layout-wrapped 404 page. Used in place of
// http.NotFound across the console so the operator always keeps the
// header + a clear path back to the dashboard. Sets X-Robots-Tag:
// noindex so external crawlers don't accumulate dead URLs.
func serveNotFound(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveNotFound: w is nil")
	}
	if r == nil {
		panic("serveNotFound: r is nil")
	}
	if ts == nil {
		panic("serveNotFound: ts is nil")
	}
	actor, _ := ActorFrom(r.Context())
	data := pageData{
		Title:   "Not found",
		Section: "",
		Actor:   actor,
		Overview: overviewData{
			Listener: cfg.HTTPAddr,
			AuthMode: cfg.AuthMode.String(),
			Build:    cfg.Build,
		},
		BuildInfo: buildBuildInfo(r.Context(), cfg),
		Page:      notFoundView{Path: r.URL.Path},
	}
	tmpl, ok := ts.pageTemplates["not-found"]
	if !ok {
		http.NotFound(w, r)
		return
	}
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "layout", data); err != nil {
		cfg.Logger.Error("console: render 404", "err", err)
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("X-Robots-Tag", "noindex")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusNotFound)
	_, _ = w.Write(buf.Bytes())
}

// notFoundView powers the not-found page template.
type notFoundView struct {
	Path string
}

// templateSet bundles the templates the handlers need. base owns
// layout + every shared fragment (no `content` definition); each
// pageTemplates[name] is a clone of base with one page's `content`
// added. The fragment endpoints reuse base so they can call any
// fragment by name from any page.
type templateSet struct {
	base          *template.Template
	pageTemplates map[string]*template.Template
}

// pageContentFiles maps the section key the handler uses to the
// HTML file that owns `{{define "content"}}` for that section.
// Adding a new page is: add a file, add an entry here.
var pageContentFiles = map[string]string{
	"dashboard":         "templates/dashboard.html",
	"workflows-list":    "templates/workflows_list.html",
	"workflow-detail":   "templates/workflow_detail.html",
	"runs-list":         "templates/runs_list.html",
	"run-detail":        "templates/run_detail.html",
	"triggers-list":     "templates/triggers_list.html",
	"trigger-detail":    "templates/trigger_detail.html",
	"dlq-list":          "templates/dlq_list.html",
	"dlq-detail":        "templates/dlq_detail.html",
	"audit-log":         "templates/audit_log.html",
	"ops-index":         "templates/ops_index.html",
	"workers-list":      "templates/workers_list.html",
	"ops-leases":        "templates/ops_leases.html",
	"kv-list":           "templates/kv_list.html",
	"streams-list":      "templates/streams_list.html",
	"logs":              "templates/logs.html",
	"metrics_dashboard": "templates/metrics_dashboard.html",
	"configuration":     "templates/configuration.html",
	"task-types-list":   "templates/task_types_list.html",
	"not-found":         "templates/not_found.html",
}

// loadTemplates builds the base tree (layout + fragments) and the
// per-page trees. Per-page trees are clones of base with the one
// page's content overlay parsed in — that's the trick that lets
// each page own its own `content` template without colliding.
func loadTemplates() (*templateSet, error) {
	base := template.New("console").Funcs(funcMap())
	base, err := base.ParseFS(templatesFS,
		"templates/layout.html",
		"templates/disabled.html",
		"templates/fragments/*.html",
		"templates/components/*.html",
	)
	if err != nil {
		return nil, fmt.Errorf("parse base templates: %w", err)
	}
	if base.Lookup("layout") == nil {
		return nil, fmt.Errorf("template tree missing `layout` template")
	}
	pages := make(map[string]*template.Template, len(pageContentFiles))
	for section, file := range pageContentFiles {
		clone, err := base.Clone()
		if err != nil {
			return nil, fmt.Errorf("clone base for %s: %w", section, err)
		}
		clone, err = clone.ParseFS(templatesFS, file)
		if err != nil {
			return nil, fmt.Errorf("parse page %s: %w", section, err)
		}
		if clone.Lookup("content") == nil {
			return nil, fmt.Errorf("page %s missing `content` template", section)
		}
		pages[section] = clone
	}
	return &templateSet{base: base, pageTemplates: pages}, nil
}

// funcMap exposes helpers used inside templates. statusIcon mirrors
// the Go-side helper so badges share the same vocabulary. pagerArgs
// packs the pager template's positional inputs into a struct so the
// template stays readable. triggerKindGlyph supplies the per-kind
// header icon for triggers list / detail.
func funcMap() template.FuncMap {
	return template.FuncMap{
		"join":             strings.Join,
		"statusIcon":       statusIcon,
		"outcomeIcon":      outcomeIcon,
		"pagerArgs":        pagerArgs,
		"triggerKindGlyph": triggerKindGlyph,
		"jsonArray":        jsonArrayHelper,
		"dict":             dictHelper,
		"tooltip":          tooltipHelper(),
		"tooltipAs":        tooltipAsHelper(),
		"tooltipText":      tooltipTextHelper,
	}
}

// tooltipAsHelper renders the tooltip with a custom visible label
// (e.g. "Leases" as the label, "lease" as the glossary key). Falls
// back to the bare label when the term is unknown so missing entries
// degrade gracefully.
func tooltipAsHelper() func(label, term string) template.HTML {
	const tmpl = `<span class="glo-tooltip-wrapper" tabindex="0">` +
		`<span class="glo-tooltip-target">%s</span>` +
		`<span class="glo-tooltip-popover" role="tooltip">%s</span>` +
		`</span>`
	return func(label, term string) template.HTML {
		if label == "" {
			return template.HTML("")
		}
		text, ok := GlossaryTooltip(term)
		if !ok {
			return template.HTML(template.HTMLEscapeString(label))
		}
		return template.HTML(fmt.Sprintf(tmpl,
			template.HTMLEscapeString(label),
			template.HTMLEscapeString(text),
		))
	}
}

// tooltipTextHelper returns the raw glossary definition for term so a
// caller (e.g. the layout nav, where we want the tooltip on the link
// element itself rather than a nested wrapper) can splice the text
// into a popover span without rendering the full tooltipHelper
// wrapper. Falls back to empty so missing terms degrade silently
// rather than leaking "<no value>" into the DOM.
func tooltipTextHelper(term string) string {
	if term == "" {
		return ""
	}
	text, ok := GlossaryTooltip(term)
	if !ok {
		return ""
	}
	return text
}

// tooltipHelper returns a template helper that wraps a glossary term
// in the glo-tooltip-wrapper markup. Terms not in the glossary fall
// back to the bare HTML-escaped label so accidental misuse degrades
// gracefully to plain text rather than emitting an empty popover.
//
// The `glo-*` class prefix is deliberate: T11's command palette
// collided with Basecoat's `.command-dialog { opacity: 0 }` rule and
// had to be renamed mid-task. A custom prefix on glossary tooltips
// avoids the same problem if Basecoat ships its own `.tooltip-*`
// classes later.
func tooltipHelper() func(term string) template.HTML {
	const tmpl = `<span class="glo-tooltip-wrapper" tabindex="0">` +
		`<span class="glo-tooltip-target">%s</span>` +
		`<span class="glo-tooltip-popover" role="tooltip">%s</span>` +
		`</span>`
	return func(term string) template.HTML {
		if term == "" {
			return template.HTML("")
		}
		text, ok := GlossaryTooltip(term)
		if !ok {
			return template.HTML(template.HTMLEscapeString(term))
		}
		return template.HTML(fmt.Sprintf(tmpl,
			template.HTMLEscapeString(term),
			template.HTMLEscapeString(text),
		))
	}
}

// dictHelper builds a map[string]any from alternating key/value
// pairs so templates can pass struct-shaped data to nested partials
// without defining one ad-hoc Go type per call site. Used by the
// run-detail page to wrap StepRows into {Rows: ...} for the
// step-list partial. Panics on odd argc / non-string keys — those
// are programmer errors at template-author time.
func dictHelper(args ...any) map[string]any {
	if len(args)%2 != 0 {
		panic("dictHelper: odd number of arguments")
	}
	out := make(map[string]any, len(args)/2)
	const argMax = 64
	for i := 0; i < len(args) && i < argMax; i += 2 {
		key, ok := args[i].(string)
		if !ok {
			panic("dictHelper: non-string key")
		}
		out[key] = args[i+1]
	}
	return out
}

// jsonArrayHelper serialises a []float64 into a compact JSON array
// suitable for embedding in a data-* attribute. Returns an empty
// string when xs is nil so the template can branch on truthiness:
// {{if .Sparkline}}<canvas .../>{{end}}.
//
// Why not encoding/json directly: hand-rolled formatting avoids the
// reflection cost for the row-by-row render hot path and keeps the
// output dependency-free.
func jsonArrayHelper(xs []float64) template.JS {
	if len(xs) == 0 {
		return ""
	}
	var b strings.Builder
	b.Grow(len(xs) * 8)
	b.WriteByte('[')
	const maxLen = 1024 // bounded loop
	for i := 0; i < len(xs) && i < maxLen; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(xs[i], 'f', -1, 64))
	}
	b.WriteByte(']')
	return template.JS(b.String())
}

// triggerKindGlyph returns a one-character icon for each trigger kind.
// Matches the rendering in the CLI's `trigger list` so the same kind
// reads identically across surfaces. Unknown kinds get a neutral dot.
func triggerKindGlyph(kind string) string {
	switch kind {
	case "cron":
		return "⏱"
	case "webhook":
		return "↘"
	case "subject":
		return "📡"
	case "http":
		return "⤴"
	}
	return "•"
}

// pagerArgsValue is the literal type the pager template binds to.
type pagerArgsValue struct {
	Page     int
	HasPrev  bool
	HasNext  bool
	PrevPage int
	NextPage int
	URL      string
}

// pagerArgs is the template helper that produces pagerArgsValue.
func pagerArgs(
	page int, hasPrev, hasNext bool, prevPage, nextPage int, url string,
) pagerArgsValue {
	return pagerArgsValue{
		Page: page, HasPrev: hasPrev, HasNext: hasNext,
		PrevPage: prevPage, NextPage: nextPage, URL: url,
	}
}

// dashboardData is what the layout + dashboard.html templates expect.
// Phase 2 T06+T07+T08 replaced the legacy MetricsTiles/MetricsAvailable
// slice with a fully assembled DashboardView (six operational tiles +
// recent panels) under Page. The legacy keys remain on the struct so
// older tests asserting on the wire-up don't regress, but the rebuilt
// dashboard.html consults Page only.
type dashboardData struct {
	Title    string
	Section  string
	Actor    Actor
	Overview overviewData
	// BuildInfo drives the R9 build/identity footer; populated by
	// serveDashboard alongside Overview so the layout template
	// sees the same field path it does for every other page.
	BuildInfo        BuildInfo
	ReadOnly         bool
	MetricsTiles     []MetricsTile
	MetricsAvailable bool
	Page             DashboardView
}

type overviewData struct {
	Listener string
	AuthMode string
	Build    string
}

// serveDashboard renders /console/ — the empty-dashboard landing page.
// dispatchRoot guarantees the path is /console/ or /console before
// calling this function, so no path check is necessary here.
func serveDashboard(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveDashboard: w is nil")
	}
	if r == nil {
		panic("serveDashboard: r is nil")
	}
	actor, _ := ActorFrom(r.Context())
	view := buildDashboardView(r.Context(), cfg)
	view.Actor = actor
	data := dashboardData{
		Title:   "Dashboard",
		Section: "dashboard",
		Actor:   actor,
		Overview: overviewData{
			Listener: cfg.HTTPAddr,
			AuthMode: cfg.AuthMode.String(),
			Build:    cfg.Build,
		},
		BuildInfo:        buildBuildInfo(r.Context(), cfg),
		ReadOnly:         cfg.ReadOnly,
		MetricsAvailable: cfg.Metrics != nil,
		Page:             view,
	}
	if cfg.Metrics != nil {
		data.MetricsTiles = buildMetricsTiles(cfg.Metrics)
	}
	tmpl, ok := ts.pageTemplates["dashboard"]
	if !ok {
		panic("serveDashboard: dashboard template not loaded")
	}
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "layout", data); err != nil {
		cfg.Logger.Error("console: render dashboard", "err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(buf.Bytes())
}

// serveGzAsset returns a handler that streams a gzip-on-disk asset
// straight to the wire with `Content-Encoding: gzip`. The browser
// decompresses transparently. Mirrors the Scalar pattern.
func serveGzAsset(name, contentType string) http.HandlerFunc {
	if name == "" {
		panic("serveGzAsset: name is empty")
	}
	if contentType == "" {
		panic("serveGzAsset: contentType is empty")
	}
	body, err := fs.ReadFile(assetsFS, "assets/"+name)
	if err != nil {
		panic(fmt.Sprintf("serveGzAsset: read %s: %v", name, err))
	}
	if len(body) == 0 {
		panic(fmt.Sprintf("serveGzAsset: %s is empty", name))
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Content-Encoding", "gzip")
		w.Header().Set("Cache-Control", assetCacheHeader())
		_, _ = w.Write(body)
	}
}

// serveFontAsset streams one of the embedded woff2 font binaries. The
// file path under /console/assets/fonts/ maps directly to the file
// name on disk; missing files render 404. woff2 is precompressed by
// the format; we do NOT add Content-Encoding, the browser handles
// decompression natively. Cache-Control is long+immutable so reloads
// don't re-fetch.
func serveFontAsset() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		name := strings.TrimPrefix(r.URL.Path, "/console/assets/fonts/")
		if name == "" || strings.Contains(name, "/") {
			http.NotFound(w, r)
			return
		}
		body, err := fs.ReadFile(assetsFS, "assets/fonts/"+name)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "font/woff2")
		w.Header().Set("Cache-Control", assetCacheHeader())
		_, _ = w.Write(body)
	}
}

// servePlainAsset is the non-gzipped variant for `app.css`, which is
// small enough that the gzip overhead and the absence of a `.gz`
// embed would only complicate the build path.
func servePlainAsset(name, contentType string) http.HandlerFunc {
	return servePlainAssetAt(name, contentType)
}

// servePlainAssetAt mirrors servePlainAsset but takes a path under
// assets/ rather than a flat filename, so it can serve files nested
// in subdirectories (e.g. assets/sources/connection-state.js).
func servePlainAssetAt(path, contentType string) http.HandlerFunc {
	if path == "" {
		panic("servePlainAssetAt: path is empty")
	}
	if contentType == "" {
		panic("servePlainAssetAt: contentType is empty")
	}
	body, err := fs.ReadFile(assetsFS, "assets/"+path)
	if err != nil {
		panic(fmt.Sprintf("servePlainAssetAt: read %s: %v", path, err))
	}
	if len(body) == 0 {
		panic(fmt.Sprintf("servePlainAssetAt: %s is empty", path))
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", assetCacheHeader())
		_, _ = w.Write(body)
	}
}

// serveHeartbeat streams a Datastar PatchElements event at the
// configured interval, exposing the current server time. The handler
// returns when the client disconnects (r.Context().Done()) — that
// branch is the proof point for SSE cleanup tests. Wire format is
// handled by the official Datastar Go SDK; we own the cadence and
// the rendered fragment, the SDK owns the SSE framing.
func serveHeartbeat(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, interval time.Duration,
) {
	if w == nil {
		panic("serveHeartbeat: w is nil")
	}
	if r == nil {
		panic("serveHeartbeat: r is nil")
	}
	if interval <= 0 {
		panic("serveHeartbeat: interval must be positive")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	sse := datastar.NewSSE(w, r)

	if err := emitHeartbeat(sse, ts.base); err != nil {
		// First write failure — client likely already gone. Nothing to
		// do beyond returning; subsequent writes are skipped.
		return
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	const maxTicks = 1_000_000
	for tick := 0; tick < maxTicks; tick++ {
		select {
		case <-r.Context().Done():
			return
		case <-ticker.C:
			if err := emitHeartbeat(sse, ts.base); err != nil {
				return
			}
		}
	}
}

// emitHeartbeat renders the heartbeat fragment template and writes it
// as one Datastar PatchElements event. Returns the first error so the
// outer loop can exit cleanly on client disconnect. Multi-line HTML
// is safe — the SDK frames each line as its own data: record per the
// Datastar wire spec.
func emitHeartbeat(
	sse *datastar.ServerSentEventGenerator, tmpl *template.Template,
) error {
	if sse == nil {
		panic("emitHeartbeat: sse is nil")
	}
	if tmpl == nil {
		panic("emitHeartbeat: tmpl is nil")
	}
	var buf bytes.Buffer
	data := struct{ Now string }{
		Now: time.Now().UTC().Format(time.RFC3339),
	}
	if err := tmpl.ExecuteTemplate(&buf, "heartbeat", data); err != nil {
		return fmt.Errorf("render heartbeat: %w", err)
	}
	if err := sse.PatchElements(buf.String()); err != nil {
		return fmt.Errorf("patch elements: %w", err)
	}
	return nil
}
