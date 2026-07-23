package console

import (
	"bytes"
	"context"
	"fmt"
	"hash/fnv"
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

// assetVersion is a short content hash of the embedded asset bundle,
// computed once at startup. It is appended as ?v=<hash> to every asset
// URL in the layout (see the assetURL template func) so a new binary
// with changed CSS/JS serves NEW asset URLs, busting the browser's
// immutable cache automatically. Without it, /console/assets/app.css is
// a stable URL cached immutable for a year — so CSS/JS fixes never
// reached users until a manual hard reload.
var assetVersion = computeAssetVersion()

// computeAssetVersion folds every embedded asset (path + bytes, in the
// deterministic lexical order fs.WalkDir yields) into one FNV hash. FNV
// is a non-cryptographic content fingerprint — all we need for cache
// busting. Any asset change flips the hash; an unchanged binary always
// produces the same value.
func computeAssetVersion() string {
	h := fnv.New64a()
	err := fs.WalkDir(assetsFS, "assets",
		func(p string, d fs.DirEntry, walkErr error) error {
			if walkErr != nil || d.IsDir() {
				return walkErr
			}
			body, readErr := fs.ReadFile(assetsFS, p)
			if readErr != nil {
				return readErr
			}
			_, _ = h.Write([]byte(p))
			_, _ = h.Write(body)
			return nil
		})
	if err != nil {
		// A walk failure means the embed is broken — a build-time
		// programmer error, not operator input.
		panic(fmt.Sprintf("computeAssetVersion: walk assets: %v", err))
	}
	return fmt.Sprintf("%x", h.Sum64())
}

// assetURL appends the asset-bundle version as a cache-busting query so
// a content change yields a fresh URL. Template func: layout.html emits
// {{assetURL "/console/assets/app.css"}}.
func assetURL(path string) string {
	if path == "" {
		panic("assetURL: path must not be empty")
	}
	return path + "?v=" + assetVersion
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

	// fireLimit gates POST /console/triggers/{id}/fire (#352). Lazily
	// allocated on first use via fireLimiter() so tests opt in by
	// hitting the endpoint, and so the production wiring stays a
	// one-line assignment in server.go.
	fireLimit *fireRateLimiter

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
	// after the call. The cleanup func unsubscribes. The ring may
	// broadcast a sentinel slog.Record{} (Time.IsZero()==true) to
	// signal that Clear() was called — SSE handlers translate that
	// into a tbody reset for live operator browsers.
	Subscribe(ctx context.Context) (<-chan slog.Record, func())
	// Clear drops every retained record. Future records still flow
	// through; only the retained buffer is wiped. Operator-driven
	// via the Logs page Clear button.
	Clear()
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

// fireLimiter returns the active per-trigger fire-now rate limiter.
// Mount() seeds cfg.fireLimit before the routes register so the
// pointer travels through every value-passed Config copy. server.go
// (production) can override by assigning cfg.fireLimit before Mount.
func (c *Config) fireLimiter() *fireRateLimiter {
	if c == nil {
		panic("Config.fireLimiter: c is nil")
	}
	if c.fireLimit == nil {
		panic("Config.fireLimiter: limiter not initialised; " +
			"Mount() should have seeded it")
	}
	return c.fireLimit
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
	if cfg.fireLimit == nil {
		cfg.fireLimit = newFireRateLimiter(
			fireRateLimitDefault, fireRateWindowDefault,
		)
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

// consoleRoute is the minimal descriptor the registry needs: a ServeMux
// pattern and the handler installed for it. Nothing more — auth, CSRF,
// read-only, and method policy stay where they already live (middleware
// wrapping mux, or inside the handlers), so the descriptor does not grow
// speculative policy fields.
type consoleRoute struct {
	pattern string
	handler http.HandlerFunc
}

// consoleRouteCountMax bounds the route table so the registration loop is
// provably bounded (TigerStyle: every loop has a fixed upper bound) and an
// accidental explosion of routes trips an assertion instead of growing
// silently. The live table is 68 unconditional routes plus one gated
// fixture route.
const consoleRouteCountMax = 128

const (
	contentTypeJS  = "application/javascript; charset=utf-8"
	contentTypeCSS = "text/css; charset=utf-8"
)

// routes validates the full console route registry, then installs every
// entry on mux in one bounded pass. Validation runs to completion before
// the first mux.HandleFunc call, so an invalid registry cannot partially
// mutate mux — it fails loudly at boot instead.
func routes(mux *http.ServeMux, ts *templateSet, cfg Config) {
	if mux == nil {
		panic("routes: mux is nil")
	}
	if ts == nil {
		panic("routes: ts is nil")
	}
	if err := registerConsoleRoutes(mux, consoleRoutes(ts, cfg)); err != nil {
		panic("routes: invalid console route registry: " + err.Error())
	}
}

// registerConsoleRoutes validates table, then installs it on mux. It
// returns an error (rather than panicking) on an invalid table so callers
// and tests can assert the mux stays untouched when validation fails.
func registerConsoleRoutes(mux *http.ServeMux, table []consoleRoute) error {
	if mux == nil {
		panic("registerConsoleRoutes: mux is nil")
	}
	if table == nil {
		panic("registerConsoleRoutes: table is nil")
	}
	if err := validateConsoleRoutes(table); err != nil {
		return err
	}
	for _, rt := range table {
		mux.HandleFunc(rt.pattern, rt.handler)
	}
	return nil
}

// validateConsoleRoutes rejects an empty pattern, a nil handler, or a
// duplicate pattern before any route is installed. The temporary set is
// local to duplicate detection; the ordered slice stays the source of
// truth. The loop is bounded by consoleRouteCountMax.
func validateConsoleRoutes(table []consoleRoute) error {
	if table == nil {
		panic("validateConsoleRoutes: table is nil")
	}
	if len(table) > consoleRouteCountMax {
		panic("validateConsoleRoutes: route count exceeds consoleRouteCountMax")
	}
	seen := make(map[string]struct{}, len(table))
	for _, rt := range table {
		if rt.pattern == "" {
			return fmt.Errorf("console route has an empty pattern")
		}
		if rt.handler == nil {
			return fmt.Errorf("console route %q has a nil handler", rt.pattern)
		}
		if _, dup := seen[rt.pattern]; dup {
			return fmt.Errorf("console route %q is registered twice", rt.pattern)
		}
		seen[rt.pattern] = struct{}{}
	}
	return nil
}

// consoleRoutes builds the ordered route registry. The slice — assembled
// from category builders in a fixed order — is the single source of truth
// for the console's HTTP surface. The fixture subtree is appended only
// when DAGNATS_ENV permits it, so production never exposes it.
func consoleRoutes(ts *templateSet, cfg Config) []consoleRoute {
	if ts == nil {
		panic("consoleRoutes: ts is nil")
	}
	table := make([]consoleRoute, 0, consoleRouteCountMax)
	table = append(table, pageRoutes(ts, cfg)...)
	table = append(table, apiRoutes(ts, cfg)...)
	table = append(table, sseRoutes(ts, cfg)...)
	table = append(table, redirectRoutes()...)
	table = append(table, assetRoutes()...)
	if fixturesEnabled() {
		table = append(table, consoleRoute{
			pattern: "/console/__fixtures__/",
			handler: serveBasecoatFixture,
		})
	}
	return table
}

// withTemplateSet adapts a handler that needs the template set and config
// into an http.HandlerFunc, closing over ts and cfg once per route.
func withTemplateSet(
	fn func(http.ResponseWriter, *http.Request, *templateSet, Config),
	ts *templateSet, cfg Config,
) http.HandlerFunc {
	if fn == nil {
		panic("withTemplateSet: fn is nil")
	}
	if ts == nil {
		panic("withTemplateSet: ts is nil")
	}
	return func(w http.ResponseWriter, r *http.Request) {
		fn(w, r, ts, cfg)
	}
}

// withConfig adapts a handler that needs only config into an
// http.HandlerFunc, closing over cfg once per route.
func withConfig(
	fn func(http.ResponseWriter, *http.Request, Config),
	cfg Config,
) http.HandlerFunc {
	if fn == nil {
		panic("withConfig: fn is nil")
	}
	return func(w http.ResponseWriter, r *http.Request) {
		fn(w, r, cfg)
	}
}

// redirectTo returns a handler that permanently redirects to target. It
// captures the four constant-target /console/ops* hops; the two that
// preserve query parameters keep their dedicated handlers.
func redirectTo(target string) http.HandlerFunc {
	if target == "" {
		panic("redirectTo: empty target")
	}
	return func(w http.ResponseWriter, r *http.Request) {
		redirectMovedPermanently(w, r, target)
	}
}

// jsSourceRoute builds a route for a plain (uncompressed) JS asset served
// from the embedded sources/ dir. Every such asset maps its console URL
// basename to sources/<basename>, so the basename is the only variable.
func jsSourceRoute(basename string) consoleRoute {
	if basename == "" {
		panic("jsSourceRoute: empty basename")
	}
	return consoleRoute{
		pattern: "/console/assets/" + basename,
		handler: servePlainAssetAt("sources/"+basename, contentTypeJS),
	}
}

// dispatchTriggersRoot handles the exact /console/triggers path. Method
// dispatch lives here, not in the route table: POST creates a trigger,
// GET/HEAD render the list, anything else is 405 with an Allow header.
func dispatchTriggersRoot(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchTriggersRoot: w is nil")
	}
	if r == nil {
		panic("dispatchTriggersRoot: r is nil")
	}
	switch r.Method {
	case http.MethodPost:
		handleTriggerCreate(w, r, cfg)
	case http.MethodGet, http.MethodHead:
		servePageTriggersList(w, r, ts, cfg)
	default:
		w.Header().Set("Allow", "GET, POST")
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// pageRoutes returns the HTML page and dispatch-subtree routes, plus the
// two log export/clear actions that live under /console/logs.
func pageRoutes(ts *templateSet, cfg Config) []consoleRoute {
	if ts == nil {
		panic("pageRoutes: ts is nil")
	}
	return []consoleRoute{
		{"/console/", withTemplateSet(dispatchRoot, ts, cfg)},
		{"/console/workflows", withTemplateSet(servePageWorkflowsList, ts, cfg)},
		{"/console/workflows/", withTemplateSet(dispatchWorkflows, ts, cfg)},
		{"/console/runs", withTemplateSet(servePageRunsList, ts, cfg)},
		{"/console/runs/lookup", withTemplateSet(serveRunIDLookup, ts, cfg)},
		{"/console/runs/", withTemplateSet(dispatchRuns, ts, cfg)},
		{"/console/triggers", withTemplateSet(dispatchTriggersRoot, ts, cfg)},
		{"/console/triggers/", withTemplateSet(dispatchTriggers, ts, cfg)},
		{"/console/traces", withTemplateSet(servePageTracesList, ts, cfg)},
		{"/console/traces/", withTemplateSet(dispatchTraces, ts, cfg)},
		{"/console/dlq", withTemplateSet(servePageDLQList, ts, cfg)},
		{"/console/dlq/", withTemplateSet(dispatchDLQ, ts, cfg)},
		{"/console/workers", withTemplateSet(servePageWorkers, ts, cfg)},
		{"/console/workers/", withTemplateSet(dispatchWorkers, ts, cfg)},
		{"/console/services", withTemplateSet(servePageServices, ts, cfg)},
		{"/console/kv", withTemplateSet(servePageKVInspector, ts, cfg)},
		{"/console/streams", withTemplateSet(servePageStreams, ts, cfg)},
		{"/console/streams/", withTemplateSet(dispatchStreams, ts, cfg)},
		{"/console/consumers", withTemplateSet(servePageConsumers, ts, cfg)},
		{"/console/server", withTemplateSet(servePageServer, ts, cfg)},
		{"/console/connections", withTemplateSet(servePageConnections, ts, cfg)},
		{"/console/concurrency", withTemplateSet(servePageConcurrency, ts, cfg)},
		{"/console/agents", withTemplateSet(servePageAgentRuntimes, ts, cfg)},
		{"/console/logs", withTemplateSet(servePageLogs, ts, cfg)},
		{"/console/logs/export", withConfig(serveLogsExport, cfg)},
		{"/console/logs/clear", withConfig(serveLogsClear, cfg)},
		{"/console/config", withTemplateSet(servePageConfiguration, ts, cfg)},
		{"/console/task-types", withTemplateSet(servePageTaskTypes, ts, cfg)},
		{"/console/functions", withTemplateSet(servePageTaskTypes, ts, cfg)},
		{"/console/functions/", withTemplateSet(dispatchFunctions, ts, cfg)},
		{"/console/metrics", withTemplateSet(servePageMetrics, ts, cfg)},
		{"/console/audit", withTemplateSet(servePageAuditLog, ts, cfg)},
	}
}

// apiRoutes returns the /console/api/* fragment and data routes.
func apiRoutes(ts *templateSet, cfg Config) []consoleRoute {
	if ts == nil {
		panic("apiRoutes: ts is nil")
	}
	return []consoleRoute{
		{"/console/api/run/", withTemplateSet(serveRunTabFragment, ts, cfg)},
		{"/console/api/fragments/workflows-list",
			withTemplateSet(serveFragmentWorkflowsList, ts, cfg)},
		{"/console/api/fragments/runs-list",
			withTemplateSet(serveFragmentRunsList, ts, cfg)},
		{"/console/api/dlq/", withTemplateSet(dispatchDLQAPIFragment, ts, cfg)},
		{"/console/api/runs/", withTemplateSet(serveRunSheet, ts, cfg)},
		{"/console/api/search", withTemplateSet(serveSearch, ts, cfg)},
		{"/console/api/nav-counts", withConfig(serveNavCounts, cfg)},
		{"/console/api/metrics/chart/", withConfig(serveAPIMetricsChart, cfg)},
	}
}

// sseRoutes returns the /console/sse/* streaming routes. Heartbeat needs
// only the configured interval, so it keeps a focused closure rather than
// the shared config adapter.
func sseRoutes(ts *templateSet, cfg Config) []consoleRoute {
	if ts == nil {
		panic("sseRoutes: ts is nil")
	}
	return []consoleRoute{
		{"/console/sse/logs", withConfig(serveSSELogs, cfg)},
		{"/console/sse/metrics", withTemplateSet(serveSSEMetrics, ts, cfg)},
		{"/console/sse/heartbeat", func(w http.ResponseWriter, r *http.Request) {
			serveHeartbeat(w, r, ts, cfg.HeartbeatInterval)
		}},
		{"/console/sse/dashboard", withTemplateSet(serveSSEDashboard, ts, cfg)},
		{"/console/sse/runs", withTemplateSet(serveSSERuns, ts, cfg)},
		{"/console/sse/runs/", withTemplateSet(serveSSERunDetail, ts, cfg)},
		{"/console/sse/agents", withTemplateSet(serveSSEAgents, ts, cfg)},
		{"/console/sse/triggers", withTemplateSet(serveSSETriggers, ts, cfg)},
		{"/console/sse/dlq", withTemplateSet(serveSSEDLQ, ts, cfg)},
	}
}

// redirectRoutes returns the legacy /console/ops* permanent redirects.
// The four constant-target hops share redirectTo; the two that preserve
// query parameters keep their dedicated handlers.
func redirectRoutes() []consoleRoute {
	return []consoleRoute{
		{"/console/ops", redirectTo("/console/")},
		{"/console/ops/workers", serveOpsWorkersRedirect},
		{"/console/ops/leases", redirectTo("/console/concurrency")},
		{"/console/ops/kv", serveOpsKVRedirect},
		{"/console/ops/audit", redirectTo("/console/audit")},
		{"/console/ops/metrics", redirectTo("/console/metrics")},
	}
}

// assetRoutes returns the static asset routes. Content types and cache
// behavior are owned by the serve* asset helpers; this builder only maps
// URL to helper. jsSourceRoute captures the eight identical plain-JS
// entries that differ solely by basename.
func assetRoutes() []consoleRoute {
	return []consoleRoute{
		{"/console/assets/console.js", serveGzAsset("console.js.gz", contentTypeJS)},
		{"/console/assets/basecoat.css", serveGzAsset("basecoat.css.gz", contentTypeCSS)},
		{"/console/assets/uplot.min.js", serveGzAsset("uplot.min.js.gz", contentTypeJS)},
		{"/console/assets/app.css", servePlainAsset("app.css", contentTypeCSS)},
		jsSourceRoute("connection-state.js"),
		jsSourceRoute("toast.js"),
		jsSourceRoute("count-chip.js"),
		jsSourceRoute("metrics.js"),
		jsSourceRoute("build-info-copy.js"),
		jsSourceRoute("sidebar-collapse.js"),
		jsSourceRoute("nav-counts.js"),
		jsSourceRoute("logs.js"),
		{"/console/assets/fonts/", serveFontAsset()},
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

// redirectMovedPermanently 301-redirects to target, preserving any
// query string so deep links keep their parameters. Used for the
// retired /console/ops* paths after the Ops hub was dissolved.
func redirectMovedPermanently(
	w http.ResponseWriter, r *http.Request, target string,
) {
	if w == nil {
		panic("redirectMovedPermanently: w is nil")
	}
	if r == nil {
		panic("redirectMovedPermanently: r is nil")
	}
	if raw := r.URL.RawQuery; raw != "" {
		target += "?" + raw
	}
	http.Redirect(w, r, target, http.StatusMovedPermanently)
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
	"run-trace":         "templates/run_trace.html",
	"traces-list":       "templates/traces_list.html",
	"trace-detail":      "templates/trace_detail.html",
	"triggers-list":     "templates/triggers_list.html",
	"trigger-detail":    "templates/trigger_detail.html",
	"dlq-list":          "templates/dlq_list.html",
	"dlq-detail":        "templates/dlq_detail.html",
	"audit-log":         "templates/audit_log.html",
	"workers-list":      "templates/workers_list.html",
	"services-list":     "templates/services_list.html",
	"worker-detail":     "templates/worker_detail.html",
	"kv-list":           "templates/kv_list.html",
	"streams-list":      "templates/streams_list.html",
	"stream-detail":     "templates/stream_detail.html",
	"consumers-list":    "templates/consumers_list.html",
	"server":            "templates/server.html",
	"connections":       "templates/connections.html",
	"concurrency":       "templates/concurrency.html",
	"agent-runtimes":    "templates/agent_runtimes.html",
	"logs":              "templates/logs.html",
	"metrics_dashboard": "templates/metrics_dashboard.html",
	"configuration":     "templates/configuration.html",
	"task-types-list":   "templates/task_types_list.html",
	"function-detail":   "templates/function_detail.html",
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
// mulHelper multiplies two integers for template arithmetic. Used by
// the run-trace tab to convert a span's tree depth into a CSS indent
// (depth * rem) without baking the math into Go-side rendering.
func mulHelper(a, b int) int { return a * b }

func funcMap() template.FuncMap {
	return template.FuncMap{
		"join":             strings.Join,
		"statusIcon":       statusIcon,
		"outcomeIcon":      outcomeIcon,
		"pagerArgs":        pagerArgs,
		"triggerKindGlyph": triggerKindGlyph,
		"assetURL":         assetURL,
		"jsonArray":        jsonArrayHelper,
		"sparkExpr":        SparkExpr,
		"deltaTone":        deltaToneClass,
		"dict":             dictHelper,
		"mul":              mulHelper,
		"tooltip":          tooltipHelper(),
		"tooltipAs":        tooltipAsHelper(),
		"tooltipText":      tooltipTextHelper,
		"tooltipID":        tooltipPopoverID,
	}
}

// tooltipAsHelper renders the tooltip with a custom visible label
// (e.g. "Leases" as the label, "lease" as the glossary key). Falls
// back to the bare label when the term is unknown so missing entries
// degrade gracefully.
func tooltipAsHelper() func(label, term string) template.HTML {
	return func(label, term string) template.HTML {
		if label == "" {
			return template.HTML("")
		}
		text, ok := GlossaryTooltip(term)
		if !ok {
			return template.HTML(template.HTMLEscapeString(label))
		}
		return renderTooltip(label, term, text)
	}
}

// tooltipPopoverID derives a stable, unique DOM id for a tooltip's
// popover from the glossary term. Deterministic (no counter, no
// randomness) so repeat renders are byte-identical and snapshot-stable;
// distinct terms yield distinct ids so two tooltips never collide on
// aria-describedby. The 8-hex FNV suffix disambiguates terms whose
// slugs collide.
//
// Tradeoff: determinism-per-term means two instances of the SAME term on
// one page produce duplicate ids (not per-element-unique). Accepted —
// glossary terms rarely repeat on a page, and snapshot stability is worth
// more than guaranteeing element-level uniqueness here.
func tooltipPopoverID(term string) string {
	if term == "" {
		panic("tooltipPopoverID: empty term")
	}
	var slug strings.Builder
	for _, r := range strings.ToLower(term) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			slug.WriteRune(r)
		default:
			slug.WriteByte('-')
		}
	}
	hash := fnv.New32a()
	if _, err := hash.Write([]byte(term)); err != nil {
		panic(fmt.Sprintf("tooltipPopoverID: fnv write: %v", err))
	}
	return fmt.Sprintf("glo-tip-%s-%08x", slug.String(), hash.Sum32())
}

// renderTooltip emits the glossary tooltip wrapper with the WCAG 2.4.7 +
// 4.1.2 wiring: a stable popover id, the wrapper's aria-describedby
// pointing at it, and aria-label carrying the visible label. The CSS
// `.glo-tooltip-wrapper:focus-visible` ring (app.css) makes keyboard
// focus visible to satisfy 2.4.7.
func renderTooltip(label, term, text string) template.HTML {
	if label == "" {
		panic("renderTooltip: empty label")
	}
	if text == "" {
		panic("renderTooltip: empty text")
	}
	const tmpl = `<span class="glo-tooltip-wrapper" tabindex="0"` +
		` aria-label="%s" aria-describedby="%s">` +
		`<span class="glo-tooltip-target">%s</span>` +
		`<span class="glo-tooltip-popover" role="tooltip" id="%s">%s</span>` +
		`</span>`
	id := tooltipPopoverID(term)
	escLabel := template.HTMLEscapeString(label)
	return template.HTML(fmt.Sprintf(tmpl,
		escLabel,
		id,
		escLabel,
		id,
		template.HTMLEscapeString(text),
	))
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
	return func(term string) template.HTML {
		if term == "" {
			return template.HTML("")
		}
		text, ok := GlossaryTooltip(term)
		if !ok {
			return template.HTML(template.HTMLEscapeString(term))
		}
		return renderTooltip(term, term, text)
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

// triggerKindGlyph returns an inline lucide SVG icon for each trigger
// kind. Returns template.HTML so html/template emits the SVG unescaped;
// the SVG strokes with currentColor so the per-kind .trigger-icon color
// rules in app.css apply unchanged. Unknown kinds get a neutral dot.
// (Emoji glyphs were replaced with proper icons — the console renders
// icons everywhere, never emoji.)
func triggerKindGlyph(kind string) template.HTML {
	return template.HTML(triggerKindSVG(kind)) //nolint:gosec // fixed literals
}

// triggerKindSVG is the raw SVG markup for a kind — split out so the
// template.HTML wrapper stays a one-liner and tests can assert on the
// string. Icons: cron=clock, webhook=webhook, http=globe, subject=radio.
func triggerKindSVG(kind string) string {
	const open = `<svg class="trigger-svg-icon" width="15" height="15" ` +
		`viewBox="0 0 24 24" fill="none" stroke="currentColor" ` +
		`stroke-width="2" stroke-linecap="round" stroke-linejoin="round" ` +
		`aria-hidden="true">`
	const close = `</svg>`
	switch kind {
	case "cron":
		return open + `<circle cx="12" cy="12" r="10"/>` +
			`<polyline points="12 6 12 12 16 14"/>` + close
	case "webhook":
		return open +
			`<path d="M18 16.98h-5.99c-1.66 0-3.01-1.34-3.01-3s1.34-3 ` +
			`3.01-3H18"/><path d="m6 17 3.13-5.78c.53-.97.1-2.18-.5-3.1a4 ` +
			`4 0 1 1 6.89-4.06"/><path d="m12 6 3.13 5.73C15.66 12.7 16.9 ` +
			`13 18 13a4 4 0 1 1-3.24 6.35"/>` + close
	case "http":
		return open + `<circle cx="12" cy="12" r="10"/>` +
			`<path d="M12 2a14.5 14.5 0 0 0 0 20 14.5 14.5 0 0 0 0-20"/>` +
			`<path d="M2 12h20"/>` + close
	case "subject":
		return open + `<path d="M4.9 19.1C1 15.2 1 8.8 4.9 4.9"/>` +
			`<path d="M7.8 16.2c-2.3-2.3-2.3-6.1 0-8.5"/>` +
			`<circle cx="12" cy="12" r="2"/>` +
			`<path d="M16.2 7.8c2.3 2.3 2.3 6.1 0 8.5"/>` +
			`<path d="M19.1 4.9C23 8.8 23 15.1 19.1 19"/>` + close
	}
	return open +
		`<circle cx="12" cy="12" r="3" fill="currentColor" stroke="none"/>` +
		close
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
