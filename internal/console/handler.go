package console

import (
	"bytes"
	"fmt"
	"html/template"
	"io/fs"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/starfederation/datastar-go/datastar"
)

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

	mux := http.NewServeMux()
	routes(mux, ts, cfg)

	return authMiddleware(cfg.AuthMode, cfg.Password, mux)
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
		servePageWorkflowDetail(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/runs", func(w http.ResponseWriter, r *http.Request) {
		servePageRunsList(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/runs/", func(w http.ResponseWriter, r *http.Request) {
		servePageRunDetail(w, r, ts, cfg)
	})
	mux.HandleFunc("/console/api/fragments/workflows-list",
		func(w http.ResponseWriter, r *http.Request) {
			serveFragmentWorkflowsList(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/api/fragments/runs-list",
		func(w http.ResponseWriter, r *http.Request) {
			serveFragmentRunsList(w, r, ts, cfg)
		})
	mux.HandleFunc("/console/assets/console.js", serveGzAsset("console.js.gz",
		"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/assets/basecoat.css", serveGzAsset("basecoat.css.gz",
		"text/css; charset=utf-8"))
	mux.HandleFunc("/console/assets/uplot.min.js", serveGzAsset("uplot.min.js.gz",
		"application/javascript; charset=utf-8"))
	mux.HandleFunc("/console/assets/app.css", servePlainAsset("app.css",
		"text/css; charset=utf-8"))
	mux.HandleFunc("/console/sse/heartbeat", func(w http.ResponseWriter, r *http.Request) {
		serveHeartbeat(w, r, ts, cfg.HeartbeatInterval)
	})
}

// dispatchRoot picks between dashboard (/console/, /console) and 404.
// Previously serveDashboard owned both checks; splitting lets the
// dashboard handler stay focused on rendering, the dispatcher on
// routing. We can't bind /console/ exclusively because Go's mux
// makes that prefix-greedy — anything not matched elsewhere falls
// through here and we need to NotFound it.
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
		http.NotFound(w, r)
		return
	}
	serveDashboard(w, r, ts, cfg)
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
	"dashboard":       "templates/dashboard.html",
	"workflows-list":  "templates/workflows_list.html",
	"workflow-detail": "templates/workflow_detail.html",
	"runs-list":       "templates/runs_list.html",
	"run-detail":      "templates/run_detail.html",
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
// template stays readable.
func funcMap() template.FuncMap {
	return template.FuncMap{
		"join":       strings.Join,
		"statusIcon": statusIcon,
		"pagerArgs":  pagerArgs,
	}
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
// Keeping it small in PR 1 — later PRs add live tiles.
type dashboardData struct {
	Title    string
	Section  string
	Actor    Actor
	Overview overviewData
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
	data := dashboardData{
		Title:   "Dashboard",
		Section: "dashboard",
		Actor:   actor,
		Overview: overviewData{
			Listener: cfg.HTTPAddr,
			AuthMode: cfg.AuthMode.String(),
			Build:    cfg.Build,
		},
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
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		_, _ = w.Write(body)
	}
}

// servePlainAsset is the non-gzipped variant for `app.css`, which is
// small enough that the gzip overhead and the absence of a `.gz`
// embed would only complicate the build path.
func servePlainAsset(name, contentType string) http.HandlerFunc {
	if name == "" {
		panic("servePlainAsset: name is empty")
	}
	if contentType == "" {
		panic("servePlainAsset: contentType is empty")
	}
	body, err := fs.ReadFile(assetsFS, "assets/"+name)
	if err != nil {
		panic(fmt.Sprintf("servePlainAsset: read %s: %v", name, err))
	}
	if len(body) == 0 {
		panic(fmt.Sprintf("servePlainAsset: %s is empty", name))
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			w.Header().Set("Allow", "GET")
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", contentType)
		w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
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
