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
// stays provider-agnostic per the project rules.
type Config struct {
	HTTPAddr string
	AuthMode AuthMode
	Password string
	Build    string
	Logger   *slog.Logger
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
	tmpl, err := loadTemplates()
	if err != nil {
		panic(fmt.Sprintf("console.Mount: load templates: %v", err))
	}
	if cfg.HeartbeatInterval <= 0 {
		cfg.HeartbeatInterval = defaultHeartbeatInterval
	}

	mux := http.NewServeMux()
	routes(mux, tmpl, cfg)

	return authMiddleware(cfg.AuthMode, cfg.Password, mux)
}

// routes wires every public path under /console/ into mux.
// Keeping this on a separate function (≤ 30 LOC) makes the route
// inventory easy to scan.
func routes(mux *http.ServeMux, tmpl *template.Template, cfg Config) {
	if mux == nil {
		panic("routes: mux is nil")
	}
	if tmpl == nil {
		panic("routes: tmpl is nil")
	}
	mux.HandleFunc("/console/", func(w http.ResponseWriter, r *http.Request) {
		serveDashboard(w, r, tmpl, cfg)
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
		serveHeartbeat(w, r, tmpl, cfg.HeartbeatInterval)
	})
}

// loadTemplates parses every HTML file under templates/ into a single
// template tree keyed by file name. The `layout` template includes
// `content`; per-page handlers ExecuteTemplate("layout", data).
func loadTemplates() (*template.Template, error) {
	root := template.New("console").Funcs(funcMap())
	tmpl, err := root.ParseFS(templatesFS, "templates/*.html",
		"templates/fragments/*.html")
	if err != nil {
		return nil, fmt.Errorf("parse templates: %w", err)
	}
	if tmpl.Lookup("layout") == nil {
		return nil, fmt.Errorf("template tree missing `layout` template")
	}
	return tmpl, nil
}

func funcMap() template.FuncMap {
	return template.FuncMap{
		// Reserved for later PRs; keeps the function map import path
		// stable so future templates can rely on a known signature.
		"join": strings.Join,
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
// Only the exact /console/ path is handled here; the openapi-style
// mount captures the trailing slash so the dashboard is the index.
func serveDashboard(
	w http.ResponseWriter, r *http.Request,
	tmpl *template.Template, cfg Config,
) {
	if w == nil {
		panic("serveDashboard: w is nil")
	}
	if r == nil {
		panic("serveDashboard: r is nil")
	}
	if r.URL.Path != "/console/" && r.URL.Path != "/console" {
		http.NotFound(w, r)
		return
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
	tmpl *template.Template, interval time.Duration,
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

	if err := emitHeartbeat(sse, tmpl); err != nil {
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
			if err := emitHeartbeat(sse, tmpl); err != nil {
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
