package console

import (
	"bytes"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/starfederation/datastar-go/datastar"
)

// pageData is the common payload for every full-page render. Section
// is the active nav tab. Title is the <title> string. Body is template
// `content`-named data ready to inject. ReadOnly mirrors Config.ReadOnly
// so the layout shows the read-only banner uniformly.
type pageData struct {
	Title    string
	Section  string
	Actor    Actor
	Overview overviewData
	// BuildInfo carries the build/identity footer payload (R9,
	// #320). renderPage populates it from cfg + a best-effort
	// ConfigSnapshot so every page surfaces a uniform footer
	// regardless of which handler renders the page.
	BuildInfo BuildInfo
	Page      any
	ReadOnly  bool
}

// renderPage executes the shared `layout` template with the given
// section-specific data. Caller pre-populates pd; we attach the actor
// and write the output. The templateKey selects which per-page
// template tree to execute against; tree keys come from the
// pageContentFiles map in handler.go.
func renderPage(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config, templateKey string, pd pageData,
) {
	if w == nil {
		panic("renderPage: w is nil")
	}
	if r == nil {
		panic("renderPage: r is nil")
	}
	if ts == nil {
		panic("renderPage: ts is nil")
	}
	if templateKey == "" {
		panic("renderPage: templateKey is empty")
	}
	tmpl, ok := ts.pageTemplates[templateKey]
	if !ok {
		panic("renderPage: unknown templateKey " + templateKey)
	}
	actor, _ := ActorFrom(r.Context())
	pd.Actor = actor
	pd.Overview = overviewData{
		Listener: cfg.HTTPAddr,
		AuthMode: cfg.AuthMode.String(),
		Build:    cfg.Build,
	}
	pd.BuildInfo = buildBuildInfo(r.Context(), cfg)
	pd.ReadOnly = cfg.ReadOnly
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, "layout", pd); err != nil {
		cfg.Logger.Error("console: render page", "section", pd.Section, "err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write(buf.Bytes())
}

// requireData reports an error to the client and returns false when
// no DataSource is configured. Pages depending on a data source must
// short-circuit through this; it keeps the 503 message uniform.
func requireData(
	w http.ResponseWriter, cfg Config, op string,
) (DataSource, bool) {
	if w == nil {
		panic("requireData: w is nil")
	}
	if op == "" {
		panic("requireData: op is empty")
	}
	if cfg.Data == nil {
		cfg.Logger.Warn("console: data source not configured", "op", op)
		http.Error(w,
			"data source not configured",
			http.StatusServiceUnavailable)
		return nil, false
	}
	return cfg.Data, true
}

// requirePort narrows the configured DataSource to the single domain
// port P a handler actually needs, reporting the same uniform 503 as
// requireData when no source is wired. Handlers depend on the narrow
// port, so their unit tests substitute a fake implementing just P (see
// unimplementedDataSource in ds_ports_test.go) rather than the whole
// surface. The production adapter implements every port, so the type
// assertion never fails in a wired deployment — a miss is a programmer
// error (a port method that isn't on the concrete adapter), hence panic.
func requirePort[P any](
	w http.ResponseWriter, cfg Config, op string,
) (P, bool) {
	if w == nil {
		panic("requirePort: w is nil")
	}
	if op == "" {
		panic("requirePort: op is empty")
	}
	var zero P
	if cfg.Data == nil {
		cfg.Logger.Warn("console: data source not configured", "op", op)
		http.Error(w,
			"data source not configured",
			http.StatusServiceUnavailable)
		return zero, false
	}
	port, ok := any(cfg.Data).(P)
	if !ok {
		panic("requirePort: data source does not implement the port for " + op)
	}
	return port, true
}

// sparklineHours is the canonical request window for list-row
// sparklines: 24 hourly buckets covering the trailing day. Lives here
// so the trigger path and the workflow path agree on the resolution.
const sparklineHours = 24

// runsMax is the safety bound on every fold over the ListRuns slice
// (the runs table header and the workflows-page run counts). Folds cap
// at this many runs and degrade to an undercount rather than panic, so
// a deployment with more total runs never 500s a list page.
const runsMax = 100_000

// prettyJSON returns indented JSON. Invalid JSON is rendered verbatim
// — operators need to see what was actually stored, not an error.
func prettyJSON(raw []byte) string {
	if len(raw) == 0 {
		return ""
	}
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		return string(raw)
	}
	return buf.String()
}

// firstQueryValue is a nil-safe wrapper around url.Values lookup.
func firstQueryValue(q map[string][]string, key string) string {
	if key == "" {
		panic("firstQueryValue: key is empty")
	}
	if q == nil {
		return ""
	}
	v, ok := q[key]
	if !ok || len(v) == 0 {
		return ""
	}
	return v[0]
}

// sheetView packages the fields the side-sheet shell template binds.
// BodyTemplate is a switch key the shell uses to pick which inner
// partial to render against Data; using a switch keeps html/template's
// type-safe contract intact (calling {{template .X .Y}} dynamically
// requires every name to be parse-time known).
type sheetView struct {
	Title        string
	BodyTemplate string
	Data         any
	FullPageHref string
}

// emitSheetFragment renders the side-sheet shell with the given view
// and emits the result as a Datastar PatchElements event targeting
// #sheet-outlet in inner mode. The label is included in slog warnings
// so a render or write failure is easy to attribute.
func emitSheetFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
	view sheetView, label string,
) {
	if ts == nil {
		panic("emitSheetFragment: ts is nil")
	}
	if label == "" {
		panic("emitSheetFragment: label is empty")
	}
	html, err := renderFragment(ts.base, "side-sheet", view)
	if err != nil {
		cfg.Logger.Error("console: render side-sheet",
			"label", label, "err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	sse := datastar.NewSSE(w, r)
	patchErr := sse.PatchElements(html,
		datastar.WithSelectorID("sheet-outlet"),
		datastar.WithModeInner())
	if patchErr != nil {
		cfg.Logger.Warn("console: sheet patch elements",
			"label", label, "err", patchErr)
	}
}

// sheetSeqFromPath extracts the <seq> token from a URL of the shape
// `<prefix><seq><suffix>`. Returns "" when the path doesn't match or
// the seq contains a slash. The pure-string version is enough — the
// caller validates the seq numerically when it looks up the entry.
func sheetSeqFromPath(path, prefix, suffix string) string {
	if path == "" || prefix == "" || suffix == "" {
		return ""
	}
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	if !strings.HasSuffix(rest, suffix) {
		return ""
	}
	seq := strings.TrimSuffix(rest, suffix)
	if seq == "" || strings.Contains(seq, "/") {
		return ""
	}
	return seq
}

// sheetSlugFromPath is sheetSeqFromPath with run-id-friendly
// validation. Same logic — kept distinct so the call sites read
// clearly at the route layer.
func sheetSlugFromPath(path, prefix, suffix string) string {
	return sheetSeqFromPath(path, prefix, suffix)
}
