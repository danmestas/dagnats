package console

import (
	"net/http"

	"github.com/starfederation/datastar-go/datastar"
)

// searchLimitDefault bounds the palette result list. 10 is enough to
// scroll through visually without paginating; the search is fast even
// against thousands of workflows because we cap before the slice
// crosses the wire.
const searchLimitDefault = 10

// serveSearch backs the cmd+k palette. GET /console/api/search?q=<term>
// returns the command-results partial wrapped in a Datastar
// PatchElements SSE event so the client patches the list without a
// full page reload. The endpoint accepts an empty query and renders
// the explicit "No results." copy — the operator must always see
// honest feedback, not stale rows from the previous keystroke.
func serveSearch(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveSearch: w is nil")
	}
	if r == nil {
		panic("serveSearch: r is nil")
	}
	if ts == nil {
		panic("serveSearch: ts is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	ds, ok := requireData(w, cfg, "search")
	if !ok {
		return
	}
	hits, err := ds.Search(r.Context(), r.URL.Query().Get("q"), searchLimitDefault)
	if err != nil {
		cfg.Logger.Error("console: search", "err", err)
		http.Error(w, "search failed", http.StatusInternalServerError)
		return
	}
	html, err := renderFragment(ts.base, "command-results", hits)
	if err != nil {
		cfg.Logger.Error("console: render command-results", "err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	sse := datastar.NewSSE(w, r)
	patchErr := sse.PatchElements(html,
		datastar.WithSelectorID("command-results"),
		datastar.WithModeInner())
	if patchErr != nil {
		cfg.Logger.Warn("console: search patch elements", "err", patchErr)
	}
}
