package console

import (
	"net/http"
	"strings"
)

// dlqModalView powers the shared dlq-action-modal partial when it's
// served as a stand-alone fragment from /console/api/dlq/<seq>/confirm.
// On the list + detail full-page renders the modal pulls its data from
// the per-row hidden <form> elements; the fragment endpoint exists for
// callers that want the modal markup ad-hoc (e.g. Datastar @get from
// the list's row button if the operator wants the server to be the
// authority on the typed-confirm word + reason text instead of the
// client-rendered fallback).
type dlqModalView struct {
	Sequence   uint64
	Action     string
	ReasonFull string
	Workflow   string
	CSRFToken  string
	ReadOnly   bool
}

// serveDLQConfirmFragment renders the typed-confirm modal as a stand-
// alone HTML fragment for the given DLQ sequence + action. The
// response body is the modal markup + its inline JS; callers either
// inject it into a body-level container or read it for parity checks
// against the full-page render.
//
// The endpoint accepts GET to keep it cacheable per-request and to
// match the @get('...') idiom Datastar uses for hypermedia fetches;
// it does not mutate state.
func serveDLQConfirmFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveDLQConfirmFragment: w is nil")
	}
	if r == nil {
		panic("serveDLQConfirmFragment: r is nil")
	}
	if ts == nil {
		panic("serveDLQConfirmFragment: ts is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	seqStr := dlqConfirmSeqFromPath(r.URL.Path)
	if seqStr == "" {
		http.NotFound(w, r)
		return
	}
	action := strings.ToLower(r.URL.Query().Get("action"))
	if action != "retry" && action != "discard" {
		http.Error(w, "unknown action", http.StatusBadRequest)
		return
	}
	ds, ok := requireData(w, cfg, "dlq-confirm-fragment")
	if !ok {
		return
	}
	view, ok := buildDLQModalView(r, ds, cfg, seqStr, action)
	if !ok {
		http.NotFound(w, r)
		return
	}
	html, err := renderFragment(ts.base, "dlq-action-modal-fragment", view)
	if err != nil {
		cfg.Logger.Error("console: render dlq-action-modal-fragment",
			"err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	if _, err := w.Write([]byte(html)); err != nil {
		cfg.Logger.Warn("console: write dlq-confirm-fragment",
			"err", err)
	}
}

// dlqConfirmSeqFromPath extracts the <seq> token from a URL of the
// shape /console/api/dlq/<seq>/confirm. Returns the empty string for
// any malformed path so the caller can 404.
func dlqConfirmSeqFromPath(path string) string {
	if path == "" {
		return ""
	}
	const prefix = "/console/api/dlq/"
	if !strings.HasPrefix(path, prefix) {
		return ""
	}
	rest := strings.TrimPrefix(path, prefix)
	const suffix = "/confirm"
	if !strings.HasSuffix(rest, suffix) {
		return ""
	}
	seq := strings.TrimSuffix(rest, suffix)
	if seq == "" || strings.Contains(seq, "/") {
		return ""
	}
	return seq
}

// buildDLQModalView resolves seqStr against the data source and
// returns the modal-binding view. ok=false on missing/garbage seq so
// the caller can 404 instead of rendering an empty modal.
func buildDLQModalView(
	r *http.Request, ds DataSource, cfg Config,
	seqStr, action string,
) (dlqModalView, bool) {
	if r == nil {
		panic("buildDLQModalView: r is nil")
	}
	if ds == nil {
		panic("buildDLQModalView: ds is nil")
	}
	detail := buildDLQDetail(r.Context(), ds, seqStr)
	if detail.NotFound {
		return dlqModalView{}, false
	}
	return dlqModalView{
		Sequence:   detail.Sequence,
		Action:     action,
		ReasonFull: detail.ReasonFull,
		Workflow:   detail.Workflow,
		CSRFToken:  csrfTokenFor(r),
		ReadOnly:   cfg.ReadOnly,
	}, true
}

// dispatchDLQAPIFragment routes /console/api/dlq/<seq>/{confirm,sheet}
// to the matching handler. Both endpoints share the prefix because
// the mux is registered on /console/api/dlq/ as a catch-all; the
// suffix selects the response shape. Unknown suffixes 404.
func dispatchDLQAPIFragment(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("dispatchDLQAPIFragment: w is nil")
	}
	if r == nil {
		panic("dispatchDLQAPIFragment: r is nil")
	}
	switch {
	case strings.HasSuffix(r.URL.Path, "/confirm"):
		serveDLQConfirmFragment(w, r, ts, cfg)
	case strings.HasSuffix(r.URL.Path, "/sheet"):
		serveDLQSheet(w, r, ts, cfg)
	default:
		http.NotFound(w, r)
	}
}

// serveDLQSheet renders /console/api/dlq/<seq>/sheet as a Datastar
// PatchElements SSE event that patches the side-sheet markup into
// #sheet-outlet (inner mode). The shell binds the dlq-sheet-body
// partial against the DLQDetailView.
func serveDLQSheet(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveDLQSheet: w is nil")
	}
	if r == nil {
		panic("serveDLQSheet: r is nil")
	}
	if ts == nil {
		panic("serveDLQSheet: ts is nil")
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	seqStr := sheetSeqFromPath(r.URL.Path, "/console/api/dlq/", "/sheet")
	if seqStr == "" {
		http.NotFound(w, r)
		return
	}
	ds, ok := requireData(w, cfg, "dlq-sheet")
	if !ok {
		return
	}
	detail := buildDLQDetail(r.Context(), ds, seqStr)
	if detail.NotFound {
		http.NotFound(w, r)
		return
	}
	view := sheetView{
		Title:        "DLQ entry #" + seqStr,
		BodyTemplate: "dlq-sheet-body",
		Data:         detail,
		FullPageHref: "/console/dlq/" + seqStr,
	}
	emitSheetFragment(w, r, ts, cfg, view, "dlq-sheet")
}
