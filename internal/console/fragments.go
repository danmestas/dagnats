package console

import (
	"bytes"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"

	"github.com/starfederation/datastar-go/datastar"
)

// Fragment endpoints reuse the base template tree (layout + every
// shared fragment). They render a single named fragment template
// — the tbody for a list page — back as one Datastar PatchElements
// SSE event. The base tree is enough because no fragment template
// needs the `content` overlay each full-page tree carries.

// fragmentEnvelope captures the Datastar-signalled state we read on
// fragment requests. Each filter input on a list page binds into a
// matching signal — workflowsFilter / runsWorkflow etc. — and the
// signal value is sent on every triggered request automatically.
type fragmentEnvelope struct {
	WorkflowsFilter string `json:"workflowsFilter,omitempty"`
	WorkflowsSort   string `json:"workflowsSort,omitempty"`
	RunsWorkflow    string `json:"runsWorkflow,omitempty"`
	RunsStatus      string `json:"runsStatus,omitempty"`
	RunsRange       string `json:"runsRange,omitempty"`
}

// serveFragmentWorkflowsList responds to /console/api/fragments/workflows-list
// with the updated tbody fragment. Datastar @get on the search input
// triggers this; the SDK frames the response into a SSE PatchElements
// event so the browser swaps in the new rows without a full reload.
func serveFragmentWorkflowsList(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveFragmentWorkflowsList: w is nil")
	}
	if r == nil {
		panic("serveFragmentWorkflowsList: r is nil")
	}
	ds, ok := requireData(w, cfg, "workflows-list-fragment")
	if !ok {
		return
	}
	q := mergeFragmentParams(r, "workflowsFilter", "workflowsSort")
	view, err := buildWorkflowsView(r.Context(), ds, q)
	if err != nil {
		cfg.Logger.Error("console: workflows fragment", "err", err)
		http.Error(w, "list workflows failed", http.StatusInternalServerError)
		return
	}
	view.ReadOnly = cfg.ReadOnly
	view.CSRFToken = csrfTokenFor(r)
	html, err := renderFragment(ts.base, "workflows-tbody", view)
	if err != nil {
		cfg.Logger.Error("console: render workflows fragment", "err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	patchHTML(w, r, cfg, html)
}

// serveFragmentRunsList is the runs-page analog. Filters fold into
// signals; the request body / query carries them.
func serveFragmentRunsList(
	w http.ResponseWriter, r *http.Request,
	ts *templateSet, cfg Config,
) {
	if w == nil {
		panic("serveFragmentRunsList: w is nil")
	}
	if r == nil {
		panic("serveFragmentRunsList: r is nil")
	}
	ds, ok := requireData(w, cfg, "runs-list-fragment")
	if !ok {
		return
	}
	q := mergeFragmentParams(r, "runsWorkflow", "runsStatus", "runsRange")
	view, err := buildRunsView(r.Context(), ds, q)
	if err != nil {
		cfg.Logger.Error("console: runs fragment", "err", err)
		http.Error(w, "list runs failed", http.StatusInternalServerError)
		return
	}
	html, err := renderFragment(ts.base, "runs-tbody", view)
	if err != nil {
		cfg.Logger.Error("console: render runs fragment", "err", err)
		http.Error(w, "render failed", http.StatusInternalServerError)
		return
	}
	patchHTML(w, r, cfg, html)
}

// mergeFragmentParams merges the request's URL query with the
// Datastar-signal envelope. URL params (page=N from pagination)
// take precedence over signals (filter input bindings) so the
// pagination buttons remain authoritative when both are set.
//
// Signal keys are the JSON tags on fragmentEnvelope; each key is
// rewritten to the matching query name buildWorkflowsView /
// buildRunsView expects. The signalKeys slice gates which envelope
// fields apply to this view — irrelevant signals stay absent so
// they don't accidentally filter the other page.
func mergeFragmentParams(
	r *http.Request, signalKeys ...string,
) map[string][]string {
	if r == nil {
		panic("mergeFragmentParams: r is nil")
	}
	if len(signalKeys) == 0 {
		panic("mergeFragmentParams: signalKeys is empty")
	}
	out := make(map[string][]string, len(r.URL.Query())+len(signalKeys))
	for k, v := range r.URL.Query() {
		out[k] = v
	}
	env, ok := readSignalsBestEffort(r)
	if !ok {
		return out
	}
	for _, key := range signalKeys {
		queryKey, value := envelopeToQuery(env, key)
		if value == "" || queryKey == "" {
			continue
		}
		if _, present := out[queryKey]; present {
			continue
		}
		out[queryKey] = []string{value}
	}
	return out
}

// envelopeToQuery maps an envelope JSON field name to its URL query
// name + value. Centralising the mapping here means the templates,
// the view builders, and the fragment endpoints all agree on the
// vocabulary without each maintaining its own copy.
func envelopeToQuery(env fragmentEnvelope, key string) (string, string) {
	switch key {
	case "workflowsFilter":
		return "filter", env.WorkflowsFilter
	case "workflowsSort":
		return "sort", env.WorkflowsSort
	case "runsWorkflow":
		return "workflow", env.RunsWorkflow
	case "runsStatus":
		return "status", env.RunsStatus
	case "runsRange":
		return "range", env.RunsRange
	}
	return "", ""
}

// readSignalsBestEffort tries the Datastar SDK helper first; on any
// error (no body, JSON malformed, parse failure) returns ok=false so
// callers fall back to URL params only. We refuse to bubble an
// internal-server-error to the operator on a missing/invalid body —
// the filter form simply renders unfiltered, which is the safe
// default.
func readSignalsBestEffort(r *http.Request) (fragmentEnvelope, bool) {
	if r == nil {
		panic("readSignalsBestEffort: r is nil")
	}
	var env fragmentEnvelope
	if err := datastar.ReadSignals(r, &env); err == nil {
		return env, true
	}
	// Fallback path: tests / older clients may POST the JSON body
	// directly without the datastar query-param wrapping. Tolerate it.
	if r.Body == nil {
		return env, false
	}
	const maxBody = 64 * 1024
	body, err := io.ReadAll(io.LimitReader(r.Body, maxBody))
	if err != nil || len(body) == 0 {
		return env, false
	}
	if jsonErr := json.Unmarshal(body, &env); jsonErr != nil {
		return env, false
	}
	return env, true
}

// renderFragment runs one named template against data and returns
// the rendered HTML. Caller streams it via the SSE PatchElements.
func renderFragment(
	tmpl *template.Template, name string, data any,
) (string, error) {
	if tmpl == nil {
		panic("renderFragment: tmpl is nil")
	}
	if name == "" {
		panic("renderFragment: name is empty")
	}
	if tmpl.Lookup(name) == nil {
		return "", fmt.Errorf("template %q not registered", name)
	}
	var buf bytes.Buffer
	if err := tmpl.ExecuteTemplate(&buf, name, data); err != nil {
		return "", fmt.Errorf("execute template %q: %w", name, err)
	}
	return buf.String(), nil
}

// patchHTML writes one Datastar PatchElements SSE event with html.
// The target element ID comes from the template — every fragment
// template renders its outer element with `id="..."` so the default
// outer-mode patch lines up.
func patchHTML(
	w http.ResponseWriter, r *http.Request, cfg Config, html string,
) {
	if w == nil {
		panic("patchHTML: w is nil")
	}
	if r == nil {
		panic("patchHTML: r is nil")
	}
	if html == "" {
		panic("patchHTML: html is empty")
	}
	sse := datastar.NewSSE(w, r)
	if err := sse.PatchElements(html); err != nil {
		cfg.Logger.Error("console: patch elements", "err", err)
		return
	}
}
