// sparkline_fix_test.go pins the three sub-fixes (N1-A/B/C) that make the
// "24h trend" sparklines actually render.
//
// Background: the sparklines never painted because (B) uplot.min.js loaded
// `defer` inside the per-page <main> while console.js (which bundles
// sparkline.js) loaded `defer` in <head>; deferred scripts run in document
// order, so sparkline init ran before window.uPlot existed and bailed.
// (A) sparkline.js only scanned canvases once on DOMContentLoaded, so
// Datastar SSE element-patch swaps replaced the tbody with un-scanned
// canvases. (C) the live trigger-row push fragment hardcoded an empty
// sparkline <td> with no canvas.
//
// Methodology:
//   - Layout ordering (B): render a full page via mountWithFake + httptest
//     and assert the uplot <script> appears in <head> BEFORE the console.js
//     <script>. The canvas paint itself is browser-only and NOT unit-tested;
//     load order is the structural proxy for "uPlot is defined when
//     sparkline init runs."
//   - No per-page duplication (B): the three list pages must no longer carry
//     their own uplot <script> tag.
//   - Live trigger row (C): render the trigger-row fragment via
//     loadTemplates/renderFragment; populated .Row.Sparkline must emit the
//     same conditional <canvas> as triggers-tbody; nil must stay an empty
//     <td>.
//   - Bundle re-init hook (A): gunzip the embedded console.js.gz and assert
//     it carries initSparklines + the MutationObserver re-init hook. This
//     proves the SHIPPED bundle (not just the source) has the fix.
//
// Two assertions per test minimum (positive + negative space).
package console

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestSparkline_layoutLoadsUplotInHeadBeforeConsoleJS pins N1-B as
// dedup + ordering hygiene: uplot.min.js loads once from a single source
// (<head>) instead of being duplicated per page, and it sits ahead of
// console.js. NOTE: ordering is NOT the causal first-paint fix — both
// uplot and console.js are `defer`, so both run before DOMContentLoaded,
// and sparkline/metrics init gate on DCL and re-check window.uPlot at
// call-time, so uPlot was already defined when init ran even on the old
// layout. Keeping uplot ahead of console.js is future-proofing for any
// synchronous uPlot consumer in the bundle. The assertion stands: a
// single head-positioned uplot tag preceding console.js.
func TestSparkline_layoutLoadsUplotInHeadBeforeConsoleJS(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/workers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	headEnd := strings.Index(body, "</head>")
	if headEnd < 0 {
		t.Fatalf("no </head> in rendered page")
	}
	head := body[:headEnd]
	uplotIdx := strings.Index(head, `/console/assets/uplot.min.js`)
	consoleIdx := strings.Index(head, `/console/assets/console.js`)

	// Positive space: uplot script lives in <head>.
	if uplotIdx < 0 {
		t.Errorf("uplot.min.js must load in <head>; not found in head:\n%s", head)
	}
	// Positive space: console.js lives in <head>.
	if consoleIdx < 0 {
		t.Errorf("console.js must load in <head>; not found in head")
	}
	// Negative space: console.js must NOT precede uplot (the original bug).
	if uplotIdx >= 0 && consoleIdx >= 0 && uplotIdx > consoleIdx {
		t.Errorf("uplot.min.js (@%d) must precede console.js (@%d) in <head> "+
			"or sparkline init bails on undefined uPlot", uplotIdx, consoleIdx)
	}
}

// TestSparkline_listPagesDropPerPageUplotTag pins N1-B's de-duplication: the
// three list pages must no longer ship their own uplot <script>. Leaving a
// per-page tag would load uPlot twice and re-introduce the body-positioned
// race for any code that depends on first-load ordering.
func TestSparkline_listPagesDropPerPageUplotTag(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	for _, path := range []string{
		"/console/workflows",
		"/console/triggers",
		"/console/metrics",
	} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, rr.Code)
		}
		body := rr.Body.String()
		headEnd := strings.Index(body, "</head>")
		if headEnd < 0 {
			t.Fatalf("GET %s: no </head>", path)
		}
		// Positive space: uplot loaded exactly once (in <head>).
		if strings.Count(body, `/console/assets/uplot.min.js`) != 1 {
			t.Errorf("GET %s: uplot.min.js must appear exactly once (head only); "+
				"count=%d", path, strings.Count(body, `/console/assets/uplot.min.js`))
		}
		// Negative space: no uplot tag in <body> (the old per-page position).
		bodyPart := body[headEnd:]
		if strings.Contains(bodyPart, `/console/assets/uplot.min.js`) {
			t.Errorf("GET %s: stray per-page uplot.min.js still in <body>", path)
		}
	}
}

// TestSparkline_triggerRowEmitsCanvas pins N1-C: the live trigger-row push
// fragment must emit the SAME conditional <canvas> as triggers-tbody so the
// re-init scan (N1-A) picks it up. Populated .Row.Sparkline => canvas;
// nil => empty cell.
func TestSparkline_triggerRowEmitsCanvas(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}

	// Populated branch.
	withData := triggerRowPatch{
		Row:   TriggerRow{ID: "trg-9", Kind: "cron", Sparkline: []float64{1, 0, 2}},
		Fresh: true,
	}
	html, err := renderFragment(set.base, "trigger-row", withData)
	if err != nil {
		t.Fatalf("renderFragment trigger-row (data): %v", err)
	}
	// Positive space: the canvas renders with the same attributes as tbody.
	if !strings.Contains(html, `<canvas class="console-sparkline"`) {
		t.Errorf("populated trigger row must render canvas; got\n%s", html)
	}
	if !strings.Contains(html, `data-sparkline-id="trigger-trg-9"`) {
		t.Errorf("trigger-row canvas must carry per-row id; got\n%s", html)
	}
	if !strings.Contains(html, `data-sparkline-data="[1,0,2]"`) {
		t.Errorf("trigger-row canvas must carry jsonArray data; got\n%s", html)
	}

	// Empty branch.
	noData := triggerRowPatch{
		Row:   TriggerRow{ID: "trg-0", Kind: "cron"},
		Fresh: true,
	}
	emptyHTML, err := renderFragment(set.base, "trigger-row", noData)
	if err != nil {
		t.Fatalf("renderFragment trigger-row (nil): %v", err)
	}
	// Negative space: nil Sparkline keeps an empty cell, no canvas.
	if !strings.Contains(emptyHTML, `<td class="console-sparkline-col"></td>`) {
		t.Errorf("nil-Sparkline trigger row should render empty cell; got\n%s",
			emptyHTML)
	}
	if strings.Contains(emptyHTML, "<canvas") {
		t.Errorf("nil-Sparkline trigger row must not emit a canvas; got\n%s",
			emptyHTML)
	}
}

// TestSparkline_bundleEmbedsReInitHook pins N1-A in the SHIPPED artifact: the
// gunzipped console.js.gz must carry initSparklines and the MutationObserver
// re-init hook. Asserting on the rebuilt bundle (not the source) fails the
// build if a maintainer edits sparkline.js but forgets to re-run esbuild.
func TestSparkline_bundleEmbedsReInitHook(t *testing.T) {
	gz, err := assetsFS.ReadFile("assets/console.js.gz")
	if err != nil {
		t.Fatalf("read console.js.gz: %v", err)
	}
	zr, err := gzip.NewReader(bytes.NewReader(gz))
	if err != nil {
		t.Fatalf("gzip reader: %v", err)
	}
	raw, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("gunzip console.js: %v", err)
	}
	js := string(raw)

	// Positive space: the exported re-scan entry point survives minify.
	if !strings.Contains(js, "initSparklines") {
		t.Errorf("rebuilt console.js bundle must export initSparklines " +
			"(N1-A re-init missing)")
	}
	// Positive space: the re-init mechanism (MutationObserver) survives.
	if !strings.Contains(js, "MutationObserver") {
		t.Errorf("rebuilt console.js bundle must wire a MutationObserver " +
			"to re-scan after Datastar SSE swaps")
	}
	// Negative space: the canvas selector the scanner keys on must survive
	// so a stub cannot pass.
	if !strings.Contains(js, "data-sparkline-data") {
		t.Errorf("rebuilt console.js bundle must reference the sparkline " +
			"canvas selector")
	}
}
