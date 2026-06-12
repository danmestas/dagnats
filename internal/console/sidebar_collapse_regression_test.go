// sidebar_collapse_regression_test.go pins the fix for the PR #402
// sidebar-collapse regression (Batch 6, ITEM 0).
//
// Symptom diagnosed against the running app: at the collapsed 56px rail
// width the brand row overflowed (scrollWidth 88 vs clientWidth 55) — the
// "://" glyph reveal rule used `display: inline`, and the brand-row kept
// its `justify-content: space-between` row layout, so the glyph + the 24px
// collapse toggle could not both fit inside 56px and the toggle pushed off
// the rail's right edge. The collapse JS (sidebar-collapse.js) was correct
// and is unchanged — the bug is pure CSS.
//
// Methodology:
//   - Serve app.css through the real asset handler (Mount → GET
//     /console/assets/app.css) so the test exercises what the browser
//     actually receives, not a file read.
//   - Positive: the collapsed-state glyph reveal uses a centered
//     inline-flex (not the buggy bare `inline`) and the collapsed
//     brand-row stacks to a column so glyph + toggle fit the 56px rail.
//   - Negative space: the served CSS no longer contains the buggy
//     `.console-glyph { display: inline }` reveal rule.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fetchAppCSS returns the body the asset handler serves for app.css.
func fetchAppCSS(t *testing.T) string {
	t.Helper()
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/assets/app.css", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET app.css: status = %d, want 200", rr.Code)
	}
	return rr.Body.String()
}

// cssRuleBody returns the declaration block (between the first { and its
// matching }) for the first occurrence of selector in css. Fails the test
// when the selector is absent so a renamed rule surfaces loudly.
func cssRuleBody(t *testing.T, css, selector string) string {
	t.Helper()
	idx := strings.Index(css, selector)
	if idx < 0 {
		t.Fatalf("css missing selector %q", selector)
	}
	open := strings.IndexByte(css[idx:], '{')
	if open < 0 {
		t.Fatalf("selector %q has no opening brace", selector)
	}
	close := strings.IndexByte(css[idx+open:], '}')
	if close < 0 {
		t.Fatalf("selector %q has no closing brace", selector)
	}
	return css[idx+open+1 : idx+open+close]
}

// TestSidebarCollapse_glyphRevealCentered asserts the collapsed-rail glyph
// reveal centers the "://" mark with a flex display rather than the buggy
// bare `inline` that left the brand row overflowing the 56px rail.
func TestSidebarCollapse_glyphRevealCentered(t *testing.T) {
	css := fetchAppCSS(t)
	const selector = `.console-header[aria-expanded="false"] .console-glyph`
	body := cssRuleBody(t, css, selector)

	// Positive: the reveal uses an inline-flex/flex display so the glyph
	// is a centerable flex item inside the collapsed brand chrome.
	if !strings.Contains(body, "inline-flex") &&
		!strings.Contains(body, "display: flex") {
		t.Errorf("collapsed glyph reveal not flex-centered; rule body: %q",
			body)
	}

	// Negative space: the buggy bare `display: inline` (followed by a
	// terminator, not `inline-flex`) must be gone.
	if strings.Contains(body, "display: inline;") ||
		strings.Contains(body, "display:inline;") {
		t.Errorf("collapsed glyph reveal still uses bare display:inline; "+
			"rule body: %q", body)
	}
}

// TestSidebarCollapse_brandRowStacksWhenCollapsed asserts the collapsed
// rail stacks the brand row to a column so the glyph and the collapse
// toggle both fit inside the 56px rail instead of overflowing.
func TestSidebarCollapse_brandRowStacksWhenCollapsed(t *testing.T) {
	css := fetchAppCSS(t)
	const selector = `.console-header[aria-expanded="false"] .console-brand-row`
	body := cssRuleBody(t, css, selector)

	if !strings.Contains(body, "column") {
		t.Errorf("collapsed brand-row not stacked to column; rule body: %q",
			body)
	}
}
