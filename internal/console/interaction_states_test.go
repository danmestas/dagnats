// interaction_states_test.go pins Batch-8 console polish in the served
// stylesheet: (1) the table header no longer paints a tinted bar — the
// base `.console-table thead th` rule is unconditionally transparent and
// the fragile theme-scoped + media-variant transparent overrides are
// gone; (2) interaction states are consistent — one shared
// :focus-visible ring rule exists, and pressed (:active) states exist
// for buttons (currently absent everywhere).
//
// Methodology:
//   - In-memory fakeDataSource feeds the layout via console.Mount.
//   - httptest.Recorder fetches /console/assets/app.css and asserts on
//     the served CSS text (intent, not brittle whitespace).
//   - Positive space: the unconditional transparent header rule and the
//     :active button rule are present. Negative space: the bright
//     --accent-soft thead background and the data-theme-scoped
//     transparent header override are gone.
//   - Own Mount; nothing shared.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// servedAppCSS renders /console/assets/app.css through a fresh Mount and
// returns the body, failing the test on a non-200.
func servedAppCSS(t *testing.T) string {
	t.Helper()
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/assets/app.css", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET app.css: status = %d, want 200", rr.Code)
	}
	return rr.Body.String()
}

// TestTableHeader_unconditionalTransparent asserts the table header
// background is transparent via a single unconditional base rule, and
// that the bright --accent-soft header fill plus the fragile
// theme-scoped transparent override are gone.
func TestTableHeader_unconditionalTransparent(t *testing.T) {
	css := servedAppCSS(t)

	// Positive space: the base thead rule sets a transparent background.
	idx := strings.Index(css, ".console-table thead th {")
	if idx < 0 {
		t.Fatalf("app.css missing .console-table thead th base rule")
	}
	// Scope to the rule body (selector-open up to its closing brace) so
	// the assertion is robust to a why-comment inside the block.
	end := strings.Index(css[idx:], "}")
	if end < 0 {
		t.Fatalf("base thead rule has no closing brace")
	}
	block := css[idx : idx+end]
	if !strings.Contains(block, "background: transparent;") {
		t.Errorf("base thead rule not transparent: %q", block)
	}

	// Negative space: the bright accent-soft header fill is retired.
	if strings.Contains(block, "background: var(--accent-soft)") {
		t.Errorf("thead still paints the bright --accent-soft bar: %q", block)
	}

	// Negative space: the fragile theme-scoped transparent override is
	// gone — the base rule now covers every theme/condition.
	if strings.Contains(css, `[data-theme="dark"] .console-table thead th`) {
		t.Errorf("fragile data-theme-scoped thead transparent override still present")
	}
}

// TestTableHeader_noHoverHighlight asserts the non-interactive header row
// does not light up on hover. Basecoat's unscoped `.table tr:hover` paints
// a --color-muted bar that shows through the transparent th cells; the
// console must neutralize the header-row hover.
func TestTableHeader_noHoverHighlight(t *testing.T) {
	css := servedAppCSS(t)
	idx := strings.Index(css, ".console-table thead tr:hover")
	if idx < 0 {
		t.Fatalf("app.css missing the thead tr:hover neutralizer (Basecoat .table tr:hover would brighten the header)")
	}
	end := strings.Index(css[idx:], "}")
	if end < 0 {
		t.Fatalf("thead tr:hover rule has no closing brace")
	}
	if !strings.Contains(css[idx:idx+end], "background: transparent") {
		t.Errorf("thead tr:hover must reset the background to transparent")
	}
}

// TestInteractionStates_focusAndActive asserts a consolidated
// :focus-visible ring rule exists and that buttons carry a pressed
// (:active) state — previously missing entirely.
func TestInteractionStates_focusAndActive(t *testing.T) {
	css := servedAppCSS(t)

	// Positive space: a shared focus-visible ring rule using the accent.
	if !strings.Contains(css, "button:focus-visible") ||
		!strings.Contains(css, "outline: 2px solid var(--accent)") {
		t.Errorf("consolidated :focus-visible accent ring rule missing")
	}

	// Positive space: a pressed state for buttons now exists.
	if !strings.Contains(css, ".btn:active") {
		t.Errorf("button :active pressed state missing")
	}

	// Negative space: the active state must do something — a bare empty
	// selector would pass the substring check above, so require a known
	// pressed treatment (translateY) accompanies it.
	if !strings.Contains(css, "transform: translateY(1px)") {
		t.Errorf("button :active state has no pressed affordance")
	}
}
