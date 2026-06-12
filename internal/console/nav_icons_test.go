// nav_icons_test.go pins Batch 7: the console nav renders real Lucide
// SVG icons (not the retired unicode glyphs), every nav link carries an
// aria-label for the collapsed icon-only rail, and the served app.css
// collapsed block keeps nav links VISIBLE as an icon rail (hiding the
// label span) instead of blanket-hiding the links.
//
// Methodology:
//   - In-memory fakeDataSource feeds the layout via console.Mount.
//   - httptest.Recorder renders /console/ and /console/assets/app.css.
//   - Positive space: real <svg class="console-nav-icon">, aria-labels,
//     label spans, the icon-rail collapse rules.
//   - Negative space: the retired unicode nav glyphs are gone, and the
//     collapsed block no longer blanket-hides .console-nav-desktop a.
//   - Own Mount; nothing shared.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestNavIcons_renderLucideSVG asserts /console/ renders Lucide SVG
// icons for nav items and no longer emits the retired unicode glyphs.
func TestNavIcons_renderLucideSVG(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /console/: status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	// Positive space: real inline SVG icons carrying the shared class.
	if !strings.Contains(body, `class="console-nav-icon"`) {
		t.Errorf("nav missing real Lucide icons (console-nav-icon svg)")
	}
	if c := strings.Count(body, `class="console-nav-icon"`); c < 18 {
		t.Errorf("nav-icon count = %d, want >= 18 (desktop nav)", c)
	}
	// Each desktop nav link wraps its label in a span so the collapsed
	// rail can hide the text and leave the icon.
	if !strings.Contains(body, `<span class="console-nav-label">Dashboard</span>`) {
		t.Errorf("nav missing wrapped label span for Dashboard")
	}

	// Negative space: the retired unicode glyphs are gone as nav glyphs.
	for _, glyph := range []string{
		`<span class="console-nav-glyph" aria-hidden="true">☠</span>`,
		`<span class="console-nav-glyph" aria-hidden="true">🔌</span>`,
		`<span class="console-nav-glyph" aria-hidden="true">▤</span>`,
	} {
		if strings.Contains(body, glyph) {
			t.Errorf("nav still renders retired unicode glyph: %q", glyph)
		}
	}
}

// TestNavIcons_linksAreLabelled asserts every nav link carries an
// aria-label and title so the collapsed icon-only rail stays accessible
// and shows a hover tooltip.
func TestNavIcons_linksAreLabelled(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET /console/: status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	for _, want := range []string{
		`aria-label="Dashboard"`,
		`aria-label="DLQ"`,
		`aria-label="Connections"`,
		`title="Dashboard"`,
		`title="DLQ"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("nav link missing accessibility attr %q", want)
		}
	}
}

// TestNavIcons_collapsedRailKeepsIcons asserts the served app.css
// collapsed block keeps nav links visible (icon rail) and hides the
// label span instead of blanket-hiding the link.
func TestNavIcons_collapsedRailKeepsIcons(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/assets/app.css", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET app.css: status = %d, want 200", rr.Code)
	}
	css := rr.Body.String()

	// Positive space: collapsed block hides the label, sizes the icon.
	if !strings.Contains(css,
		`.console-header[aria-expanded="false"] .console-nav-label`) {
		t.Errorf("collapsed block does not hide .console-nav-label")
	}
	if !strings.Contains(css, ".console-nav-icon {") {
		t.Errorf("app.css missing .console-nav-icon sizing rule")
	}

	// Negative space: the collapsed block must NOT blanket-hide nav
	// links — that was the pre-B7 regression that showed no nav.
	if strings.Contains(css,
		`.console-header[aria-expanded="false"] .console-nav-desktop a,`) {
		t.Errorf("collapsed block still blanket-hides nav links")
	}
}
