// Methodology: red-green TDD against the run-detail tab-group contract.
// The vendored Basecoat bundle paints the .tabs [role=tablist] container
// as a light segmented PILL (background-color:var(--color-muted),
// border-radius) and the active [role=tab][aria-selected=true] as a
// raised white inner pill (background-color:var(--color-background) +
// box-shadow). Those rules live inside @layer components in basecoat.css,
// so an UNLAYERED rule in app.css beats them regardless of specificity
// (cascade-layer precedence) — no !important needed. The MagicPath
// "Observe" mockup (ConsoleViewsObserve.tsx .dn-tabs/.dn-tab/.dn-active)
// renders plain underline tabs: transparent container with a full-width
// bottom rule, muted inactive text, and a teal (--accent) active label
// with a teal bottom-border underline, plus a visible focus ring.
//
// These tests read app.css (and the served stylesheet) and assert the
// pill is neutralized and the underline treatment is present. Positive
// space: the reset/underline rules exist. Negative space: the active tab
// does NOT repaint a surface/background pill and does NOT keep a shadow.
package console

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAppCSS_runDetailTabsAreUnderlineNotPill(t *testing.T) {
	body, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	css := string(body)

	// Container: the Basecoat pill rail must be neutralized — no muted
	// fill, no pill radius, no shadow; a flat full-width underline rail.
	container := cssBlock(t, css, ".run-detail-tabs-list")
	if !strings.Contains(container, "background: transparent") {
		t.Errorf(".run-detail-tabs-list must reset the pill bg to transparent")
	}
	if !strings.Contains(container, "border-radius: 0") {
		t.Errorf(".run-detail-tabs-list must reset the pill border-radius to 0")
	}
	if !strings.Contains(container, "box-shadow: none") {
		t.Errorf(".run-detail-tabs-list must drop the pill box-shadow")
	}
	if !strings.Contains(container, "border-bottom") {
		t.Errorf(".run-detail-tabs-list must carry the underline rail")
	}

	// Inactive tab: borderless transparent text button, muted text.
	tab := cssBlock(t, css, `.run-detail-tabs-list [role="tab"]`)
	if !strings.Contains(tab, "background: transparent") {
		t.Errorf(`[role="tab"] must reset the Basecoat tab bg to transparent`)
	}
	if !strings.Contains(tab, "var(--text-secondary)") {
		t.Errorf(`inactive [role="tab"] must be muted (--text-secondary)`)
	}
	if !strings.Contains(tab, "border-bottom: 2px solid transparent") {
		t.Errorf(`[role="tab"] must reserve a 2px transparent underline slot`)
	}

	// Active tab: teal text + teal underline, NO white inner pill, NO shadow.
	active := cssBlock(t, css, `.run-detail-tabs-list [role="tab"][aria-selected="true"]`)
	if !strings.Contains(active, "background: transparent") {
		t.Errorf("active tab must kill the white inner pill (background: transparent)")
	}
	if !strings.Contains(active, "box-shadow: none") {
		t.Errorf("active tab must kill the raised pill shadow (box-shadow: none)")
	}
	if !strings.Contains(active, "border-bottom-color: var(--accent)") {
		t.Errorf("active tab must paint a teal (--accent) underline")
	}
	if !strings.Contains(active, "color: var(--accent)") {
		t.Errorf("active tab label must be teal (--accent)")
	}
	// Negative space: the active tab must not repaint a surface/background
	// pill via the vendored tokens — otherwise the white pill returns.
	if strings.Contains(active, "var(--color-background)") ||
		strings.Contains(active, "var(--surface)") {
		t.Errorf("active tab must not repaint a surface/background pill")
	}

	// A hover and a visible focus ring complete the affordance.
	hover := cssBlock(t, css, `.run-detail-tabs-list [role="tab"]:hover`)
	if !strings.Contains(hover, "var(--text-primary)") {
		t.Errorf("tab hover must lift to --text-primary")
	}
	focus := cssBlock(t, css, `.run-detail-tabs-list [role="tab"]:focus-visible`)
	if !strings.Contains(focus, "outline") || !strings.Contains(focus, "var(--accent)") {
		t.Errorf("tab :focus-visible must draw a visible --accent outline ring")
	}
}

func TestServeAsset_appCSSCarriesUnderlineTabRule(t *testing.T) {
	h := newTestConsole(t)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/assets/app.css", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	// The served stylesheet must carry the underline override — proving the
	// console does not rely on the Basecoat pill at runtime.
	if !strings.Contains(body, ".run-detail-tabs-list") {
		t.Fatalf("served app.css missing the run-detail tab override")
	}
	if !strings.Contains(body, "border-radius: 0") {
		t.Fatalf("served app.css missing the pill border-radius reset")
	}
}
