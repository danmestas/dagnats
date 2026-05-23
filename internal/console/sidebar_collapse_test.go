// sidebar_collapse_test.go exercises R6 (#341): the desktop rail
// collapse-to-icons affordance and the build-info footer slot inside
// the rail bottom at >=1024px viewports.
//
// Methodology:
//   - Pure handler tests against fakeDataSource; no NATS.
//   - DOM is server-rendered, viewport is selected client-side by
//     CSS. The server response carries both the rail-bottom footer
//     slot AND the below-main footer; CSS picks which one renders
//     per viewport. Both must appear in the markup; visibility is
//     verified by the agent-browser smoke at three viewport widths.
//   - Positive substrings: the toggle button, the rail-bottom footer
//     slot wrapper, and the sidebar-collapse JS reference.
//   - Negative substring: only one <footer class="build-info-footer">
//     would be wrong — we want the rail copy and the below-main copy
//     to both ship so the CSS-driven regime switch is purely visual.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestSidebarCollapse_ToggleButtonPresent asserts the layout chrome
// carries a collapse-toggle button with the right ARIA wiring. The
// JS hooks onto this button to flip aria-expanded on the rail
// container.
func TestSidebarCollapse_ToggleButtonPresent(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	// The rail container must carry aria-expanded so the JS toggle
	// has something to flip and CSS can key off the state.
	if !strings.Contains(body, `aria-expanded`) {
		t.Errorf("layout missing aria-expanded on rail container")
	}

	// Toggle button with a stable id the JS keys off.
	wants := []string{
		`id="sidebar-collapse-toggle"`,
		`aria-label="Collapse sidebar"`,
	}
	for _, w := range wants {
		if !strings.Contains(body, w) {
			t.Errorf("layout missing toggle markup %q", w)
		}
	}
}

// TestSidebarCollapse_RailFooterSlotRenders asserts the build-info
// footer is present inside the rail (between the brand/nav and the
// page edge) AND below </main>. CSS handles the regime switch:
// rail-bottom copy is visible at >=1024px and the below-main copy is
// visible at <1024px. Both ship in the markup.
func TestSidebarCollapse_RailFooterSlotRenders(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap = ConfigSnapshot{
		NATSURL:      "nats://127.0.0.1:4222",
		NATSEmbedded: true,
		Streams: []StreamSnapshot{
			{Name: "WORKFLOW_HISTORY", Provisioned: true},
		},
	}
	h := mountWithFake(t, fake)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/", nil))
	body := rr.Body.String()

	// Find the rail container (console-header).
	railStart := strings.Index(body, `class="console-header"`)
	if railStart < 0 {
		t.Fatalf("console-header rail not found in body")
	}
	railEnd := strings.Index(body[railStart:], "</header>")
	if railEnd < 0 {
		t.Fatalf("console-header end tag missing")
	}
	railBlock := body[railStart : railStart+railEnd]

	// The rail must carry a build-info-footer copy nested inside it.
	if !strings.Contains(railBlock, `class="build-info-footer`) {
		t.Errorf("rail missing build-info-footer slot; rail was: %s",
			truncBody(railBlock))
	}

	// And the below-main copy must still ship for the <1024px regime.
	mainEnd := strings.Index(body, `</main>`)
	if mainEnd < 0 {
		t.Fatalf("</main> tag missing in body")
	}
	belowMain := body[mainEnd:]
	if !strings.Contains(belowMain, `class="build-info-footer`) {
		t.Errorf("below-main build-info-footer copy missing; tail: %s",
			truncBody(belowMain))
	}
}

// TestSidebarCollapse_ScriptIncluded asserts the layout pulls in the
// sidebar-collapse.js asset. The JS is the only place that writes
// aria-expanded + localStorage; without the include the collapse
// toggle would be a no-op.
func TestSidebarCollapse_ScriptIncluded(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/", nil))
	body := rr.Body.String()

	want := `/console/assets/sidebar-collapse.js`
	if !strings.Contains(body, want) {
		t.Errorf("layout missing %q script include", want)
	}
}

// TestSidebarCollapse_AssetServes asserts the /console/assets URL
// for the new JS file resolves and serves JavaScript content.
func TestSidebarCollapse_AssetServes(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/assets/sidebar-collapse.js", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	ct := rr.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "application/javascript") {
		t.Errorf("content-type = %q, want application/javascript*", ct)
	}
	body := rr.Body.String()
	// Sanity check: the JS owns aria-expanded + localStorage.
	for _, w := range []string{"aria-expanded", "localStorage"} {
		if !strings.Contains(body, w) {
			t.Errorf("sidebar-collapse.js missing %q", w)
		}
	}
}
