// build_info_footer_test.go exercises R9 (#320): the always-on
// build/identity footer strip rendered below <main> on every console
// page.
//
// Methodology:
//   - Pure handler tests against fakeDataSource; no NATS.
//   - Each subtest GETs a different console page and asserts the
//     footer markup is present, populated, and free of duplicated
//     status pill copy (the audit-locked anti-pattern).
//   - Positive substring: footer carries dagnats version, NATS URL,
//     embedded marker, and N/M stream count.
//   - Negative substring: footer must NOT contain "ONLINE" or
//     "OFFLINE" — that copy belongs to the header connection pill
//     and duplicating it across the surface is the audit miss
//     #320 was rewritten to prevent.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestBuildInfoFooter_RendersOnEveryPage asserts the footer appears
// in the body on representative pages (dashboard, workflows, config)
// and carries the documented content shape.
func TestBuildInfoFooter_RendersOnEveryPage(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap = ConfigSnapshot{
		NATSURL:      "nats://127.0.0.1:4222",
		NATSEmbedded: true,
		Streams: []StreamSnapshot{
			{Name: "WORKFLOW_HISTORY", Provisioned: true},
			{Name: "TASK_QUEUES", Provisioned: true},
			{Name: "TASK_RESULTS", Provisioned: true},
			{Name: "AUDIT", Provisioned: false},
		},
	}
	h := mountWithFake(t, fake)

	paths := []string{
		"/console/",
		"/console/workflows",
		"/console/config",
	}
	for _, p := range paths {
		t.Run(p, func(t *testing.T) {
			rr := httptest.NewRecorder()
			h.ServeHTTP(rr, httptest.NewRequest(
				http.MethodGet, p, nil))
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rr.Code)
			}
			body := rr.Body.String()

			// Footer marker.
			startMark := `class="build-info-footer"`
			startIdx := strings.Index(body, startMark)
			if startIdx < 0 {
				t.Fatalf("build-info footer not rendered on %s",
					p)
			}
			endIdx := strings.Index(body[startIdx:], "</footer>")
			if endIdx < 0 {
				t.Fatalf("build-info footer end tag missing on %s",
					p)
			}
			footer := body[startIdx : startIdx+endIdx]

			// Positive substrings — the spec content shape.
			wants := []string{
				// Version (cfg.Build = "test" in mountWithFake).
				`dagnats test`,
				// NATS host URL.
				`nats://127.0.0.1:4222`,
				// Embedded marker.
				`(embedded)`,
				// Provisioned / known stream count.
				`3/4 streams`,
			}
			for _, w := range wants {
				if !strings.Contains(footer, w) {
					t.Errorf("%s: footer missing %q (was: %s)",
						p, w, footer)
				}
			}

			// Regression guard: the audit-locked anti-pattern is
			// duplicating the header connection pill's real-time
			// status. The footer is identity, not status.
			for _, banned := range []string{"ONLINE", "OFFLINE"} {
				if strings.Contains(footer, banned) {
					t.Errorf("%s: footer must not contain %q "+
						"(duplicates header connection pill); "+
						"was: %s", p, banned, footer)
				}
			}
		})
	}
}

// TestBuildInfoFooter_OmitsEmbeddedWhenExternal asserts the
// "(embedded)" marker is suppressed when the NATS connection is
// against an external server. The footer must not lie about
// deployment topology.
func TestBuildInfoFooter_OmitsEmbeddedWhenExternal(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap = ConfigSnapshot{
		NATSURL:      "nats://10.0.0.1:4222",
		NATSEmbedded: false,
		Streams: []StreamSnapshot{
			{Name: "WORKFLOW_HISTORY", Provisioned: true},
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	startIdx := strings.Index(body, `class="build-info-footer"`)
	if startIdx < 0 {
		t.Fatalf("footer not rendered")
	}
	endIdx := strings.Index(body[startIdx:], "</footer>")
	if endIdx < 0 {
		t.Fatalf("footer end tag missing")
	}
	footer := body[startIdx : startIdx+endIdx]

	if !strings.Contains(footer, "nats://10.0.0.1:4222") {
		t.Errorf("footer missing external NATS URL; was: %s",
			footer)
	}
	if strings.Contains(footer, "(embedded)") {
		t.Errorf("external deployment must not show (embedded) "+
			"marker; was: %s", footer)
	}
}

// TestBuildInfoFooter_ClickToCopyHook asserts the host URL is wrapped
// in a click-to-copy affordance so operators can grab it without
// selecting text. Asserts the hook attribute the small JS handler
// keys off — the JS itself runs in the browser-smoke test, not here.
func TestBuildInfoFooter_ClickToCopyHook(t *testing.T) {
	fake := newFakeDS()
	fake.configSnap = ConfigSnapshot{
		NATSURL:      "nats://127.0.0.1:4222",
		NATSEmbedded: true,
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(
		http.MethodGet, "/console/", nil))
	body := rr.Body.String()

	startIdx := strings.Index(body, `class="build-info-footer"`)
	if startIdx < 0 {
		t.Fatalf("footer not rendered")
	}
	endIdx := strings.Index(body[startIdx:], "</footer>")
	footer := body[startIdx : startIdx+endIdx]

	// Hook attribute + copy payload.
	if !strings.Contains(footer, `data-copy="nats://127.0.0.1:4222"`) {
		t.Errorf("footer missing click-to-copy hook attribute; "+
			"was: %s", footer)
	}
}
