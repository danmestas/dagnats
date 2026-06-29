// console_batch_b_test.go covers the design-system fixes from the UI
// audit + user screenshots (branch fix/console-batch-b). Three Norman
// affordance/consistency fixes, asserted at the rendered-DOM level:
//
//	Fix 1 — the "+ Add trigger" button must render inside a
//	  .console-section-actions container (the consistent slot every
//	  other list/detail page uses), NOT stranded inside the lede
//	  <p class="console-lede console-section-meta"> next to the count.
//
//	Fix 2 — the live connection-status pill (#console-connection) must
//	  render in the header-utils container, and must come BEFORE the
//	  theme toggle in DOM order so it is the most prominent item in the
//	  utils area (visible near the top of the sidebar, not only after
//	  the actor/theme chrome at the bottom).
//
//	Fix 3a — AUTH MODE must be a clearly-non-interactive read-only
//	  segmented indicator: a role="group" wrapper, a "(set via config)"
//	  annotation, and NO false-affordance (no role="button" on the
//	  pills).
//
//	Fix 3b — logs severity chips must be REAL toggles: each chip carries
//	  role="button" so its affordance matches its behaviour (clicking it
//	  sets the severity select + submits the filter form).
//
// Methodology:
//   - Pure handler tests against the fake data sources (no NATS).
//   - Each subtest mounts its own console; tests never share state.
//   - Region-scoped substring assertions where a token could collide
//     elsewhere on the page (header-utils, access-posture card).
//   - Min 2 assertions per test (positive + negative space).
package console

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/trigger"
)

// headerUtilsRegion returns the slice of body inside the
// .console-header-utils container so DOM-order assertions are scoped to
// the utils strip and can't be satisfied by chrome elsewhere.
func headerUtilsRegion(t *testing.T, body string) string {
	t.Helper()
	const marker = `class="console-header-utils"`
	start := strings.Index(body, marker)
	if start < 0 {
		t.Fatalf("console-header-utils container not rendered")
	}
	rest := body[start:]
	// The utils container is followed by the build-info rail slot.
	end := strings.Index(rest, `class="build-info-footer-slot"`)
	if end < 0 {
		t.Fatalf("could not find end of console-header-utils region")
	}
	return rest[:end]
}

// Fix 1 — the add-trigger button lives in a section-actions slot, not
// stranded in the lede paragraph.
func TestTriggersList_addButtonInSectionActions(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{sampleTrigger("c1", "alpha", "cron")}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/triggers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	actionsIdx := strings.Index(body, `class="console-section-actions"`)
	if actionsIdx < 0 {
		t.Fatalf("console-section-actions container not rendered")
	}
	btnIdx := strings.Index(body, "trigger-add-btn")
	if btnIdx < 0 {
		t.Fatalf("trigger-add-btn not rendered")
	}
	// The button must come AFTER the section-actions opener and BEFORE
	// that container closes — i.e. nested inside it.
	actionsTail := body[actionsIdx:]
	closeIdx := strings.Index(actionsTail, "</div>")
	relBtn := strings.Index(actionsTail, "trigger-add-btn")
	if relBtn < 0 || closeIdx < 0 || relBtn > closeIdx {
		t.Errorf("trigger-add-btn not nested inside console-section-actions")
	}

	// Negative space: the button must NOT be inside the count lede <p>.
	ledeIdx := strings.Index(body, "console-section-meta")
	if ledeIdx >= 0 {
		ledeTail := body[ledeIdx:]
		ledeClose := strings.Index(ledeTail, "</p>")
		ledeBtn := strings.Index(ledeTail, "trigger-add-btn")
		if ledeBtn >= 0 && ledeClose >= 0 && ledeBtn < ledeClose {
			t.Errorf("trigger-add-btn still stranded inside the lede <p>")
		}
	}
}

// Fix 2 — the connection pill renders in header-utils and precedes the
// theme toggle in DOM order.
func TestLayout_connectionPillInHeaderUtilsBeforeTheme(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/triggers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	region := headerUtilsRegion(t, rr.Body.String())

	connIdx := strings.Index(region, `id="console-connection"`)
	if connIdx < 0 {
		t.Fatalf("connection pill not rendered inside console-header-utils")
	}
	themeIdx := strings.Index(region, `id="theme-toggle"`)
	if themeIdx < 0 {
		t.Fatalf("theme toggle not rendered inside console-header-utils")
	}
	// Negative space: the connection pill must come BEFORE the theme
	// toggle so it surfaces near the top of the utils strip.
	if connIdx > themeIdx {
		t.Errorf("connection pill (%d) renders after theme toggle (%d); "+
			"it should precede it in the utils strip", connIdx, themeIdx)
	}
}

// Fix 3a — AUTH MODE is a read-only segmented indicator with no
// false-affordance and a "set via config" annotation.
func TestConfigPage_authModeIsReadOnlyIndicator(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/config", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	region := accessCardRegion(t, rr.Body.String())

	// The mode pills are wrapped in a labelled group so screen readers
	// read them as one segmented indicator, not loose spans.
	if !strings.Contains(region, `class="config-modegroup"`) {
		t.Errorf("auth-mode pills not wrapped in .config-modegroup indicator")
	}
	// A "set via config" annotation tells the operator why it's read-only.
	if !strings.Contains(region, "set via config") {
		t.Errorf("auth-mode group missing the 'set via config' annotation")
	}
	// Negative space: the pills must NOT carry a button false-affordance.
	if strings.Contains(region, `role="button"`) {
		t.Errorf("auth-mode pills carry a role=button false-affordance")
	}
}

// Fix 3b — logs severity chips are real button-role toggles.
func TestLogsPage_severityChipsAreToggles(t *testing.T) {
	t.Parallel()
	base := time.Now()
	fake := &fakeLogRing{
		records: []slog.Record{
			seedRecord(base, -3*time.Second, slog.LevelInfo, "engine: startup"),
			seedRecord(base, -2*time.Second, slog.LevelWarn, "trigger: skew"),
			seedRecord(base, -1*time.Second, slog.LevelError, "worker: crash"),
		},
	}
	srv := httptest.NewServer(mountWithLogRing(t, fake))
	defer srv.Close()
	resp, err := http.Get(srv.URL + "/console/logs")
	if err != nil {
		t.Fatalf("GET /console/logs: %v", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	got := string(body)

	stripIdx := strings.Index(got, "logs-severity-chip-strip")
	if stripIdx < 0 {
		t.Fatalf("severity chip strip not rendered (no seeded counts?)")
	}
	// Each chip must carry role="button" so its affordance (looks
	// pressable) matches its behaviour (sets the severity filter).
	chip := got[stripIdx:]
	if !strings.Contains(chip, `role="button"`) {
		t.Errorf("severity chips missing role=button toggle affordance")
	}
	// And a keyboard-focusable tabindex so the toggle is reachable.
	if !strings.Contains(chip, `tabindex="0"`) {
		t.Errorf("severity chips not keyboard-focusable (no tabindex)")
	}
}
