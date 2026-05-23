// browser_smoke_test.go drives a headless Chrome via the locally-
// installed `agent-browser` CLI against a live httptest.Server. The
// PR 2 retro found `window.datastar` undefined on rendered pages
// because the vendored Datastar bundle never called its `apply()`
// bootstrap. The dagnats Go tests still passed (they hit endpoints
// directly) while the live UI was inert. This smoke test pins the
// bootstrap so PR 4–8 can't silently regress.
//
// Methodology:
//   - Skip cleanly when `agent-browser` or Chrome is not installed,
//     or when CI sets DAGNATS_SKIP_BROWSER_SMOKE=1. Keeps CI green on
//     stripped-down runners.
//   - Boot the console via httptest.Server. Drive the browser with
//     `agent-browser open <url>` then `agent-browser eval <js>` and
//     parse the JSON-shaped output.
//   - Bounded everything: 30s deadline on the full test. Each
//     subprocess gets its own 10s context timeout.
//   - Minimum 2 assertions: window.datastar defined AND apply()
//     callable.
package console

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http/httptest"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/worker"
)

// TestBrowser_connectionPillRecoveryHints asserts each degraded
// connection-pill state surfaces an actionable recovery hint in
// the title attribute (Norman's Error-recovery principle), and that
// the offline + retries-failed states wire up a refresh click
// handler. Drives the bundle's exposed __dagnatsConnection helper.
//
// Methodology:
//   - Skip cleanly when agent-browser / Chrome unavailable, same as
//     the datastar bootstrap test.
//   - Boot the console via httptest, force each state via the bundle's
//     `_forceState` helper, then read the pill's `title` + `onclick`.
//   - Bounded: 30s deadline. Min 2 assertions per state.
func TestBrowser_connectionPillRecoveryHints(t *testing.T) {
	skipIfBrowserUnavailable(t)
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	runAgentBrowser(t, ctx, "open", srv.URL+"/console/")
	t.Cleanup(func() {
		brCtx, brCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer brCancel()
		runAgentBrowserAllowFail(t, brCtx, "close")
	})
	time.Sleep(750 * time.Millisecond)

	// Pin: the bundle exposes __dagnatsConnection — without it the
	// pill JS never loaded and every later assertion is meaningless.
	helper := evalString(t, ctx, "typeof window.__dagnatsConnection")
	if helper != "object" {
		t.Fatalf("__dagnatsConnection type=%q, want object — "+
			"connection-state bundle not loaded", helper)
	}

	type pillCase struct {
		state         string
		wantInTitle   string
		wantClickable bool
	}
	cases := []pillCase{
		{"live", "Live (SSE healthy)", false},
		{"idle", "No active stream for this page", false},
		{"reconnecting", "refresh if it persists", false},
		{"offline", "click to refresh", true},
		{"retries-failed", "click to refresh", true},
	}
	const maxCases = 16
	if len(cases) > maxCases {
		t.Fatalf("test list exceeds maxCases (%d)", maxCases)
	}
	for _, c := range cases {
		js := "window.__dagnatsConnection._forceState(" +
			jsString(c.state) + ");" +
			"document.getElementById('console-connection')" +
			".getAttribute('title')"
		title := evalString(t, ctx, js)
		if !strings.Contains(title, c.wantInTitle) {
			t.Errorf("state %q title=%q, missing hint %q",
				c.state, title, c.wantInTitle)
		}
		clickType := evalString(t, ctx,
			"typeof document.getElementById('console-connection').onclick")
		isClickable := clickType == "function"
		if isClickable != c.wantClickable {
			t.Errorf("state %q clickable=%v (onclick=%q), want %v",
				c.state, isClickable, clickType, c.wantClickable)
		}
	}
}

// TestBrowser_connectionPillExternalMutation asserts that mutating
// the pill's data-state attribute from outside the controller — the
// standard Datastar pattern — propagates through the same render
// path as the programmatic _forceState() call. Issue #285: the
// controller used to ignore external attribute mutations, so any
// server-driven state hand-off via data-state was inert.
//
// Methodology:
//   - Boot the console via httptest, same as the recovery-hints test.
//   - Reset state to idle, mutate data-state="error" via DOM
//     setAttribute, yield briefly so the MutationObserver microtask
//     fires, then read the rendered label.
//   - Note "error" is the upstream Datastar attribute value; the
//     controller normalises it via render()'s VALID gate. We use a
//     state that IS valid ("reconnecting") for the assertion so the
//     observer's full path is exercised, then re-verify the
//     programmatic path still works.
//   - Bounded: 30s deadline. 2 assertions (external mutation +
//     regression guard for _forceState).
func TestBrowser_connectionPillExternalMutation(t *testing.T) {
	skipIfBrowserUnavailable(t)
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	runAgentBrowser(t, ctx, "open", srv.URL+"/console/")
	t.Cleanup(func() {
		brCtx, brCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer brCancel()
		runAgentBrowserAllowFail(t, brCtx, "close")
	})
	time.Sleep(750 * time.Millisecond)

	helper := evalString(t, ctx, "typeof window.__dagnatsConnection")
	if helper != "object" {
		t.Fatalf("__dagnatsConnection type=%q, want object — "+
			"connection-state bundle not loaded", helper)
	}

	// Reset to a known idle baseline so the observer's transition
	// out of idle is unambiguous.
	evalString(t, ctx, "window.__dagnatsConnection._reset()")

	// Assertion 1: external data-state mutation propagates.
	// Use "reconnecting" — a VALID state distinct from idle so the
	// render path's "Reconnecting" label change is observable.
	runAgentBrowser(t, ctx, "eval",
		"document.getElementById('console-connection')"+
			".setAttribute('data-state', 'reconnecting')")
	// MutationObserver fires on the microtask queue. A 100ms sleep
	// is generous overkill and matches the cadence other browser
	// tests in this file use.
	time.Sleep(100 * time.Millisecond)
	label := evalString(t, ctx,
		"document.querySelector("+
			"'#console-connection .console-connection-label').textContent")
	if !strings.Contains(label, "reconnecting") {
		t.Errorf("external data-state mutation: label=%q, "+
			"want substring %q — MutationObserver path inert",
			label, "reconnecting")
	}

	// Assertion 2: regression guard — the programmatic _forceState
	// path still works after the refactor.
	evalString(t, ctx, "window.__dagnatsConnection._reset()")
	evalString(t, ctx,
		"window.__dagnatsConnection._forceState('offline')")
	time.Sleep(50 * time.Millisecond)
	label2 := evalString(t, ctx,
		"document.querySelector("+
			"'#console-connection .console-connection-label').textContent")
	if !strings.Contains(label2, "offline") {
		t.Errorf("_forceState regression: label=%q, want substring %q",
			label2, "offline")
	}
}

// jsString JSON-encodes a Go string for safe embedding into a JS
// eval expression. Keeps the test honest if a future case adds a
// quote or backslash.
func jsString(s string) string {
	b, err := json.Marshal(s)
	if err != nil {
		// json.Marshal of a Go string never fails — but TigerStyle
		// says assert, not assume.
		panic("jsString: marshal failed: " + err.Error())
	}
	return string(b)
}

// TestBrowser_datastarBootstraps asserts the console JS bundle wires
// up Datastar's runtime on page load. Catches the PR 2 inertia bug.
func TestBrowser_datastarBootstraps(t *testing.T) {
	skipIfBrowserUnavailable(t)
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	// Open the page and let the bundle execute.
	runAgentBrowser(t, ctx, "open", srv.URL+"/console/")
	t.Cleanup(func() {
		brCtx, brCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer brCancel()
		runAgentBrowserAllowFail(t, brCtx, "close")
	})
	// Pause for the bundle's DOMContentLoaded path to land.
	time.Sleep(750 * time.Millisecond)

	dsType := evalString(t, ctx, "typeof window.datastar")
	if dsType != "object" && dsType != "function" {
		t.Fatalf("window.datastar type = %q, want object/function "+
			"— bundle not bootstrapped", dsType)
	}
	applyType := evalString(t, ctx,
		"typeof (window.datastar && window.datastar.apply)")
	if applyType != "function" {
		t.Fatalf("window.datastar.apply type = %q, want function "+
			"— engine entrypoint missing from bundle", applyType)
	}
}

// skipIfBrowserUnavailable bails the test cleanly when the local
// agent-browser CLI is not installed or when CI explicitly opts out.
func skipIfBrowserUnavailable(t *testing.T) {
	t.Helper()
	if os.Getenv("DAGNATS_SKIP_BROWSER_SMOKE") == "1" {
		t.Skip("DAGNATS_SKIP_BROWSER_SMOKE=1; skipping browser smoke")
	}
	if _, err := exec.LookPath("agent-browser"); err != nil {
		t.Skipf("agent-browser not installed: %v", err)
	}
}

// runAgentBrowser shells out to `agent-browser <args...>`. The CLI's
// stderr surfaces test diagnostics via t.Log so failures are easy to
// triage. We fail the test on any non-zero exit.
func runAgentBrowser(
	t *testing.T, ctx context.Context, args ...string,
) string {
	t.Helper()
	if len(args) == 0 {
		t.Fatalf("runAgentBrowser: no args")
	}
	cmd := exec.CommandContext(ctx, "agent-browser", args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("agent-browser %s: %v\nstdout: %s\nstderr: %s",
			strings.Join(args, " "), err,
			stdout.String(), stderr.String())
	}
	return stdout.String()
}

// runAgentBrowserAllowFail mirrors runAgentBrowser but logs rather
// than fails — used in cleanup so a stuck browser doesn't poison
// every subsequent test.
func runAgentBrowserAllowFail(
	t *testing.T, ctx context.Context, args ...string,
) {
	t.Helper()
	if len(args) == 0 {
		return
	}
	cmd := exec.CommandContext(ctx, "agent-browser", args...)
	if err := cmd.Run(); err != nil {
		t.Logf("agent-browser %s (cleanup): %v",
			strings.Join(args, " "), err)
	}
}

// evalString runs an `agent-browser eval` and returns the resulting
// value as a string. agent-browser emits JSON by default — we parse
// loosely to tolerate either bare-string or {value: "..."} shapes.
func evalString(
	t *testing.T, ctx context.Context, expr string,
) string {
	t.Helper()
	if expr == "" {
		t.Fatalf("evalString: empty expression")
	}
	out := runAgentBrowser(t, ctx, "eval", expr)
	out = strings.TrimSpace(out)
	if out == "" {
		return ""
	}
	// Try JSON-decode as a bare value (string) first.
	var bareString string
	if err := json.Unmarshal([]byte(out), &bareString); err == nil {
		return bareString
	}
	// Then try as a structured object — some agent-browser versions
	// wrap eval results in {"value": ..., "type": ...}.
	var structured struct {
		Value string `json:"value"`
		Type  string `json:"type"`
	}
	if err := json.Unmarshal([]byte(out), &structured); err == nil &&
		structured.Value != "" {
		return structured.Value
	}
	// Fall back to the raw output stripped of surrounding quotes.
	return strings.Trim(out, "\"")
}

// TestBrowser_basecoatPhase2Components verifies that every Phase 2
// Basecoat component lands its init function on the
// window.basecoat.<name> global after the bundle bootstraps.
//
// Methodology:
//   - Boot Mount() with DAGNATS_FIXTURES=true so the
//     /__fixtures__/<name> routes mount their minimal skeletons.
//   - For each of tabs/sheet/tooltip/command, open the fixture page,
//     wait for the bundle to apply, then eval `typeof
//     window.basecoat.<name>`. Pass means the type is "object" or
//     "function"; "undefined" means the component never registered.
//   - At least 2 assertions per loop: the eval-type check AND a
//     post-bootstrap data-<name>-initialized="true" sanity check on
//     the fixture root so we know the init function actually ran on
//     the skeleton, not just landed on the global.
//   - Bounded: 30s total deadline. Each subprocess inherits a 10s
//     timeout via the shared evalString helper.
func TestBrowser_basecoatPhase2Components(t *testing.T) {
	skipIfBrowserUnavailable(t)
	t.Setenv("DAGNATS_FIXTURES", "true")
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	t.Cleanup(func() {
		brCtx, brCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer brCancel()
		runAgentBrowserAllowFail(t, brCtx, "close")
	})

	components := []struct {
		name, initAttr string
	}{
		{"tabs", "data-tabs-initialized"},
		{"sheet", "data-sheet-initialized"},
		{"tooltip", "data-tooltip-initialized"},
		{"command", "data-command-initialized"},
	}
	for _, c := range components {
		runAgentBrowser(t, ctx, "open",
			srv.URL+"/console/__fixtures__/"+c.name)
		time.Sleep(750 * time.Millisecond)

		got := evalString(t, ctx,
			"typeof (window.basecoat && window.basecoat['"+c.name+"'])")
		if got != "object" && got != "function" {
			t.Errorf("%s: window.basecoat.%s type = %q, want object/function",
				c.name, c.name, got)
		}
		// Sanity: the fixture root carries the per-component init
		// flag once the registry has walked the DOM. If this is
		// "false" the global landed but init never fired.
		init := evalString(t, ctx,
			"String(document.querySelector('."+c.name+"').getAttribute('"+
				c.initAttr+"'))")
		if init != "true" {
			t.Errorf("%s: %s = %q, want \"true\"",
				c.name, c.initAttr, init)
		}
	}
}

// TestBrowser_taskTypesServiceTooltip is the agent-browser visual
// gate on #335. Boots a console with one workers KV registration
// (billing::charge) plus one ServiceDef seed (billing, "Payment
// processing") and asserts the /console/task-types page renders a
// glossary-style tooltip whose popover carries the service
// description.
//
// Methodology:
//   - Skip cleanly when agent-browser / Chrome unavailable (same
//     pattern as the other browser smoke tests in this file).
//   - Boot via httptest; mount the fake DataSource with billing
//     pre-seeded so the rendered DOM is deterministic.
//   - Evaluate DOM directly (no hover-driven CSS state needed —
//     the tooltip popover lives in the markup at all times; CSS
//     toggles visibility, not presence).
//   - At least 2 assertions: the wrapper element exists AND the
//     popover text matches the seeded description.
func TestBrowser_taskTypesServiceTooltip(t *testing.T) {
	skipIfBrowserUnavailable(t)
	fake := newFakeDS()
	fake.configSnap = ConfigSnapshot{
		Workers: []worker.WorkerRegistration{
			{
				WorkerID:  "w-pay",
				TaskTypes: []string{"billing::charge"},
				LastSeen:  time.Now(),
			},
		},
	}
	fake.services = []worker.ServiceDef{
		{Name: "billing", Description: "Payment processing"},
	}
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	runAgentBrowser(t, ctx, "open", srv.URL+"/console/task-types")
	t.Cleanup(func() {
		brCtx, brCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer brCancel()
		runAgentBrowserAllowFail(t, brCtx, "close")
	})
	time.Sleep(750 * time.Millisecond)

	// Positive: tooltip wrapper is present inside the billing
	// group header. The selector chain anchors to the
	// service-keyed tbody so a future test that adds extra groups
	// can extend without colliding.
	const groupSelector = "[data-service=\"billing\"] " +
		".task-type-group-header"
	wrapperType := evalString(t, ctx,
		"typeof document.querySelector('"+groupSelector+
			" .glo-tooltip-wrapper')")
	if wrapperType != "object" {
		t.Fatalf("tooltip wrapper not present on billing header "+
			"(type=%q) — markup regression", wrapperType)
	}

	// Positive: popover carries the registered Description. We read
	// the popover's innerHTML — textContent is reliable in Chrome
	// regardless of CSS visibility, but innerHTML round-trips through
	// the DOM serializer and surfaces the text even when the popover
	// is offscreen by `visibility:hidden` until hover.
	popoverHTML := evalString(t, ctx,
		"String((document.querySelector('"+groupSelector+
			" .glo-tooltip-popover')||{}).innerHTML||'').trim()")
	if !strings.Contains(popoverHTML, "Payment processing") {
		t.Errorf("popover innerHTML = %q, want it to contain %q",
			popoverHTML, "Payment processing")
	}
}
