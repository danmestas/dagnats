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
)

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
