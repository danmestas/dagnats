// print_smoke_test.go verifies the @media print stylesheet expands
// every run-detail tab panel so PDFs/printouts include the full
// content, not only the active tab.
//
// Issue #283 (Phase 2 audit): the audit claimed the print rule needed
// to switch from `.tabs-content[hidden]` to `[role="tabpanel"][hidden]`.
// The Ousterhout review found the `.tabs-content[hidden] { display:
// block !important }` rule already exists at basecoat.css:325. This
// test is the verification gate: it renders /console/runs/<id> in
// real headless Chrome, prints to PDF (which Chrome serves with print
// media emulation), and asserts every panel's marker text reaches the
// PDF. If all three panels show up, the existing rule already does
// what the audit thought needed adding — no selector change required.
//
// Methodology:
//   - Skip cleanly when Chrome or pdftotext aren't available, or when
//     CI sets DAGNATS_SKIP_BROWSER_SMOKE=1. Print verification is a
//     smoke test, not a hard CI gate.
//   - Boot the console via httptest.Server with a fake data source
//     seeded with a known run. The Steps panel is server-rendered;
//     Events + Input/Output panels carry placeholder text from the
//     run_detail.html template (`Loading events`, `Loading input`)
//     because they lazy-load via SSE. All three placeholders are
//     unique strings we can search for in the PDF.
//   - Drive Chrome with `--headless --print-to-pdf`. Chrome
//     automatically emulates print media for this mode, which is
//     exactly the cascade we need to verify.
//   - Run pdftotext on the result and assert every panel's marker
//     string appears. Minimum 3 assertions (one per panel) + a
//     positive assertion that the PDF is non-empty.
//   - Bounded: 60s total deadline. The Chrome subprocess gets 30s,
//     pdftotext 10s.
package console

import (
	"bytes"
	"context"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
)

// chromeBinary returns the local Chrome path. macOS uses the bundled
// app; other platforms fall back to anything `chrome`/`chromium`/
// `google-chrome` on PATH. Returns "" + skip when nothing usable
// is found.
func chromeBinary() string {
	if runtime.GOOS == "darwin" {
		p := "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome"
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	for _, name := range []string{
		"google-chrome", "chrome", "chromium", "chromium-browser",
	} {
		if p, err := exec.LookPath(name); err == nil {
			return p
		}
	}
	return ""
}

// TestPrintCSS_allRunDetailTabsRender pins the print-media fix from
// issue #283. Every tab panel on /console/runs/<id> must reach the
// printed PDF, not only the currently-active one.
func TestPrintCSS_allRunDetailTabsRender(t *testing.T) {
	if os.Getenv("DAGNATS_SKIP_BROWSER_SMOKE") == "1" {
		t.Skip("DAGNATS_SKIP_BROWSER_SMOKE=1; skipping print smoke")
	}
	chrome := chromeBinary()
	if chrome == "" {
		t.Skip("no chrome binary found; skipping print smoke")
	}
	if _, err := exec.LookPath("pdftotext"); err != nil {
		t.Skipf("pdftotext not installed: %v", err)
	}

	// Seed a fake run with a unique marker on the steps panel. The
	// Events + Input/Output panels render placeholder text from
	// run_detail.html — those strings (`Loading events`,
	// `Loading input/output`) are our markers for the lazy-loaded
	// panels.
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{sampleWorkflow("alpha")}
	now := time.Now()
	run := runWithSteps("run-print", "alpha",
		dag.RunStatusFailed,
		map[string]dag.StepState{
			"first": {
				Status:   dag.StepStatusFailed,
				Attempts: 1,
				Error:    "panel-marker-steps",
			},
		},
		now.Add(-time.Minute),
	)
	fake.runs = []dag.WorkflowRun{run}
	fake.events["run-print"] = []api.RunEvent{
		{
			Type: "workflow.started", RunID: "run-print",
			Timestamp: now.Add(-2 * time.Minute),
		},
	}

	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	dir := t.TempDir()
	pdf := filepath.Join(dir, "run-print.pdf")
	url := srv.URL + "/console/runs/run-print"

	ctx, cancel := context.WithTimeout(
		context.Background(), 60*time.Second,
	)
	defer cancel()

	// Chrome's print-to-pdf path emulates print media on the rendered
	// page. --no-sandbox keeps the test compatible with CI runners
	// that don't have user-namespace support; --disable-gpu avoids a
	// noisy GPU stack on headless. --virtual-time-budget lets any
	// onload work finish before the snapshot.
	chromeCtx, chromeCancel := context.WithTimeout(ctx, 30*time.Second)
	defer chromeCancel()
	args := []string{
		"--headless=new",
		"--disable-gpu",
		"--no-sandbox",
		"--virtual-time-budget=5000",
		"--print-to-pdf=" + pdf,
		"--print-to-pdf-no-header",
		url,
	}
	cmd := exec.CommandContext(chromeCtx, chrome, args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("chrome --print-to-pdf: %v\nstdout: %s\nstderr: %s",
			err, stdout.String(), stderr.String())
	}

	info, err := os.Stat(pdf)
	if err != nil {
		t.Fatalf("pdf not produced at %s: %v", pdf, err)
	}
	if info.Size() == 0 {
		t.Fatalf("pdf at %s is empty", pdf)
	}

	pdfCtx, pdfCancel := context.WithTimeout(ctx, 10*time.Second)
	defer pdfCancel()
	txt := filepath.Join(dir, "run-print.txt")
	pdfCmd := exec.CommandContext(pdfCtx,
		"pdftotext", "-layout", pdf, txt)
	var pdfErr bytes.Buffer
	pdfCmd.Stderr = &pdfErr
	if err := pdfCmd.Run(); err != nil {
		t.Fatalf("pdftotext: %v\nstderr: %s", err, pdfErr.String())
	}
	raw, err := os.ReadFile(txt)
	if err != nil {
		t.Fatalf("read pdftotext output: %v", err)
	}
	body := string(raw)
	t.Logf("pdf text (%d bytes):\n%s", len(body), body)

	// Per-panel markers. Steps panel: the step error string we seeded
	// above is server-rendered into the (default-active) panel.
	// Events + Input/Output panels: the inline run_detail.html template
	// emits these placeholder strings into the hidden panels — they
	// are exactly what we expect a printout to capture for tabs that
	// haven't yet been clicked.
	markers := []struct {
		name string
		want string
	}{
		{"steps-panel", "panel-marker-steps"},
		{"events-panel", "Loading events"},
		{"io-panel", "Loading input"},
	}
	for _, m := range markers {
		if !strings.Contains(body, m.want) {
			t.Errorf("print PDF missing %s marker %q — "+
				"tab not expanded in print media",
				m.name, m.want)
		}
	}
}
