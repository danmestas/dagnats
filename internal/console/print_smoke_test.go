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
//     seeded with a known run. After the mockup reshape the run detail
//     page has three panels: Events (eager, server-rendered rows),
//     Input/Output (lazy — placeholder text `Loading input/output…`),
//     and Timeline (server-rendered gantt of the step list). We seed a
//     known event type and step names so each panel contributes a unique
//     string we can search for in the PDF.
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

	// Seed a fake run whose three panels each emit a unique marker. The
	// Events panel is eager: the seeded event type `workflow.started`
	// renders into the default-active panel. The Timeline panel (hidden
	// until clicked) server-renders the step list, so the step name
	// `panel-marker-timeline` is our marker for that panel reaching the
	// print PDF. The Input/Output panel stays lazy and carries the
	// `Loading input/output…` placeholder.
	fake := newFakeDS()
	fake.workflows = []dag.WorkflowDef{
		{
			Name:    "alpha",
			Version: "v1",
			Steps: []dag.StepDef{
				{ID: "panel-marker-timeline", Task: "echo",
					Timeout: time.Minute},
			},
		},
	}
	now := time.Now()
	run := runWithSteps("run-print", "alpha",
		dag.RunStatusFailed,
		map[string]dag.StepState{
			"panel-marker-timeline": {
				Status:   dag.StepStatusFailed,
				Attempts: 1,
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

	// Per-panel markers after the mockup reshape. Events panel: eager,
	// so the seeded event type renders directly into the default-active
	// panel. IO panel: still lazy, so its `Loading input/output…`
	// placeholder is the marker. Timeline panel: hidden until clicked,
	// but the print rule must expand it — the server-rendered step name
	// is the marker proving the hidden panel reached the PDF.
	markers := []struct {
		name string
		want string
	}{
		{"events-panel", "workflow.started"},
		{"io-panel", "Loading input"},
		{"timeline-panel", "panel-marker-timeline"},
	}
	for _, m := range markers {
		if !strings.Contains(body, m.want) {
			t.Errorf("print PDF missing %s marker %q — "+
				"tab not expanded in print media",
				m.name, m.want)
		}
	}
}
