// Methodology: integration-fixture render. Not a unit test — gated by
// the DAGNATS_T02_FIXTURE env var so it never runs in CI. The goal is
// to produce a real HTML page (layout-wrapped, with the bundled CSS
// inlined or referenced via file://) that agent-browser can open and
// screenshot for Norman self-check. Toggle on locally:
//
//	DAGNATS_T02_FIXTURE=1 go test ./internal/console/ -run TestStepList_writesFixture
//
// Writes /tmp/step-list-fixture-light.html and
// /tmp/step-list-fixture-dark.html — two pages that differ only in
// the data-theme attribute on <html>, so a single CSS file drives
// both light and dark verification.
package console

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/danmestas/dagnats/dag"
)

// TestStepList_writesFixture emits two HTML files that visualize the
// step list partial against a representative def + run. Skipped unless
// DAGNATS_T02_FIXTURE=1 — keeps CI deterministic and avoids writing
// outside the workspace during normal test runs.
func TestStepList_writesFixture(t *testing.T) {
	if os.Getenv("DAGNATS_T02_FIXTURE") == "" {
		t.Skip("set DAGNATS_T02_FIXTURE=1 to emit the fixture HTML")
	}
	def := &dag.WorkflowDef{
		Name:    "ingest-pipeline",
		Version: "v3",
		Steps: []dag.StepDef{
			{ID: "fetch-source", Type: dag.StepTypeNormal},
			{ID: "validate-schema", Type: dag.StepTypeNormal},
			{ID: "transform-rows", Type: dag.StepTypeNormal},
			{ID: "publish-events", Type: dag.StepTypeNormal},
			{ID: "notify-downstream", Type: dag.StepTypeNormal},
		},
	}
	run := &dag.WorkflowRun{
		Steps: map[string]dag.StepState{
			"fetch-source":      {Status: dag.StepStatusCompleted, Attempts: 1},
			"validate-schema":   {Status: dag.StepStatusCompleted, Attempts: 1},
			"transform-rows":    {Status: dag.StepStatusFailed, Attempts: 3, Error: "row 4221: column 'amount' not a number"},
			"publish-events":    {Status: dag.StepStatusPending},
			"notify-downstream": {Status: dag.StepStatusSkipped, Attempts: 0},
		},
	}
	var step bytes.Buffer
	if err := RenderStepList(&step, def, run); err != nil {
		t.Fatalf("render: %v", err)
	}
	if step.Len() == 0 {
		t.Fatal("empty render")
	}
	cssPath, err := filepath.Abs("assets/sources/basecoat-raw.css")
	if err != nil {
		t.Fatalf("abs css: %v", err)
	}
	appPath, err := filepath.Abs("assets/app.css")
	if err != nil {
		t.Fatalf("abs app css: %v", err)
	}
	if err := writeFixturePage(t, "/tmp/step-list-fixture-light.html", "light", cssPath, appPath, step.String()); err != nil {
		t.Fatalf("light: %v", err)
	}
	if err := writeFixturePage(t, "/tmp/step-list-fixture-dark.html", "dark", cssPath, appPath, step.String()); err != nil {
		t.Fatalf("dark: %v", err)
	}
}

// writeFixturePage renders a minimal HTML page that loads the project
// CSS (basecoat-raw.css + app.css) by file:// path and embeds the
// step list body. theme = "light" | "dark" controls the data-theme
// attribute that the e-ink palette reads via the matching CSS block.
func writeFixturePage(
	t *testing.T, path, theme, cssPath, appPath, body string,
) error {
	if t == nil {
		panic("writeFixturePage: t is nil")
	}
	if theme != "light" && theme != "dark" {
		panic("writeFixturePage: bad theme")
	}
	const tmpl = `<!doctype html>
<html lang="en" data-theme="%s">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>step list fixture (%s)</title>
<link rel="stylesheet" href="file://%s">
<link rel="stylesheet" href="file://%s">
<style>
  body { padding: 2rem; font-family: system-ui, sans-serif; background: var(--bg, #F8F5EF); color: var(--text-primary, #1F1B16); margin: 0; }
  main { max-width: 880px; margin: 0 auto; }
  h1 { font-size: 1.25rem; margin-bottom: 1.25rem; color: var(--text-primary); }
  .card { background: var(--surface, #FFFCF6); border: 1px solid var(--border, #D6CFC0); border-radius: 8px; overflow: hidden; }
</style>
</head>
<body>
<main>
<h1>Run ingest-pipeline / v3 &mdash; step list (%s)</h1>
<div class="card">
%s
</div>
</main>
</body>
</html>`
	out := fmt.Sprintf(tmpl, theme, theme, cssPath, appPath, theme, body)
	if err := os.WriteFile(path, []byte(out), 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	t.Logf("wrote fixture: %s", path)
	return nil
}
