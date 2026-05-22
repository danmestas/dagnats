package console

import (
	"net/http"
	"os"
	"strings"
)

// fixturesEnabled gates the /__fixtures__/* routes used by Phase 2
// browser smoke tests. Off by default; production binaries never
// expose fixture pages. Tests flip DAGNATS_FIXTURES=true before Mount.
//
// Why an env var rather than a build tag: the smoke test in this
// package needs to instantiate Mount() in-process via httptest.Server;
// build tags can't be flipped per-test. The fixture pages render
// trivial Basecoat component skeletons against the live asset bundle,
// so it is acceptable to ship the route code in the binary as long as
// the runtime gate stays default-off.
//
// Defense in depth: a single misconfigured DAGNATS_FIXTURES env in
// production would otherwise expose the fixture surface. The explicit
// DAGNATS_ENV=="production" short-circuit means even if the flag leaks
// on, prod still refuses. We only fire the prod-block when DAGNATS_ENV
// is set to "production" explicitly (rather than fail-closed on unset)
// so the existing dev/test workflow — which never sets DAGNATS_ENV —
// keeps working; production deploys are expected to set it.
func fixturesEnabled() bool {
	env := strings.ToLower(strings.TrimSpace(os.Getenv("DAGNATS_ENV")))
	if env == "production" {
		return false
	}
	v := strings.ToLower(strings.TrimSpace(os.Getenv("DAGNATS_FIXTURES")))
	if v == "" {
		return false
	}
	return v == "1" || v == "true" || v == "yes"
}

// serveBasecoatFixture routes /__fixtures__/<component> to a minimal
// HTML page that loads the live console asset bundle and renders a
// single Basecoat component skeleton. The browser smoke test asserts
// each component bootstraps onto window.basecoat.<component>.
//
// Bounded: the dispatch table is fixed at four components; any other
// path returns 404 so the route does not become a generic HTML
// rendering surface.
func serveBasecoatFixture(w http.ResponseWriter, r *http.Request) {
	if w == nil {
		panic("serveBasecoatFixture: w is nil")
	}
	if r == nil {
		panic("serveBasecoatFixture: r is nil")
	}
	name := strings.TrimPrefix(r.URL.Path, "/console/__fixtures__/")
	body, ok := basecoatFixtureBody(name)
	if !ok {
		http.NotFound(w, r)
		return
	}
	page := fixtureHTML(name, body)
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Robots-Tag", "noindex")
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte(page)); err != nil {
		// Best-effort: client disconnect during write is non-fatal.
		_ = err
	}
}

// basecoatFixtureBody returns the per-component skeleton HTML wired to
// hit Basecoat's expected selector. Each skeleton is the smallest
// markup that exercises the component's init function.
func basecoatFixtureBody(name string) (string, bool) {
	switch name {
	case "tabs":
		return fixtureBodyTabs(), true
	case "sheet":
		return fixtureBodySheet(), true
	case "tooltip":
		return fixtureBodyTooltip(), true
	case "command":
		return fixtureBodyCommand(), true
	}
	return "", false
}

// fixtureHTML wraps the per-component body in a minimal HTML page that
// pulls in the live console asset bundle. Keeping the chrome small so
// the smoke test's window.basecoat.* assertion is unambiguous — no
// stray markup that might also load Basecoat indirectly.
func fixtureHTML(name, body string) string {
	const tmpl = `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>fixture: %s</title>
<link rel="stylesheet" href="/console/assets/basecoat.css">
<link rel="stylesheet" href="/console/assets/app.css">
<script src="/console/assets/console.js" defer></script>
</head>
<body data-fixture="%s">
<main style="padding:2rem;max-width:640px;margin:0 auto;">
%s
</main>
</body>
</html>`
	return sprintf3(tmpl, name, name, body)
}

// sprintf3 is a tiny inline formatter that substitutes three %s tokens
// without dragging fmt into the request-hot path. Inputs come from a
// fixed dispatch table so there is no untrusted data.
func sprintf3(tmpl, a, b, c string) string {
	out := strings.Replace(tmpl, "%s", a, 1)
	out = strings.Replace(out, "%s", b, 1)
	out = strings.Replace(out, "%s", c, 1)
	return out
}

// fixtureBodyTabs returns the Basecoat tabs skeleton — a tablist with
// two tab triggers + two tabpanels. After init the first tab is
// selected and Arrow keys cycle focus.
func fixtureBodyTabs() string {
	return `<div class="tabs" data-fixture-id="tabs">
  <div role="tablist" aria-label="Demo tabs">
    <button role="tab" id="tab-a" aria-controls="panel-a" aria-selected="true" tabindex="0">Overview</button>
    <button role="tab" id="tab-b" aria-controls="panel-b" aria-selected="false" tabindex="-1">Details</button>
  </div>
  <section id="panel-a" role="tabpanel" aria-labelledby="tab-a">Overview content.</section>
  <section id="panel-b" role="tabpanel" aria-labelledby="tab-b" hidden>Details content.</section>
</div>`
}

// fixtureBodySheet returns the in-house sheet skeleton — a hidden side
// panel plus a trigger button. Clicking the trigger flips
// aria-hidden=false and slides the panel in.
func fixtureBodySheet() string {
	return `<button type="button" data-sheet-target="demo-sheet">Open sheet</button>
<div class="sheet" id="demo-sheet" aria-hidden="true" role="dialog" aria-labelledby="demo-sheet-title">
  <div class="sheet-overlay" aria-hidden="true"></div>
  <aside class="sheet-panel" role="document">
    <header class="sheet-header">
      <h2 class="sheet-title" id="demo-sheet-title">Sheet title</h2>
      <button type="button" class="sheet-close" data-sheet-close aria-label="Close sheet">&times;</button>
    </header>
    <div class="sheet-body">Sheet body content goes here.</div>
  </aside>
</div>`
}

// fixtureBodyTooltip returns the in-house tooltip skeleton — a trigger
// wrapped in .tooltip with [data-tooltip-content] sibling. Hover/focus
// flips data-tooltip-open.
func fixtureBodyTooltip() string {
	return `<span class="tooltip" data-tooltip-side="top">
  <button type="button" data-tooltip-trigger>Hover me</button>
  <span data-tooltip-content>Tooltip body text</span>
</span>`
}

// fixtureBodyCommand returns the Basecoat command (cmd+k) skeleton —
// inline (not in a dialog) so the smoke test can inspect bootstrap
// without driving a dialog open animation.
func fixtureBodyCommand() string {
	return `<div class="command" data-fixture-id="command">
  <header><input type="text" placeholder="Type a command..." aria-label="Command palette search"></header>
  <div role="menu">
    <div role="menuitem" id="cmd-item-1">Go to dashboard</div>
    <div role="menuitem" id="cmd-item-2">Open runs</div>
    <div role="menuitem" id="cmd-item-3">Trigger workflow</div>
  </div>
</div>`
}
