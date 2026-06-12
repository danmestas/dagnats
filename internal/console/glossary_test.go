package console

import (
	"html/template"
	"io"
	"strings"
	"testing"
)

// Methodology: glossary tests guard a deliberately small annotation
// surface. The plan's original Step 3 listed twelve terms; the wave-3
// Norman audit trimmed that to genuine jargon only (eight terms) and
// added an explicit anti-clutter test that asserts self-explanatory
// status words are NOT annotated. These tests freeze that decision so
// future authors can't drift back to "annotate everything" without
// failing CI.

func TestGlossary_jargonTermsHaveDefinitions(t *testing.T) {
	required := []string{
		"DLQ", "lease", "trigger",
		"p50", "p95", "p99",
		"KV", "SSE",
	}
	const minDefinitionLen = 20
	if len(required) == 0 {
		t.Fatalf("required terms list is empty; test is meaningless")
	}
	for _, term := range required {
		text, ok := GlossaryTooltip(term)
		if !ok {
			t.Errorf("missing glossary entry for %q", term)
			continue
		}
		if len(text) < minDefinitionLen {
			t.Errorf("glossary entry %q too short (%d chars, want >= %d)",
				term, len(text), minDefinitionLen)
		}
	}
}

func TestGlossary_statusWordsAreNOTAnnotated(t *testing.T) {
	// These terms must NOT be in the glossary. Operators know what
	// "running" or "failed" mean; annotating them adds clutter
	// without information. This test guards against drift.
	forbidden := []string{
		"running", "failed", "completed",
		"pending", "cancelled", "skipped",
		"queued",
	}
	if len(forbidden) == 0 {
		t.Fatalf("forbidden list is empty; test is meaningless")
	}
	for _, term := range forbidden {
		text, ok := GlossaryTooltip(term)
		if ok {
			t.Errorf("status word %q must NOT be glossary-annotated "+
				"(got definition %q) — see Norman's "+
				"signifiers > tooltips rule", term, text)
		}
	}
}

func TestTooltipHelper_renderTooltipMarkup(t *testing.T) {
	helper := tooltipHelper()
	if helper == nil {
		t.Fatalf("tooltipHelper returned nil")
	}
	out := helper("DLQ")
	s := string(out)
	wantSubstrings := []string{
		`class="glo-tooltip-wrapper"`,
		`tabindex="0"`,
		`>DLQ<`,
		`Dead-letter queue`,
		`role="tooltip"`,
	}
	if len(s) == 0 {
		t.Fatalf("tooltip helper produced empty output")
	}
	for _, sub := range wantSubstrings {
		if !strings.Contains(s, sub) {
			t.Errorf("tooltip output missing %q in:\n%s", sub, s)
		}
	}
}

func TestTooltipHelper_unknownTermFallsBackToPlainLabel(t *testing.T) {
	helper := tooltipHelper()
	if helper == nil {
		t.Fatalf("tooltipHelper returned nil")
	}
	out := helper("not-a-jargon-term")
	s := string(out)
	if strings.Contains(s, "glo-tooltip-wrapper") {
		t.Errorf("unknown term should render plain label, got %q", s)
	}
	if !strings.Contains(s, "not-a-jargon-term") {
		t.Errorf("unknown term label missing from output %q", s)
	}
}

// TestNavDLQIsPlainLink asserts the DLQ + KV nav items are plain
// anchors after the B3 nav/IA pass — the glo-tooltip-wrapper
// mystery-meat (help-cursor + hover popover, a broken signifier
// versus every other nav item) was removed. The glossary tooltip
// still lives on in-page headers; it just no longer wraps nav links.
func TestNavDLQIsPlainLink(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	if set == nil {
		t.Fatalf("templateSet is nil")
	}
	// Pick any page template so layout renders. The dashboard page
	// content is rendered inside layout so the nav (which is where
	// the DLQ tooltip lives) is exercised on every page.
	page := set.pageTemplates["dashboard"]
	if page == nil {
		// Fallback: walk for any defined page tree.
		for _, v := range set.pageTemplates {
			page = v
			break
		}
	}
	if page == nil {
		t.Fatalf("no page templates loaded")
	}
	data := minimalLayoutData()
	var buf strings.Builder
	if err := page.ExecuteTemplate(&buf, "layout", data); err != nil {
		t.Fatalf("execute layout: %v", err)
	}
	html := buf.String()
	// The nav must not wrap any link in the glossary tooltip mystery-meat.
	if strings.Contains(html, `glo-tooltip-wrapper`) {
		t.Errorf("nav still wraps a link in glo-tooltip-wrapper; got\n%s",
			truncateForLog(html))
	}
	// DLQ + KV must be present as plain anchors with the shared glyph link.
	for _, want := range []string{
		`href="/console/dlq"`,
		`href="/console/kv"`,
		"console-nav-glyph-link",
	} {
		if !strings.Contains(html, want) {
			t.Errorf("nav missing %q; got\n%s", want, truncateForLog(html))
		}
	}
}

// minimalLayoutData returns the smallest struct the layout template
// dereferences. Mirrors the layout's expectations (Title, Section,
// Actor, ReadOnly) so the render exercises the nav and the new tooltip
// without needing real page data.
func minimalLayoutData() map[string]any {
	return map[string]any{
		"Title":    "test",
		"Section":  "dashboard",
		"Actor":    map[string]any{"Display": "", "Source": ""},
		"ReadOnly": false,
		"Page":     map[string]any{},
	}
}

// truncateForLog trims rendered HTML so a failed assertion doesn't
// flood the test log. 2048 chars is long enough to see the nav region.
func truncateForLog(s string) string {
	const maxLen = 2048
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "…(truncated)"
}

// stub to keep imports honest if a future test needs to read templates
// directly without executing them — currently unused outside the
// helper file. Kept here so removing it shows up in review rather
// than leaking into production code.
var _ = template.HTML("")
var _ io.Writer = (*strings.Builder)(nil)
