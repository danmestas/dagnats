// Methodology: red-green TDD against the borderless-card contract.
// The MagicPath prototype renders content panels as borderless,
// radiused, surface-filled blocks — NO outer 1px frame and NO colored
// left-accent bar — with section titles as quiet uppercase labels
// rather than teal-tinted header bands. These tests read app.css and
// assert each content card/banner block has shed its border/accent,
// while deliberate functional treatments (the read-only mode banner,
// the transient toast stripe, table separators, focus outlines) stay
// intact. Positive space: borderless rules present. Negative space:
// the removed accents/bands are absent from the relevant blocks.
package console

import (
	"io/fs"
	"regexp"
	"strings"
	"testing"
)

// cssBlock returns the declaration body for the first rule whose
// selector list exactly starts with sel followed by ` {`. It is a
// deliberately small scanner — enough to assert a single block's
// properties without pulling in a full CSS parser.
func cssBlock(t *testing.T, css, sel string) string {
	t.Helper()
	re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(sel) + `\s*\{`)
	loc := re.FindStringIndex(css)
	if loc == nil {
		t.Fatalf("app.css: selector %q not found", sel)
	}
	rest := css[loc[1]:]
	end := strings.IndexByte(rest, '}')
	if end < 0 {
		t.Fatalf("app.css: unterminated block for %q", sel)
	}
	return rest[:end]
}

func TestAppCSS_contentCardsAreBorderless(t *testing.T) {
	body, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	css := string(body)

	// Content cards / banners / tiles that must be borderless filled:
	// surface bg + border-radius, but no 1px outer frame and no
	// colored left-accent bar.
	borderless := []string{
		".alert",
		".console-step-card",
		".config-endpoint-card",
		".config-jetstream-pane",
		".config-trigger-card",
		".logs-table-card",
	}
	for _, sel := range borderless {
		block := cssBlock(t, css, sel)
		if strings.Contains(block, "border:") &&
			strings.Contains(block, "1px solid var(--border)") {
			t.Errorf("%s still has a 1px outer border (should be borderless)", sel)
		}
		if strings.Contains(block, "border-left") {
			t.Errorf("%s still has a colored left-accent bar", sel)
		}
		if !strings.Contains(block, "border-radius") {
			t.Errorf("%s lost its border-radius (must stay radiused)", sel)
		}
	}

	// The run-error banner base (.alert) must not paint a left accent
	// via the status modifiers either. The modifier rules may be dropped
	// entirely (no rule == no accent); if present, they must not draw a
	// left-accent bar.
	for _, sel := range []string{".alert-warning", ".alert-error"} {
		if strings.Contains(css, sel+" {") || strings.Contains(css, sel+"   {") {
			if strings.Contains(cssBlock(t, css, sel), "border-left") {
				t.Errorf("%s still carries a left-accent bar", sel)
			}
		}
	}

	// .console-step-card status classes must not re-introduce a colored
	// frame (border-color flips read as a card outline). Dropping the
	// rules entirely is the cleanest way to satisfy that; if a rule
	// survives it must not paint a border-color.
	for _, sel := range []string{
		".console-step-card.status-completed",
		".console-step-card.status-running",
		".console-step-card.status-failed",
		".console-step-card.status-pending",
	} {
		if strings.Contains(css, sel+" ") || strings.Contains(css, sel+"{") {
			if strings.Contains(cssBlock(t, css, sel), "border-color") {
				t.Errorf("%s still tints a card border (should be borderless)", sel)
			}
		}
	}
}

func TestAppCSS_cardHeaderIsQuietLabelNotBand(t *testing.T) {
	body, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	css := string(body)

	// The teal-tinted header band must be gone: no accent-soft fill on
	// the section header, and no bottom rule turning it into a band.
	header := cssBlock(t, css, ".card-header")
	if strings.Contains(header, "var(--accent-soft)") {
		t.Errorf(".card-header still fills with accent-soft (band, not quiet label)")
	}
	if strings.Contains(header, "border-bottom") {
		t.Errorf(".card-header still draws a bottom rule (reads as a band)")
	}

	// The title text renders as a quiet uppercase muted label.
	title := cssBlock(t, css, ".card-header h2")
	if !strings.Contains(title, "text-transform: uppercase") {
		t.Errorf(".card-header h2 is not an uppercase label")
	}
	if !strings.Contains(title, "var(--text-secondary)") {
		t.Errorf(".card-header h2 is not muted (--text-secondary)")
	}
	if !strings.Contains(title, "letter-spacing") {
		t.Errorf(".card-header h2 lacks letter-spacing of a quiet label")
	}
}

func TestAppCSS_readonlyBannerAndToastKeepTreatment(t *testing.T) {
	body, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	css := string(body)

	// Deliberate functional treatments are preserved: the read-only
	// mode warning banner keeps its border, and the transient toast
	// keeps its left stripe.
	if b := cssBlock(t, css, ".console-readonly-banner"); !strings.Contains(b, "border-left") {
		t.Errorf(".console-readonly-banner lost its deliberate left border")
	}
	if b := cssBlock(t, css, ".console-toast"); !strings.Contains(b, "border-left") {
		t.Errorf(".console-toast lost its transient left stripe")
	}
}

// TestAppCSS_overridesBasecoatStatusAccents guards the status-colored
// left-accent bars the Basecoat bundle paints on the run-detail error
// banner (.run-error-banner, 4px) and the inline step-error box
// (.step-error, 3px) — the exact bars from the failed-run-detail
// screenshot. Those rules live in basecoat.css, not app.css, so the
// borderless contract is satisfied by an app.css override (app.css loads
// after basecoat). Asserting the OLD .alert base rule alone missed them.
func TestAppCSS_overridesBasecoatStatusAccents(t *testing.T) {
	body, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	css := string(body)
	zero := regexp.MustCompile(`border-left:\s*0`)
	for _, sel := range []string{".run-error-banner", ".step-error"} {
		block := cssBlock(t, css, sel)
		if !zero.MatchString(block) {
			t.Errorf("%s: app.css must override the Basecoat status "+
				"left-accent with border-left:0 (got %q)", sel, block)
		}
	}
}
