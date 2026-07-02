// trigger_icon_test.go pins the trigger-kind icon rendering: the
// console must render proper inline SVG icons, never emoji glyphs.
//
// Methodology:
//   - triggerKindSVG is pure, so drive it directly.
//   - Positive: each known kind returns an <svg> that strokes with
//     currentColor (so the per-kind .trigger-icon color rules apply).
//   - Negative space: the output must contain no emoji glyph and each
//     kind's icon must be distinct from the others.
package console

import (
	"strings"
	"testing"
)

func TestTriggerKindGlyph_rendersSVGNotEmoji(t *testing.T) {
	// The emoji the console used to render — none may survive.
	oldEmoji := []string{"⏱", "↘", "📡", "⤴", "•"}
	kinds := []string{"cron", "webhook", "http", "subject", "unknown-kind"}

	seen := make(map[string]bool, len(kinds))
	for _, kind := range kinds {
		svg := triggerKindSVG(kind)
		if !strings.HasPrefix(svg, "<svg") || !strings.HasSuffix(svg, "</svg>") {
			t.Errorf("kind %q: want inline <svg>…</svg>, got %q", kind, svg)
		}
		if !strings.Contains(svg, `stroke="currentColor"`) {
			t.Errorf("kind %q: icon must stroke currentColor for theming", kind)
		}
		for _, e := range oldEmoji {
			if strings.Contains(svg, e) {
				t.Errorf("kind %q: emoji %q must not appear in icon", kind, e)
			}
		}
		seen[svg] = true
	}
	// Each of the four known kinds + the fallback must be visually
	// distinct (5 unique SVG strings).
	if len(seen) != len(kinds) {
		t.Errorf("distinct icons = %d, want %d (kinds collided)",
			len(seen), len(kinds))
	}

	// The template-facing wrapper returns the same markup as HTML.
	if string(triggerKindGlyph("cron")) != triggerKindSVG("cron") {
		t.Error("triggerKindGlyph must wrap triggerKindSVG unchanged")
	}
}
