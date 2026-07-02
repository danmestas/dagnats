// agent_runtime_legibility_test.go guards the /console/agents runtime
// cards against the dark-on-dark regression: the card header classes
// (workflow title, run/gen meta, and the budget dt/dd) shipped with NO
// style rules, so they inherited Basecoat's near-black --foreground and
// rendered invisible on the dark card. Each must carry an explicit,
// theme-aware color token. Reuses cssBlock from borderless_cards_test.go.
package console

import (
	"io/fs"
	"strings"
	"testing"
)

func TestAppCSS_agentRuntimeCardIsLegible(t *testing.T) {
	body, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	css := string(body)

	// Value text must read as primary; label/meta text as the muted
	// secondary token. cssBlock fatals if a selector is missing entirely
	// — which is itself the "shipped unstyled" guard.
	primary := map[string]string{
		".console-agent-runtime-title": "var(--text-primary)",
		".console-agent-budget dd":     "var(--text-primary)",
	}
	secondary := map[string]string{
		".console-agent-runtime-meta": "var(--text-secondary)",
		".console-agent-budget dt":    "var(--text-secondary)",
	}
	for sel, want := range primary {
		block := cssBlock(t, css, sel)
		if !strings.Contains(block, "color: "+want) {
			t.Errorf("%s must set color: %s (else it inherits near-black "+
				"--foreground, dark-on-dark); got %q", sel, want, block)
		}
	}
	for sel, want := range secondary {
		block := cssBlock(t, css, sel)
		if !strings.Contains(block, "color: "+want) {
			t.Errorf("%s must set color: %s; got %q", sel, want, block)
		}
	}
}
