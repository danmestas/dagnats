// sse_run_row_test.go pins the click-navigation contract on the SSE
// run-row FRAGMENT (the one emitted by emitRunPatch over the live
// runs feed), keeping it in lockstep with the statically-rendered
// runs-tbody row.
//
// Methodology:
//   - Load the real template set via loadTemplates() and render the
//     "run-row" define directly with a rowPatch struct — the exact
//     binding emitRunPatch passes. Pure template render: no NATS,
//     no HTTP, no DataSource.
//   - The static runs-tbody row carries data-href="/console/runs/<id>"
//     plus a row-chevron-cell <td> so the whole row is click-navigable
//     (Batch #473). A morphed SSE row that lacks these silently breaks
//     click-nav. Assert the SSE fragment now matches.
//   - Two assertions: data-href present with the run id (positive), and
//     the row-chevron-cell present (positive).
package console

import (
	"strings"
	"testing"
)

// TestSSERunRow_carriesDataHrefAndChevron pins the SSE-emitted run row
// against the static row: both must carry data-href and a chevron cell
// so a live-updated row is click-navigable like a statically rendered
// one. RED before the fix (run_row.html lacked both), GREEN after.
func TestSSERunRow_carriesDataHrefAndChevron(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	if set == nil || set.base == nil {
		t.Fatalf("templateSet missing base tree")
	}
	html, err := renderFragment(set.base, "run-row", rowPatch{
		Row: RunRow{
			RunID:       "run-abc-def",
			RunIDShort:  "run-abc-d",
			WorkflowID:  "alpha",
			Status:      "running",
			TriggerKind: "manual",
			StartedAt:   "2026-05-22T00:00:00Z",
			Duration:    "1s",
		},
		Fresh: true,
	})
	if err != nil {
		t.Fatalf("renderFragment run-row: %v", err)
	}
	if !strings.Contains(html, `data-href="/console/runs/run-abc-def"`) {
		t.Errorf("SSE run row must carry data-href for click-nav; got\n%s", html)
	}
	if !strings.Contains(html, `class="row-chevron-cell"`) {
		t.Errorf("SSE run row must include row-chevron-cell to match static row; got\n%s", html)
	}
}
