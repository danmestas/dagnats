// inspect_button_label_test.go pins an accessible name on the repeated
// runs "Inspect" buttons.
//
// Methodology:
//   - Every run row carries an "Inspect" button with identical visible
//     text. To a screen-reader user they are an undifferentiated list of
//     "Inspect, Inspect, Inspect…" — a Norman signifier failure: the
//     control gives no clue WHICH run it inspects. Pin an aria-label that
//     names the run by its short (12-char) id.
//   - Both the static runs-tbody row and the SSE run-row fragment carry
//     the same Inspect button, so both must carry the label to stay in
//     lockstep (a morphed live row must be as labelled as a static one).
//   - Render each fragment directly via loadTemplates + renderFragment.
//   - Two assertions per fragment: label present with the short id
//     (positive), and the label is non-empty (negative space against a
//     bare aria-label="").
package console

import (
	"strings"
	"testing"
)

// TestInspectButton_runsTbody_hasAriaLabel pins the accessible name on
// the static runs-tbody Inspect button. RED before the fix, GREEN after.
func TestInspectButton_runsTbody_hasAriaLabel(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := RunsListView{
		Rows: []RunRow{{
			RunID:       "abcdef012345-6789-aaaa",
			RunIDShort:  "abcdef012345",
			WorkflowID:  "alpha",
			Status:      "running",
			TriggerKind: "manual",
			StartedAt:   "2026-05-22T00:00:00Z",
			Duration:    "1s",
		}},
	}
	html, err := renderFragment(set.base, "runs-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment runs-tbody: %v", err)
	}
	if !strings.Contains(html, `aria-label="Inspect run abcdef012345"`) {
		t.Errorf("static Inspect button must name the run via aria-label; got\n%s", html)
	}
	if strings.Contains(html, `aria-label=""`) {
		t.Errorf("Inspect button aria-label must not be empty; got\n%s", html)
	}
}

// TestInspectButton_sseRunRow_hasAriaLabel pins the same accessible
// name on the SSE-emitted run-row Inspect button so live-updated rows
// match the static ones.
func TestInspectButton_sseRunRow_hasAriaLabel(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	html, err := renderFragment(set.base, "run-row", rowPatch{
		Row: RunRow{
			RunID:       "abcdef012345-6789-aaaa",
			RunIDShort:  "abcdef012345",
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
	if !strings.Contains(html, `aria-label="Inspect run abcdef012345"`) {
		t.Errorf("SSE Inspect button must name the run via aria-label; got\n%s", html)
	}
	if strings.Contains(html, `aria-label=""`) {
		t.Errorf("SSE Inspect button aria-label must not be empty; got\n%s", html)
	}
}
