// row_chevron_test.go pins the drill-affordance chevron contract on
// the three drillable list fragments (workflows, runs, triggers) and
// asserts it is ABSENT on the DLQ fragment, which uses inline
// Retry/Discard actions instead. See #319 (parent #274 R7).
//
// Methodology:
//   - Load the real template set via loadTemplates() and render each
//     fragment directly with hand-built view structs. No NATS, no HTTP,
//     no DataSource — templates are pure functions of their inputs and
//     the assertion target is exact HTML bytes.
//   - Two assertions per test minimum (positive + negative space).
//   - Each affected tbody is checked for: (a) row-chevron presence on
//     a populated row, and (b) bumped colspan on the empty-state row.
//   - DLQ tbody is checked for the OPPOSITE — chevron MUST NOT appear,
//     so the inline action affordance stays uncontested.
package console

import (
	"strings"
	"testing"
)

// TestRowChevron_workflowsTbody_populatedRowHasChevron pins the
// positive branch on the workflows fragment.
func TestRowChevron_workflowsTbody_populatedRowHasChevron(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	if set == nil || set.base == nil {
		t.Fatalf("templateSet missing base tree")
	}
	view := WorkflowsListView{
		Rows: []WorkflowRow{{Name: "alpha", Version: "v1"}},
	}
	html, err := renderFragment(set.base, "workflows-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment workflows-tbody: %v", err)
	}
	if !strings.Contains(html, `class="row-chevron-cell"`) {
		t.Errorf("workflows row must include row-chevron-cell; got\n%s", html)
	}
	if !strings.Contains(html, `class="row-chevron"`) {
		t.Errorf("workflows row must include row-chevron span; got\n%s", html)
	}
}

// TestRowChevron_workflowsTbody_emptyStateColspanBumped pins the
// empty-row colspan bump. The column count is 9: name, version,
// steps, triggers, last-run, status, activity sparkline, Run cell
// (#329), chevron.
func TestRowChevron_workflowsTbody_emptyStateColspanBumped(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := WorkflowsListView{Rows: nil}
	html, err := renderFragment(set.base, "workflows-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment workflows-tbody: %v", err)
	}
	if !strings.Contains(html, `colspan="9"`) {
		t.Errorf("workflows empty row must use colspan=9; got\n%s", html)
	}
	if strings.Contains(html, `colspan="8"`) {
		t.Errorf("workflows empty row must not use stale colspan=8; got\n%s", html)
	}
}

// TestRowChevron_runsTbody_populatedRowHasChevron pins the chevron on
// the runs fragment. Runs has an Inspect button but that's a non-destructive
// sidesheet — the chevron's drill affordance is still warranted.
func TestRowChevron_runsTbody_populatedRowHasChevron(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := RunsListView{
		Rows: []RunRow{{
			RunID:       "run-abc",
			RunIDShort:  "run-abc",
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
	if !strings.Contains(html, `class="row-chevron-cell"`) {
		t.Errorf("runs row must include row-chevron-cell; got\n%s", html)
	}
	if !strings.Contains(html, `class="row-chevron"`) {
		t.Errorf("runs row must include row-chevron span; got\n%s", html)
	}
}

// TestRowChevron_runsTbody_emptyStateColspanBumped pins the empty-row
// colspan bump from 7 → 8.
func TestRowChevron_runsTbody_emptyStateColspanBumped(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := RunsListView{Rows: nil}
	html, err := renderFragment(set.base, "runs-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment runs-tbody: %v", err)
	}
	if !strings.Contains(html, `colspan="8"`) {
		t.Errorf("runs empty row must use colspan=8; got\n%s", html)
	}
	if strings.Contains(html, `colspan="7"`) {
		t.Errorf("runs empty row must not use stale colspan=7; got\n%s", html)
	}
}

// TestRowChevron_triggersTbody_populatedRowHasChevron pins the chevron
// on the triggers fragment.
func TestRowChevron_triggersTbody_populatedRowHasChevron(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := TriggersListView{
		Rows: []TriggerRow{{
			ID:          "trg-1",
			Kind:        "cron",
			Target:      "*/5 * * * *",
			Workflow:    "alpha",
			Enabled:     true,
			StatusLabel: "active",
			StatusIcon:  "*",
		}},
	}
	html, err := renderFragment(set.base, "triggers-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment triggers-tbody: %v", err)
	}
	if !strings.Contains(html, `class="row-chevron-cell"`) {
		t.Errorf("triggers row must include row-chevron-cell; got\n%s", html)
	}
	if !strings.Contains(html, `class="row-chevron"`) {
		t.Errorf("triggers row must include row-chevron span; got\n%s", html)
	}
}

// TestRowChevron_triggersTbody_emptyStateColspanBumped pins the empty-row
// colspan bump tracking the column count. Bumped 6 → 7 when the
// sparkline col landed; bumped 7 → 8 when the Fire-now action col
// landed (#352).
func TestRowChevron_triggersTbody_emptyStateColspanBumped(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := TriggersListView{Rows: nil}
	html, err := renderFragment(set.base, "triggers-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment triggers-tbody: %v", err)
	}
	if !strings.Contains(html, `colspan="8"`) {
		t.Errorf("triggers empty row must use colspan=8; got\n%s", html)
	}
	if strings.Contains(html, `colspan="7"`) {
		t.Errorf("triggers empty row must not use stale colspan=7; got\n%s", html)
	}
}

// TestRowChevron_dlqTbody_absent is the regression guard: DLQ rows have
// inline Retry/Discard buttons; piling a chevron on top would muddle
// the action affordance. The dumb partial deliberately is NOT included
// in dlq_tbody.html — this test fails the build if anyone wires it in.
func TestRowChevron_dlqTbody_absent(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := DLQListView{
		ReadOnly:  false,
		CSRFToken: "test-token",
		Rows: []DLQRow{{
			Sequence:      42,
			ReasonShort:   "timeout",
			ReasonFull:    "deadline exceeded",
			Workflow:      "alpha",
			OriginalRunID: "run-x",
			FailedAt:      "2026-05-22T00:00:00Z",
			Attempts:      3,
			BodyPreserved: true,
		}},
	}
	html, err := renderFragment(set.base, "dlq-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment dlq-tbody: %v", err)
	}
	if strings.Contains(html, `class="row-chevron-cell"`) {
		t.Errorf("dlq row must NOT include row-chevron-cell (inline actions own the affordance); got\n%s", html)
	}
	if strings.Contains(html, `class="row-chevron"`) {
		t.Errorf("dlq row must NOT include row-chevron span; got\n%s", html)
	}
	// Positive control: confirm the inline actions are still present
	// so we know we rendered something meaningful (not an empty result).
	if !strings.Contains(html, `data-dlq-action="retry"`) {
		t.Errorf("dlq row should still contain retry action; got\n%s", html)
	}
}
