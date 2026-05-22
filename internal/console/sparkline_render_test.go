// sparkline_render_test.go pins the per-row sparkline cell rendering
// contract for the workflows-tbody and triggers-tbody fragments.
//
// Methodology:
//   - Load the real template set via loadTemplates() and render each
//     fragment directly with hand-built view structs. No NATS, no HTTP,
//     no DataSource — the templates are pure functions of their inputs
//     and the assertion target is exact HTML bytes.
//   - For each fragment we check both branches of the .Sparkline guard:
//     (a) row with a populated []float64 must produce a <canvas …
//     class="console-sparkline" … >, and (b) row with nil Sparkline
//     must produce an empty <td class="console-sparkline-col"></td>
//     — no &middot;, no placeholder dot, no muted span. The latter
//     is the per-row honesty contract from #304, sibling to #284's
//     per-tile dashboard cleanup.
//   - Two assertions per test minimum (positive + negative space).
package console

import (
	"strings"
	"testing"
)

// TestSparklineCell_workflowsTbody_nilSparklineRendersEmpty pins the
// per-row empty contract: when a WorkflowRow has Sparkline == nil, the
// fragment must NOT emit the muted-dot placeholder. The cell stays
// empty (no glyph, no aria-label, no span).
func TestSparklineCell_workflowsTbody_nilSparklineRendersEmpty(t *testing.T) {
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
	// Positive space: empty cell present.
	if !strings.Contains(html, `<td class="console-sparkline-col"></td>`) {
		t.Errorf("nil Sparkline workflow row should render empty cell; got\n%s",
			html)
	}
	// Negative space: no muted-dot placeholder of any flavor.
	for _, leak := range []string{
		"&middot;",
		`aria-label="no activity data"`,
		`<span class="muted" aria-label="no activity data"`,
	} {
		if strings.Contains(html, leak) {
			t.Errorf("workflow row leaked placeholder %q in\n%s",
				leak, html)
		}
	}
}

// TestSparklineCell_workflowsTbody_dataRendersCanvas pins the populated
// branch: when SparklineData returns a real []float64, the canvas
// element must render unchanged (data-sparkline-id + data-sparkline-data
// both set).
func TestSparklineCell_workflowsTbody_dataRendersCanvas(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := WorkflowsListView{
		Rows: []WorkflowRow{{
			Name:      "beta",
			Version:   "v2",
			Sparkline: []float64{1, 2, 3},
		}},
	}
	html, err := renderFragment(set.base, "workflows-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment workflows-tbody: %v", err)
	}
	if !strings.Contains(html, `<canvas class="console-sparkline"`) {
		t.Errorf("populated Sparkline should render canvas; got\n%s", html)
	}
	if !strings.Contains(html, `data-sparkline-id="workflow-beta"`) {
		t.Errorf("canvas should carry per-row id; got\n%s", html)
	}
}

// TestSparklineCell_triggersTbody_nilSparklineRendersEmpty mirrors the
// workflow-row test on the triggers fragment: nil Sparkline must yield
// an empty <td>, no muted-dot.
func TestSparklineCell_triggersTbody_nilSparklineRendersEmpty(t *testing.T) {
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
	if !strings.Contains(html, `<td class="console-sparkline-col"></td>`) {
		t.Errorf("nil Sparkline trigger row should render empty cell; got\n%s",
			html)
	}
	for _, leak := range []string{
		"&middot;",
		`aria-label="no activity data"`,
		`<span class="muted" aria-label="no activity data"`,
	} {
		if strings.Contains(html, leak) {
			t.Errorf("trigger row leaked placeholder %q in\n%s",
				leak, html)
		}
	}
}

// TestSparklineCell_triggersTbody_dataRendersCanvas pins the populated
// branch on the triggers fragment.
func TestSparklineCell_triggersTbody_dataRendersCanvas(t *testing.T) {
	set, err := loadTemplates()
	if err != nil {
		t.Fatalf("loadTemplates: %v", err)
	}
	view := TriggersListView{
		Rows: []TriggerRow{{
			ID:          "trg-2",
			Kind:        "http",
			Target:      "POST /hooks/x",
			Workflow:    "beta",
			Enabled:     true,
			StatusLabel: "active",
			StatusIcon:  "*",
			Sparkline:   []float64{4, 5, 6},
		}},
	}
	html, err := renderFragment(set.base, "triggers-tbody", view)
	if err != nil {
		t.Fatalf("renderFragment triggers-tbody: %v", err)
	}
	if !strings.Contains(html, `<canvas class="console-sparkline"`) {
		t.Errorf("populated Sparkline should render canvas; got\n%s", html)
	}
	if !strings.Contains(html, `data-sparkline-id="trigger-trg-2"`) {
		t.Errorf("canvas should carry per-row id; got\n%s", html)
	}
}
