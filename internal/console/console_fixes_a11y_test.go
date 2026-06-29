// console_fixes_a11y_test.go covers a batch of small console
// correctness + accessibility fixes that share the page-render test
// harness (newFakeDS + getPage from page_header_test.go).
//
// Methodology:
//   - Each test renders a real page through the handler with a seeded
//     fakeDataSource and asserts on stable substrings the template
//     emits (positive space) plus the old/wrong string is gone
//     (negative space).
//   - The tooltip a11y test exercises the Go helpers (tooltipAsHelper /
//     tooltipHelper) directly so the derived id + aria wiring is
//     asserted at the source, independent of any one page's markup.
//   - Tests never share state; each builds its own Mount via getPage.
package console

import (
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// TestRunsListFindPlaceholder_saysSubstring asserts the find-by-id input
// on /console/runs tells the operator it filters the table on any part
// of a run id. This supersedes the old "full run UUID" copy (M2): the
// find box now drives a substring filter over the runs LIST rather than
// navigating to a single detail page, so a partial id is valid input.
func TestRunsListFindPlaceholder_saysSubstring(t *testing.T) {
	fake := newFakeDS()
	fake.runs = []dag.WorkflowRun{
		{RunID: "11111111-2222-3333-4444-555555555555",
			WorkflowID: "alpha", Status: dag.RunStatusRunning,
			CreatedAt: time.Now()},
	}
	body := getPage(t, fake, "/console/runs")

	if !strings.Contains(body, "any part of a run id") {
		t.Errorf("runs find placeholder missing substring-filter hint")
	}
	// Negative space: the old full-UUID copy must be gone now that the
	// box filters on substrings instead of demanding an exact id.
	if strings.Contains(body, "full run UUID") {
		t.Errorf("runs find placeholder still uses the old full-UUID copy")
	}
}

// TestConsumersTable_wrappedInCard asserts the consumers table sits
// inside a .card so the shared `card { overflow-x:auto }` rule gives the
// wide table a horizontal scrollbar instead of clipping at the viewport
// edge (N3). streams/dlq already wrap their tables this way.
func TestConsumersTable_wrappedInCard(t *testing.T) {
	fake := newFakeDS()
	fake.consumers = []ConsumerRow{
		{Stream: "TASK_QUEUES", Name: "wkr-img",
			Filter: "task.image-pipeline.>", AckPolicy: "explicit"},
	}
	body := getPage(t, fake, "/console/consumers")

	idx := strings.Index(body, `id="consumers-table"`)
	if idx < 0 {
		t.Fatalf("consumers page missing the table")
	}
	// The nearest enclosing card div must open before the table.
	cardIdx := strings.LastIndex(body[:idx], `class="card"`)
	if cardIdx < 0 {
		t.Errorf("consumers table is not wrapped in a .card")
	}
}

// TestConnectionsHeader_pendingUnitQualified asserts the connections
// table's Pending column header names its unit (bytes), because the cell
// renders a byte count, not an item count (m10).
func TestConnectionsHeader_pendingUnitQualified(t *testing.T) {
	fake := newFakeDS()
	fake.connections = []ConnRow{
		{CID: 7, Name: "dagnats-engine", Kind: "Client", Lang: "go"},
	}
	body := getPage(t, fake, "/console/connections")

	if !strings.Contains(body, "Pending (bytes)") {
		t.Errorf("connections header missing the unit-qualified %q",
			"Pending (bytes)")
	}
	// Negative space: the bare <th>Pending</th> must be gone.
	if strings.Contains(body, "<th>Pending</th>") {
		t.Errorf("connections header still uses the bare unqualified %q",
			"<th>Pending</th>")
	}
}

// TestTooltipHelper_emitsAccessibleWiring asserts the glossary tooltip
// helpers wire the popover and wrapper for screen readers (m6-D3,
// WCAG 2.4.7 + 4.1.2): the popover carries a stable non-empty id, the
// wrapper's aria-describedby points at that id, and aria-label carries
// the visible label text.
func TestTooltipHelper_emitsAccessibleWiring(t *testing.T) {
	asHelper := tooltipAsHelper()
	html := string(asHelper("Triggers", "trigger"))

	id := extractAttr(t, html, "glo-tooltip-popover", "id")
	if id == "" {
		t.Fatalf("popover id is empty: %q", html)
	}
	if got := extractWrapperAttr(t, html, "aria-describedby"); got != id {
		t.Errorf("aria-describedby = %q, want popover id %q", got, id)
	}
	if got := extractWrapperAttr(t, html, "aria-label"); got != "Triggers" {
		t.Errorf("aria-label = %q, want %q", got, "Triggers")
	}

	// Determinism: rendering the same term twice yields the same id so
	// snapshots stay stable (no counter / no randomness).
	again := string(asHelper("Triggers", "trigger"))
	if id2 := extractAttr(t, again, "glo-tooltip-popover", "id"); id2 != id {
		t.Errorf("popover id not deterministic: %q vs %q", id, id2)
	}

	// Distinct terms get distinct ids so two tooltips on one page never
	// collide. "lease" is the other glossary entry alongside "trigger".
	other := string(tooltipHelper()("lease"))
	if otherID := extractAttr(t, other, "glo-tooltip-popover", "id"); otherID == id {
		t.Errorf("distinct terms share an id %q", otherID)
	}
}

// extractAttr returns the value of attr on the element carrying the
// given class marker. Test-local string scan; the markup is small and
// stable so a full HTML parser is unwarranted.
func extractAttr(t *testing.T, html, classMarker, attr string) string {
	t.Helper()
	classIdx := strings.Index(html, classMarker)
	if classIdx < 0 {
		t.Fatalf("class %q not found in %q", classMarker, html)
	}
	// Search for attr after the class marker, within the same tag.
	tagEnd := strings.IndexByte(html[classIdx:], '>')
	if tagEnd < 0 {
		t.Fatalf("no tag close after %q", classMarker)
	}
	return attrValue(html[classIdx:classIdx+tagEnd], attr)
}

// extractWrapperAttr returns the value of attr on the
// glo-tooltip-wrapper opening tag.
func extractWrapperAttr(t *testing.T, html, attr string) string {
	t.Helper()
	return extractAttr(t, html, "glo-tooltip-wrapper", attr)
}

// attrValue pulls `attr="..."` out of a single tag's text.
func attrValue(tag, attr string) string {
	needle := attr + `="`
	i := strings.Index(tag, needle)
	if i < 0 {
		return ""
	}
	rest := tag[i+len(needle):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}
