// Methodology: table-driven HTML structure assertions. We parse the
// rendered output with x/net/html and assert selector presence, count,
// and key attributes — not exact byte strings. The e-ink palette and
// Basecoat classes will evolve; the structural contract (one row per
// step, data-step-id attribute, expanded-by-default for failed steps,
// nil-run-fallback to all-pending) must not.
package console

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/html"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
)

// TestStepList_rendersRowPerStep asserts that a 3-step def with a
// 3-step run renders 3 rows with the correct data-step-id anchors and
// surfaces the failed step's error message in the body.
func TestStepList_rendersRowPerStep(t *testing.T) {
	def := &dag.WorkflowDef{
		Name: "demo",
		Steps: []dag.StepDef{
			{ID: "fetch", Type: dag.StepTypeNormal},
			{ID: "transform", Type: dag.StepTypeNormal},
			{ID: "publish", Type: dag.StepTypeNormal},
		},
	}
	run := &dag.WorkflowRun{
		Steps: map[string]dag.StepState{
			"fetch":     {Status: dag.StepStatusCompleted, Attempts: 1},
			"transform": {Status: dag.StepStatusFailed, Attempts: 3, Error: "boom"},
			"publish":   {Status: dag.StepStatusPending},
		},
	}
	var buf bytes.Buffer
	if err := RenderStepList(&buf, def, run); err != nil {
		t.Fatalf("render: %v", err)
	}
	doc, err := html.Parse(&buf)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	rows := countNodesByClass(doc, "step-list-row")
	if rows != 3 {
		t.Errorf("step row count: got %d want 3", rows)
	}
	if !containsAttr(doc, "data-step-id", "transform") {
		t.Error("missing data-step-id=transform anchor")
	}
	if !containsAttr(doc, "data-step-id", "fetch") {
		t.Error("missing data-step-id=fetch anchor")
	}
	if !containsText(doc, "boom") {
		t.Error("failed step error 'boom' not rendered")
	}
}

// TestStepList_filtersEventsPerStep asserts BuildStepRows groups
// events under the step that produced them. The run-detail handler
// loads the full event stream once; BuildStepRows fans it out per
// row so each step's expanded body shows only its own events.
//
// Methodology: synthetic 2-step def with a 2-event-per-step event
// stream; positive (all events present) and negative (no cross-step
// leakage) checks.
func TestStepList_filtersEventsPerStep(t *testing.T) {
	def := &dag.WorkflowDef{
		Name:  "demo",
		Steps: []dag.StepDef{{ID: "a"}, {ID: "b"}},
	}
	run := &dag.WorkflowRun{
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted},
			"b": {Status: dag.StepStatusFailed, Error: "oops"},
		},
	}
	now := time.Now()
	events := []api.RunEvent{
		{Seq: 1, StepID: "a", Type: "step.started",
			Timestamp: now.Add(-4 * time.Second)},
		{Seq: 2, StepID: "a", Type: "step.completed",
			Timestamp: now.Add(-3 * time.Second)},
		{Seq: 3, StepID: "b", Type: "step.started",
			Timestamp: now.Add(-2 * time.Second)},
		{Seq: 4, StepID: "b", Type: "step.failed",
			Timestamp: now.Add(-1 * time.Second)},
	}
	rows := BuildStepRows(def, run, events, nil, nil)
	if len(rows) != 2 {
		t.Fatalf("rows: %d want 2", len(rows))
	}
	if len(rows[0].Events) != 2 {
		t.Errorf("a events: %d want 2", len(rows[0].Events))
	}
	if len(rows[1].Events) != 2 {
		t.Errorf("b events: %d want 2", len(rows[1].Events))
	}
	if rows[1].Events[1].Type != "step.failed" {
		t.Errorf("b last event: %s want step.failed",
			rows[1].Events[1].Type)
	}
	// Per-step duration derived from earliest→latest event timestamps.
	if rows[0].Duration == "" {
		t.Error("step a duration should be derived from events")
	}
}

// TestStepList_buildRowsHandlesNoEvents covers the partial-data path
// where the run snapshot exists but no events have been pulled (e.g.
// the user hit the page while the engine is still mid-write). Each
// row should still render with state + attempts; Duration is empty.
func TestStepList_buildRowsHandlesNoEvents(t *testing.T) {
	def := &dag.WorkflowDef{
		Name:  "demo",
		Steps: []dag.StepDef{{ID: "x"}},
	}
	run := &dag.WorkflowRun{
		Steps: map[string]dag.StepState{
			"x": {Status: dag.StepStatusCompleted, Attempts: 1},
		},
	}
	rows := BuildStepRows(def, run, nil, nil, nil)
	if len(rows) != 1 {
		t.Fatalf("rows: %d want 1", len(rows))
	}
	if rows[0].State != "completed" {
		t.Errorf("state: %q want completed", rows[0].State)
	}
	if rows[0].Duration != "" {
		t.Errorf("duration should be empty without events; got %q",
			rows[0].Duration)
	}
}

// TestComputeTimelineGeometry_offsetWidthFromEvents asserts the gantt
// geometry helper projects each step's first/last event onto a percent
// offset + width relative to the run window [runStart, runStart+total].
// Methodology: two steps with known event spans inside a 100s window;
// positive (correct percentages) and negative space (a step with no
// events gets zero geometry — the timeline falls back to the step list
// row rather than fabricating a bar).
func TestComputeTimelineGeometry_offsetWidthFromEvents(t *testing.T) {
	runStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	total := 100 * time.Second
	rows := []stepRow{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	events := []api.RunEvent{
		// step a spans 10s..30s -> offset 10%, width 20%.
		{StepID: "a", Timestamp: runStart.Add(10 * time.Second)},
		{StepID: "a", Timestamp: runStart.Add(30 * time.Second)},
		// step b spans 40s..90s -> offset 40%, width 50%.
		{StepID: "b", Timestamp: runStart.Add(40 * time.Second)},
		{StepID: "b", Timestamp: runStart.Add(90 * time.Second)},
		// step c: no events -> zero geometry.
	}
	out := computeTimelineGeometry(rows, events, runStart, total)
	if len(out) != 3 {
		t.Fatalf("rows: %d want 3", len(out))
	}
	if got := out[0].OffsetPct; got < 9.9 || got > 10.1 {
		t.Errorf("a offset = %v want ~10", got)
	}
	if got := out[0].WidthPct; got < 19.9 || got > 20.1 {
		t.Errorf("a width = %v want ~20", got)
	}
	if got := out[1].OffsetPct; got < 39.9 || got > 40.1 {
		t.Errorf("b offset = %v want ~40", got)
	}
	if got := out[1].WidthPct; got < 49.9 || got > 50.1 {
		t.Errorf("b width = %v want ~50", got)
	}
	// Negative space: stepless row keeps zero geometry (no fabricated bar).
	if out[2].OffsetPct != 0 || out[2].WidthPct != 0 {
		t.Errorf("c geometry = (%v,%v) want zero", out[2].OffsetPct, out[2].WidthPct)
	}
}

// TestComputeTimelineGeometry_zeroTotalIsHonest asserts a zero/negative
// run window yields zero geometry for every row — never a divide-by-zero
// or a fabricated full-width bar.
func TestComputeTimelineGeometry_zeroTotalIsHonest(t *testing.T) {
	runStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	rows := []stepRow{{ID: "a"}}
	events := []api.RunEvent{
		{StepID: "a", Timestamp: runStart.Add(time.Second)},
		{StepID: "a", Timestamp: runStart.Add(2 * time.Second)},
	}
	out := computeTimelineGeometry(rows, events, runStart, 0)
	if out[0].OffsetPct != 0 || out[0].WidthPct != 0 {
		t.Errorf("zero-total geometry = (%v,%v) want zero",
			out[0].OffsetPct, out[0].WidthPct)
	}
}

// TestStepList_handlesNilRun asserts that omitting the run entirely
// renders every step as pending — the same partial powers the static
// workflow-definition page where no run exists.
func TestStepList_handlesNilRun(t *testing.T) {
	def := &dag.WorkflowDef{
		Name:  "demo",
		Steps: []dag.StepDef{{ID: "only", Type: dag.StepTypeNormal}},
	}
	var buf bytes.Buffer
	if err := RenderStepList(&buf, def, nil); err != nil {
		t.Fatalf("render nil run: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "pending") {
		t.Error("nil run should default rows to pending state")
	}
	if !strings.Contains(out, `data-step-id="only"`) {
		t.Error("nil run should still emit one row per step")
	}
}

// countNodesByClass walks doc and counts elements whose class attribute
// contains the requested class token.
func countNodesByClass(doc *html.Node, class string) int {
	if doc == nil {
		panic("countNodesByClass: doc is nil")
	}
	if class == "" {
		panic("countNodesByClass: class is empty")
	}
	count := 0
	// Iterative DFS with explicit stack: avoids recursion per coding rules.
	const stackMax = 8192
	stack := make([]*html.Node, 0, 64)
	stack = append(stack, doc)
	for len(stack) > 0 && len(stack) <= stackMax {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n.Type == html.ElementNode && hasClass(n, class) {
			count++
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			stack = append(stack, c)
		}
	}
	return count
}

// hasClass checks whether an element node's class attribute contains
// the given token.
func hasClass(n *html.Node, class string) bool {
	if n == nil {
		panic("hasClass: n is nil")
	}
	if class == "" {
		panic("hasClass: class is empty")
	}
	for _, a := range n.Attr {
		if a.Key != "class" {
			continue
		}
		for _, tok := range strings.Fields(a.Val) {
			if tok == class {
				return true
			}
		}
	}
	return false
}

// containsAttr returns true if any node in doc has the given attribute
// key/value pair.
func containsAttr(doc *html.Node, key, val string) bool {
	if doc == nil {
		panic("containsAttr: doc is nil")
	}
	if key == "" {
		panic("containsAttr: key is empty")
	}
	const stackMax = 8192
	stack := make([]*html.Node, 0, 64)
	stack = append(stack, doc)
	for len(stack) > 0 && len(stack) <= stackMax {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n.Type == html.ElementNode {
			for _, a := range n.Attr {
				if a.Key == key && a.Val == val {
					return true
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			stack = append(stack, c)
		}
	}
	return false
}

// containsText returns true if any TextNode in doc contains the
// given substring.
func containsText(doc *html.Node, needle string) bool {
	if doc == nil {
		panic("containsText: doc is nil")
	}
	if needle == "" {
		panic("containsText: needle is empty")
	}
	const stackMax = 8192
	stack := make([]*html.Node, 0, 64)
	stack = append(stack, doc)
	for len(stack) > 0 && len(stack) <= stackMax {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if n.Type == html.TextNode && strings.Contains(n.Data, needle) {
			return true
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			stack = append(stack, c)
		}
	}
	return false
}
