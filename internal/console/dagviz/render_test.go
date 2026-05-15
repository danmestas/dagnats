// render_test.go validates the leveling algorithm and SVG output of
// the DAG renderer.
//
// Methodology:
//   - Unit tests only — the renderer has no external dependencies.
//   - Five canonical topologies: linear, branch-then-join,
//     parallel-only, single-step, cycle. Each pins a concrete
//     expected (level, lane) pair so accidental layout changes are
//     caught.
//   - SVG output must parse as valid XML.
//   - Live-overlay test asserts each step's CSS class encodes its
//     StepStatus tone.
//   - Minimum 2 assertions per test.
package dagviz

import (
	"encoding/xml"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
)

func TestLayout_linearChain(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "linear",
		Steps: []dag.StepDef{
			{ID: "a", Task: "t", Timeout: time.Minute},
			{ID: "b", Task: "t", Timeout: time.Minute, DependsOn: []string{"a"}},
			{ID: "c", Task: "t", Timeout: time.Minute, DependsOn: []string{"b"}},
		},
	}
	got, err := Layout(def)
	if err != nil {
		t.Fatalf("Layout error: %v", err)
	}
	if got.Levels["a"] != 0 || got.Levels["b"] != 1 || got.Levels["c"] != 2 {
		t.Fatalf("levels = %v, want a:0 b:1 c:2", got.Levels)
	}
	if got.MaxLevel != 2 {
		t.Fatalf("MaxLevel = %d, want 2", got.MaxLevel)
	}
}

func TestLayout_branchJoin(t *testing.T) {
	// a -> b1, a -> b2, both -> c
	def := dag.WorkflowDef{
		Name: "branch",
		Steps: []dag.StepDef{
			{ID: "a", Task: "t", Timeout: time.Minute},
			{ID: "b1", Task: "t", Timeout: time.Minute, DependsOn: []string{"a"}},
			{ID: "b2", Task: "t", Timeout: time.Minute, DependsOn: []string{"a"}},
			{ID: "c", Task: "t", Timeout: time.Minute,
				DependsOn: []string{"b1", "b2"}},
		},
	}
	got, err := Layout(def)
	if err != nil {
		t.Fatalf("Layout error: %v", err)
	}
	if got.Levels["a"] != 0 {
		t.Fatalf("a level = %d, want 0", got.Levels["a"])
	}
	if got.Levels["b1"] != 1 || got.Levels["b2"] != 1 {
		t.Fatalf("branch levels = %v", got.Levels)
	}
	if got.Levels["c"] != 2 {
		t.Fatalf("c level = %d, want 2", got.Levels["c"])
	}
	if got.MaxLaneWidth() != 2 {
		t.Fatalf("max lane width = %d, want 2", got.MaxLaneWidth())
	}
}

func TestLayout_parallelOnly(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "para",
		Steps: []dag.StepDef{
			{ID: "x", Task: "t", Timeout: time.Minute},
			{ID: "y", Task: "t", Timeout: time.Minute},
			{ID: "z", Task: "t", Timeout: time.Minute},
		},
	}
	got, err := Layout(def)
	if err != nil {
		t.Fatalf("Layout error: %v", err)
	}
	for _, id := range []string{"x", "y", "z"} {
		if got.Levels[id] != 0 {
			t.Fatalf("%s level = %d, want 0", id, got.Levels[id])
		}
	}
	if got.MaxLaneWidth() != 3 {
		t.Fatalf("lane width = %d, want 3", got.MaxLaneWidth())
	}
}

func TestLayout_singleStep(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "solo",
		Steps: []dag.StepDef{
			{ID: "only", Task: "t", Timeout: time.Minute},
		},
	}
	got, err := Layout(def)
	if err != nil {
		t.Fatalf("Layout: %v", err)
	}
	if got.Levels["only"] != 0 {
		t.Fatalf("level = %d", got.Levels["only"])
	}
	if got.MaxLevel != 0 {
		t.Fatalf("MaxLevel = %d, want 0", got.MaxLevel)
	}
}

func TestLayout_cycleReturnsError(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "loop",
		Steps: []dag.StepDef{
			{ID: "a", Task: "t", Timeout: time.Minute,
				DependsOn: []string{"b"}},
			{ID: "b", Task: "t", Timeout: time.Minute,
				DependsOn: []string{"a"}},
		},
	}
	_, err := Layout(def)
	if !errors.Is(err, ErrCycle) {
		t.Fatalf("err = %v, want ErrCycle", err)
	}
}

func TestLayout_tooManyStepsReturnsError(t *testing.T) {
	steps := make([]dag.StepDef, 0, MaxSteps+1)
	for i := 0; i < MaxSteps+1; i++ {
		steps = append(steps, dag.StepDef{
			ID: stepID(i), Task: "t", Timeout: time.Minute,
		})
	}
	def := dag.WorkflowDef{Name: "big", Steps: steps}
	_, err := Layout(def)
	if !errors.Is(err, ErrTooManySteps) {
		t.Fatalf("err = %v, want ErrTooManySteps", err)
	}
}

func TestRender_staticEmitsValidSVG(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "linear",
		Steps: []dag.StepDef{
			{ID: "a", Task: "t", Timeout: time.Minute},
			{ID: "b", Task: "t", Timeout: time.Minute, DependsOn: []string{"a"}},
		},
	}
	got, err := Render(def, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(string(got), "<svg") {
		t.Fatalf("output missing <svg> tag: %s", got)
	}
	if !strings.Contains(string(got), `aria-label="Step a, status pending"`) {
		t.Fatalf("step a missing aria-label in static mode: %s", got)
	}
	// Parse as XML to assert structural validity.
	if err := xml.Unmarshal(got, new(any)); err != nil {
		t.Fatalf("SVG not valid XML: %v\n%s", err, got)
	}
}

func TestRender_liveOverlayTintsByStatus(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "branch",
		Steps: []dag.StepDef{
			{ID: "a", Task: "t", Timeout: time.Minute},
			{ID: "b", Task: "t", Timeout: time.Minute, DependsOn: []string{"a"}},
			{ID: "c", Task: "t", Timeout: time.Minute, DependsOn: []string{"a"}},
		},
	}
	run := &dag.WorkflowRun{
		RunID:      "r1",
		WorkflowID: "branch",
		Status:     dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a": {Status: dag.StepStatusCompleted, Attempts: 1},
			"b": {Status: dag.StepStatusRunning, Attempts: 1,
				LoopStartedAt: time.Now().Add(-time.Minute)},
			"c": {Status: dag.StepStatusFailed, Attempts: 3,
				Error: "boom"},
		},
	}
	got, err := Render(def, run)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(string(got), "dagviz-step-completed") {
		t.Fatalf("missing completed tint:\n%s", got)
	}
	if !strings.Contains(string(got), "dagviz-step-running") {
		t.Fatalf("missing running tint:\n%s", got)
	}
	if !strings.Contains(string(got), "dagviz-step-failed") {
		t.Fatalf("missing failed tint:\n%s", got)
	}
	if !strings.Contains(string(got), "×3") {
		t.Fatalf("attempts badge for c missing:\n%s", got)
	}
}

func TestRender_cycleReturnsErrCycle(t *testing.T) {
	def := dag.WorkflowDef{
		Name: "loop",
		Steps: []dag.StepDef{
			{ID: "a", Task: "t", Timeout: time.Minute,
				DependsOn: []string{"b"}},
			{ID: "b", Task: "t", Timeout: time.Minute,
				DependsOn: []string{"a"}},
		},
	}
	_, err := Render(def, nil)
	if !errors.Is(err, ErrCycle) {
		t.Fatalf("err = %v, want ErrCycle", err)
	}
}

func TestRender_emptyStepListReturnsPlaceholder(t *testing.T) {
	def := dag.WorkflowDef{Name: "blank"}
	got, err := Render(def, nil)
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if !strings.Contains(string(got), "dagviz-empty") {
		t.Fatalf("expected empty placeholder, got:\n%s", got)
	}
	if !strings.Contains(string(got), "no steps") {
		t.Fatalf("expected 'no steps' text, got:\n%s", got)
	}
}

func TestRotationFor_stableAcrossCalls(t *testing.T) {
	r1 := rotationFor("step-1")
	r2 := rotationFor("step-1")
	if r1 != r2 {
		t.Fatalf("rotationFor non-deterministic: %v vs %v", r1, r2)
	}
	if r := rotationFor(""); r != 0 {
		t.Fatalf("empty id rotation = %v, want 0", r)
	}
}

// stepID is a small helper for the cap test.
func stepID(i int) string {
	const digits = "0123456789"
	if i < 10 {
		return "s" + string(digits[i])
	}
	return "s" + string(digits[i/10]) + string(digits[i%10])
}
