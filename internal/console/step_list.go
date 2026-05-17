// Step list partial: foundational primitive that will replace the DAG
// as the run-detail page's primary view (T04), reuses on the workflow
// definition page (static, no run), and inside the failed-run banner
// (single failed step, expanded). One renderer, three consumers.
//
// The partial is intentionally low-coupling: callers pass a dag.WorkflowDef
// and optionally a *dag.WorkflowRun. T04 will extend the renderer to
// accept per-step Input/Output/Events; today's surface is the structural
// minimum that satisfies T02's acceptance criteria.
package console

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"io"

	"github.com/danmestas/dagnats/dag"
)

//go:embed templates/components/step_list.html
var stepListFS embed.FS

var stepListTmpl = template.Must(
	template.ParseFS(stepListFS, "templates/components/step_list.html"),
)

// stepRow is one row of the rendered step list. Field names mirror
// the template; new optional columns added by T04 must keep this
// struct backward-compatible (empty string / zero / nil = render nothing).
type stepRow struct {
	ID                string
	Name              string
	State             string
	Icon              string
	Duration          string
	DurationLong      string
	RelativeStart     string
	Attempts          int
	Error             string
	Input             string
	Output            string
	Events            []stepEvent
	ExpandedByDefault bool
}

// stepEvent is one entry in a step's filtered event timeline. T04
// populates this slice; T02 leaves it nil.
type stepEvent struct {
	Time    string
	Type    string
	Summary string
}

// RenderStepList writes the step list HTML for def, overlaid with run
// state when run is non-nil. A nil run is valid: every step renders
// as pending. Returns an error if def is nil or template execution
// fails — callers are expected to fall back to a plain text section.
func RenderStepList(w io.Writer, def *dag.WorkflowDef, run *dag.WorkflowRun) error {
	if w == nil {
		panic("RenderStepList: writer is nil")
	}
	if def == nil {
		return fmt.Errorf("step list: nil definition")
	}
	rows := make([]stepRow, 0, len(def.Steps))
	for i := range def.Steps {
		rows = append(rows, buildStepRow(def.Steps[i], run))
	}
	var buf bytes.Buffer
	data := map[string]any{"Rows": rows}
	if err := stepListTmpl.ExecuteTemplate(&buf, "step-list", data); err != nil {
		return fmt.Errorf("step list render: %w", err)
	}
	if _, err := w.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("step list write: %w", err)
	}
	return nil
}

// buildStepRow projects one StepDef + matching run state to a stepRow.
// When run is nil, or the step has no state yet, the row defaults to
// pending. Failed rows are pre-expanded so the operator sees the
// error message without a click.
func buildStepRow(s dag.StepDef, run *dag.WorkflowRun) stepRow {
	if s.ID == "" {
		panic("buildStepRow: step id is empty")
	}
	r := stepRow{ID: s.ID, Name: s.ID, State: "pending", Icon: stepIcon("pending")}
	if run == nil {
		return r
	}
	state, ok := run.Steps[s.ID]
	if !ok {
		return r
	}
	r.State = state.Status.String()
	r.Icon = stepIcon(r.State)
	r.Attempts = state.Attempts
	r.Error = state.Error
	r.ExpandedByDefault = state.Status == dag.StepStatusFailed
	return r
}

// stepIcon maps a step state string to a single-character glyph.
// Mirrors statusIcon in pages.go but is scoped to the step list to
// keep the partial self-contained — when T04 lands, the two can
// converge.
func stepIcon(state string) string {
	if state == "" {
		panic("stepIcon: empty state")
	}
	switch state {
	case "completed":
		return "✓" // ✓
	case "running":
		return "●" // ●
	case "failed":
		return "✗" // ✗
	case "skipped", "cancelled":
		return "⊘" // ⊘
	default:
		return "○" // ○
	}
}
