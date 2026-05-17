// Step list partial: foundational primitive that replaces the DAG as
// the run-detail page's primary view (T04), reuses on the workflow
// definition page (static, no run), and inside the failed-run banner
// (single failed step, expanded). One renderer, three consumers.
//
// The partial is intentionally low-coupling: callers pass a
// dag.WorkflowDef and optionally a *dag.WorkflowRun. T04 extended the
// renderer with BuildStepRows that takes pre-loaded events plus
// (optional) per-step input/output blobs; durations are derived from
// the event stream because dag.StepState does not carry a duration.
package console

import (
	"bytes"
	"embed"
	"fmt"
	"html/template"
	"io"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
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

// BuildStepRows assembles rows for the step list, filtering events
// per step and overlaying caller-supplied I/O. Inputs / outputs are
// optional maps keyed by step id; either may be nil (initial render
// passes nil; the io-tab handler fills them on demand).
//
// Per-step duration is computed on the fly from the earliest and
// latest event timestamps for that step — dag.StepState has no
// duration field, and computing it here keeps the dag package pure.
// Empty event slice → empty duration, which the template treats as
// "no duration available" (the column simply hides).
func BuildStepRows(
	def *dag.WorkflowDef, run *dag.WorkflowRun,
	events []api.RunEvent,
	inputs, outputs map[string]string,
) []stepRow {
	if def == nil {
		panic("BuildStepRows: def is nil")
	}
	if len(def.Steps) == 0 {
		return nil
	}
	eventsByStep := groupEventsByStep(events)
	rows := make([]stepRow, 0, len(def.Steps))
	for i := range def.Steps {
		row := buildStepRow(def.Steps[i], run)
		row.Events = eventsByStep[row.ID]
		row.Duration, row.DurationLong =
			durationFromEvents(events, row.ID)
		if inputs != nil {
			row.Input = inputs[row.ID]
		}
		if outputs != nil {
			row.Output = outputs[row.ID]
		}
		rows = append(rows, row)
	}
	return rows
}

// groupEventsByStep partitions the run's event stream by step id.
// Events without a step id (workflow-level: started/completed) are
// skipped — they belong to the timeline, not any single row.
// Bounded by the caller's slice length; one allocation per step that
// produced at least one event.
func groupEventsByStep(events []api.RunEvent) map[string][]stepEvent {
	if events == nil {
		return nil
	}
	const eventsMax = 100_000 // safety bound per coding rules
	out := make(map[string][]stepEvent, 8)
	for i := 0; i < len(events) && i < eventsMax; i++ {
		e := events[i]
		if e.StepID == "" {
			continue
		}
		out[e.StepID] = append(out[e.StepID], stepEvent{
			Time:    e.Timestamp.UTC().Format("15:04:05"),
			Type:    e.Type,
			Summary: stepEventSummary(e.Data),
		})
	}
	return out
}

// stepEventSummary trims the raw event payload to a short preview.
// The full payload is available on the Events tab; the per-step row
// just needs a hint. Empty payloads return empty string so the
// template renders only the type label.
func stepEventSummary(data string) string {
	const previewMax = 80
	if len(data) == 0 {
		return ""
	}
	if len(data) <= previewMax {
		return data
	}
	return data[:previewMax]
}

// durationFromEvents derives the elapsed time for a step from the
// earliest and latest event with that step id. Returns ("", "") when
// the step has zero or one event (no measurable span).
//
// Why event-derived: dag.StepState carries Attempts + Error + Output,
// but not a duration. The engine emits step.queued / step.started /
// step.completed / step.failed events with timestamps; the spread of
// those marks is the most-accurate duration we can surface without
// extending the dag schema.
func durationFromEvents(
	events []api.RunEvent, stepID string,
) (string, string) {
	if stepID == "" {
		panic("durationFromEvents: stepID is empty")
	}
	if len(events) == 0 {
		return "", ""
	}
	var first, last time.Time
	const eventsMax = 100_000
	for i := 0; i < len(events) && i < eventsMax; i++ {
		e := events[i]
		if e.StepID != stepID {
			continue
		}
		if first.IsZero() || e.Timestamp.Before(first) {
			first = e.Timestamp
		}
		if last.IsZero() || e.Timestamp.After(last) {
			last = e.Timestamp
		}
	}
	if first.IsZero() || last.IsZero() || !last.After(first) {
		return "", ""
	}
	d := last.Sub(first)
	return humanizeDuration(d), fmt.Sprintf("%dms", d.Milliseconds())
}

// humanizeDuration renders a duration in a compact operator-friendly
// form. Mirrors formatDuration in pages.go but is scoped to the step
// list so the package can later split if needed. < 1s → ms, < 1m → s,
// else m / s.
func humanizeDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	mins := int(d.Minutes())
	secs := int(d.Seconds()) - mins*60
	return fmt.Sprintf("%dm%ds", mins, secs)
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
