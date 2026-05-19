package dagviz

import (
	"bytes"
	"fmt"
	"hash/fnv"
	"html"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// render.go converts a LayoutResult + WorkflowDef into inline SVG.
// The look is deliberately e-ink + hand-drawn-feel: paper-grain fill,
// slight rotation per step seeded by id, muted accent colours for
// status. Run-state overlay is optional — Render with run=nil produces
// the static workflow-detail DAG, run!=nil produces the live run-
// detail DAG.

// Render returns inline SVG bytes for def. When run is nil, the SVG
// shows the static definition (every step in the "pending" tint). When
// run is non-nil, each step is tinted by StepStatus from run.Steps.
// On ErrCycle / ErrTooManySteps callers should render their own
// fallback markup; this function returns those errors unchanged.
func Render(
	def dag.WorkflowDef, run *dag.WorkflowRun,
) ([]byte, error) {
	if def.Name == "" {
		return nil, fmt.Errorf("render: workflow def has no name")
	}
	layout, err := Layout(def)
	if err != nil {
		return nil, err
	}
	if len(def.Steps) == 0 {
		return []byte(emptySVG(def.Name)), nil
	}
	g := newGeometry(layout)
	var buf bytes.Buffer
	g.openSVG(&buf, def.Name)
	g.drawDefs(&buf)
	g.drawEdges(&buf, def)
	g.drawNodes(&buf, def, run)
	buf.WriteString("</svg>")
	return buf.Bytes(), nil
}

// emptySVG is the static placeholder for a 0-step workflow definition.
// Kept tiny so the surrounding chrome (workflow header) still pads to
// the expected DAG height.
func emptySVG(name string) string {
	const w, h = 320, 80
	return fmt.Sprintf(
		`<svg xmlns="http://www.w3.org/2000/svg" `+
			`viewBox="0 0 %d %d" role="img" `+
			`aria-label="Workflow %s has no steps" `+
			`class="dagviz dagviz-empty">`+
			`<text x="160" y="44" text-anchor="middle" `+
			`class="dagviz-empty-text">no steps</text>`+
			`</svg>`,
		w, h, html.EscapeString(name),
	)
}

// geometry holds the pixel dimensions derived from a LayoutResult.
// All measurements are in SVG user units (1u = 1px at default zoom).
type geometry struct {
	layout          LayoutResult
	stepW, stepH    float64
	gapX, gapY      float64
	padX, padY      float64
	headerH         float64
	canvasW         float64
	canvasH         float64
	connectorIndent float64
}

// newGeometry computes the per-render layout. The values below come
// from the design notes: 180x64 step boxes, 48px horizontal gap, 24px
// vertical gap, 40px header strip, 32px padding all around.
func newGeometry(layout LayoutResult) geometry {
	const (
		stepW   = 180.0
		stepH   = 64.0
		gapX    = 48.0
		gapY    = 24.0
		padX    = 32.0
		padY    = 32.0
		headerH = 0.0
		indent  = 4.0
	)
	levels := float64(layout.MaxLevel + 1)
	maxLane := float64(layout.MaxLaneWidth())
	if maxLane < 1 {
		maxLane = 1
	}
	width := padX*2 + levels*stepW + (levels-1)*gapX
	height := padY*2 + headerH + maxLane*stepH + (maxLane-1)*gapY
	return geometry{
		layout: layout, stepW: stepW, stepH: stepH,
		gapX: gapX, gapY: gapY, padX: padX, padY: padY,
		headerH: headerH, canvasW: width, canvasH: height,
		connectorIndent: indent,
	}
}

// openSVG writes the opening <svg> tag + accessibility attributes.
// viewBox is the natural canvas size; CSS classes pick up theme tints
// from app.css. role="img" + aria-label make the picture announcable.
func (g geometry) openSVG(buf *bytes.Buffer, name string) {
	// Explicit width/height match viewBox so the SVG renders at its
	// natural dimensions (180×64 step boxes) instead of stretching to
	// fill the parent container. Wide DAGs scroll horizontally in the
	// parent's overflow-x:auto container — better than scaling every
	// node up proportionally on small DAGs.
	fmt.Fprintf(buf,
		`<svg xmlns="http://www.w3.org/2000/svg" `+
			`width="%.0f" height="%.0f" `+
			`viewBox="0 0 %.0f %.0f" preserveAspectRatio="xMidYMid meet" `+
			`role="img" aria-label="DAG: workflow %s" class="dagviz">`,
		g.canvasW, g.canvasH, g.canvasW, g.canvasH,
		html.EscapeString(name),
	)
}

// drawDefs writes the inline <defs> block: an arrow marker for edge
// terminations and a soft drop-shadow for step boxes. Kept tiny so
// the asset weighs less than 1 KB per render.
func (g geometry) drawDefs(buf *bytes.Buffer) {
	buf.WriteString(`<defs>`)
	buf.WriteString(
		`<marker id="dagviz-arrow" viewBox="0 0 10 10" ` +
			`refX="8" refY="5" markerWidth="6" markerHeight="6" ` +
			`orient="auto-start-reverse">` +
			`<path d="M 0 0 L 10 5 L 0 10 z" class="dagviz-arrow"/>` +
			`</marker>`,
	)
	buf.WriteString(`</defs>`)
}

// drawEdges emits one <path> per DependsOn relationship. We use a
// gentle quadratic curve rather than a straight line so the eye
// follows the flow without snapping into the perpendicular grid.
func (g geometry) drawEdges(buf *bytes.Buffer, def dag.WorkflowDef) {
	for _, s := range def.Steps {
		for _, dep := range s.DependsOn {
			if _, ok := g.layout.Levels[dep]; !ok {
				continue
			}
			from := g.cellCenter(dep)
			to := g.cellCenter(s.ID)
			g.writeEdge(buf, from, to)
		}
	}
}

// writeEdge draws one edge from start to end, exiting the right edge
// of the source box and entering the left edge of the target. A small
// midpoint control point makes the line curve gently downward for
// lane-changing edges. Pure-SVG; no JS.
func (g geometry) writeEdge(buf *bytes.Buffer, from, to point) {
	startX := from.x + g.stepW/2 - g.connectorIndent
	startY := from.y
	endX := to.x - g.stepW/2 + g.connectorIndent
	endY := to.y
	midX := (startX + endX) / 2
	fmt.Fprintf(buf,
		`<path class="dagviz-edge" d="M %.1f %.1f C %.1f %.1f %.1f %.1f %.1f %.1f" `+
			`marker-end="url(#dagviz-arrow)" fill="none"/>`,
		startX, startY, midX, startY, midX, endY, endX, endY,
	)
}

// point is a 2-D coordinate used by edge math. Kept private — callers
// outside the package don't construct geometry.
type point struct{ x, y float64 }

// cellCenter returns the pixel center of the cell for stepID.
func (g geometry) cellCenter(stepID string) point {
	level := g.layout.Levels[stepID]
	lane := g.layout.Lanes[stepID]
	x := g.padX + float64(level)*(g.stepW+g.gapX) + g.stepW/2
	y := g.padY + g.headerH + float64(lane)*(g.stepH+g.gapY) + g.stepH/2
	return point{x: x, y: y}
}

// drawNodes emits one group per step. The group carries:
//   - class for the per-status tint
//   - a <title> for native browser tooltip with attempts + duration
//   - a focusable <rect> with role="button" + aria-label for keyboard
//     navigation and screen-reader announcements.
func (g geometry) drawNodes(
	buf *bytes.Buffer, def dag.WorkflowDef, run *dag.WorkflowRun,
) {
	for _, s := range def.Steps {
		node := g.nodeFor(s, run)
		g.writeNode(buf, s.ID, node)
	}
}

// nodeView is the per-step rendering payload.
type nodeView struct {
	id        string
	label     string
	status    string
	tone      string
	tooltip   string
	rotation  float64
	x, y      float64
	width     float64
	height    float64
	durLabel  string
	attempts  int
	stepType  string
	hasError  bool
	errorClip string
}

// nodeFor computes per-step view data. tone is the CSS class suffix
// that picks the box tint; tooltip carries hover-text content. When
// run is nil the tone is "pending" for every step (static mode).
func (g geometry) nodeFor(
	s dag.StepDef, run *dag.WorkflowRun,
) nodeView {
	cx := g.cellCenter(s.ID)
	view := nodeView{
		id: s.ID, label: s.ID,
		x: cx.x - g.stepW/2, y: cx.y - g.stepH/2,
		width: g.stepW, height: g.stepH,
		rotation: rotationFor(s.ID),
		stepType: stepTypeShort(s.Type),
	}
	if run == nil {
		view.tone = "pending"
		view.status = "pending"
		view.tooltip = "Step " + s.ID + " (pending — no run state)"
		return view
	}
	state, ok := run.Steps[s.ID]
	if !ok {
		view.tone = "pending"
		view.status = "pending"
		view.tooltip = "Step " + s.ID + " (no state yet)"
		return view
	}
	view.tone = toneForStatus(state.Status)
	view.status = state.Status.String()
	view.attempts = state.Attempts
	view.tooltip = tooltipFor(s.ID, state)
	if state.Error != "" {
		view.hasError = true
		view.errorClip = clipError(state.Error)
	}
	if !state.LoopStartedAt.IsZero() {
		view.durLabel = formatDur(time.Since(state.LoopStartedAt))
	}
	return view
}

// writeNode emits one step group: rectangle, label, hover-title,
// status pill, and (in run mode) an attempts badge.
func (g geometry) writeNode(
	buf *bytes.Buffer, id string, v nodeView,
) {
	rectClass := "dagviz-step dagviz-step-" + v.tone
	ariaLabel := fmt.Sprintf(
		"Step %s, status %s", id, v.status,
	)
	fmt.Fprintf(buf,
		`<g class="dagviz-node" transform="rotate(%.2f %.1f %.1f)">`,
		v.rotation, v.x+v.width/2, v.y+v.height/2,
	)
	fmt.Fprintf(buf,
		`<rect role="button" tabindex="0" `+
			`aria-label="%s" `+
			`class="%s" x="%.1f" y="%.1f" width="%.1f" height="%.1f" `+
			`rx="6" ry="6" data-step-id="%s" data-step-status="%s">`+
			`<title>%s</title></rect>`,
		html.EscapeString(ariaLabel),
		rectClass,
		v.x, v.y, v.width, v.height,
		html.EscapeString(id), html.EscapeString(v.status),
		html.EscapeString(v.tooltip),
	)
	g.writeNodeLabel(buf, v)
	g.writeNodeBadges(buf, v)
	buf.WriteString(`</g>`)
}

// writeNodeLabel emits the step id + type. Kept on a separate function
// so writeNode stays under 70 lines.
func (g geometry) writeNodeLabel(buf *bytes.Buffer, v nodeView) {
	fmt.Fprintf(buf,
		`<text class="dagviz-label" x="%.1f" y="%.1f" `+
			`text-anchor="middle" pointer-events="none">%s</text>`,
		v.x+v.width/2, v.y+v.height/2-4,
		html.EscapeString(v.label),
	)
	if v.stepType != "" {
		fmt.Fprintf(buf,
			`<text class="dagviz-sublabel" x="%.1f" y="%.1f" `+
				`text-anchor="middle" pointer-events="none">%s</text>`,
			v.x+v.width/2, v.y+v.height/2+12,
			html.EscapeString(v.stepType),
		)
	}
}

// writeNodeBadges emits the status pill (top-right) and attempts
// counter (bottom-right) when run state is present.
func (g geometry) writeNodeBadges(buf *bytes.Buffer, v nodeView) {
	if v.status == "" || v.status == "pending" {
		return
	}
	fmt.Fprintf(buf,
		`<rect class="dagviz-pill dagviz-pill-%s" x="%.1f" y="%.1f" `+
			`width="56" height="14" rx="7" ry="7"/>`+
			`<text class="dagviz-pill-text" x="%.1f" y="%.1f" `+
			`text-anchor="middle" pointer-events="none">%s</text>`,
		v.tone,
		v.x+v.width-60, v.y+4,
		v.x+v.width-32, v.y+15,
		html.EscapeString(statusGlyph(v.status)+" "+v.status),
	)
	if v.attempts > 1 {
		fmt.Fprintf(buf,
			`<text class="dagviz-attempts" x="%.1f" y="%.1f" `+
				`text-anchor="end" pointer-events="none">×%d</text>`,
			v.x+v.width-6, v.y+v.height-6, v.attempts,
		)
	}
}

// rotationFor returns a stable, tiny rotation (in degrees) seeded by
// the step id. Range is ±0.6° so the picture has the hand-drawn feel
// without disturbing legibility. Pure function — same input always
// returns the same output across renders.
func rotationFor(id string) float64 {
	if id == "" {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(id))
	v := h.Sum32()
	// Map to [-0.6, 0.6).
	const span = 1.2
	return (float64(v%1024)/1024.0)*span - span/2
}

// toneForStatus maps a step status to the CSS tone suffix the
// templates pick up. Keeps the SVG markup free of explicit colours;
// theme switches happen in CSS.
func toneForStatus(s dag.StepStatus) string {
	switch s {
	case dag.StepStatusRunning:
		return "running"
	case dag.StepStatusCompleted:
		return "completed"
	case dag.StepStatusFailed:
		return "failed"
	case dag.StepStatusQueued:
		return "queued"
	case dag.StepStatusSkipped:
		return "skipped"
	case dag.StepStatusCancelled:
		return "cancelled"
	case dag.StepStatusRecovered:
		return "recovered"
	}
	return "pending"
}

// stepTypeShort returns a one-or-two-word badge string for the step
// type. Used as the sub-label under the step id. StepType zero value
// is "normal" — we hide that to avoid clutter on every task step.
func stepTypeShort(t dag.StepType) string {
	s := t.String()
	if s == "" || s == "normal" {
		return ""
	}
	return strings.ToLower(s)
}

// tooltipFor returns the native-title text shown on hover. Carries
// attempt count and short error preview when present.
func tooltipFor(id string, state dag.StepState) string {
	parts := []string{
		"Step " + id,
		"status " + state.Status.String(),
		fmt.Sprintf("attempts %d", state.Attempts),
	}
	if state.Error != "" {
		parts = append(parts, "error: "+clipError(state.Error))
	}
	if !state.LoopStartedAt.IsZero() {
		dur := formatDur(time.Since(state.LoopStartedAt))
		parts = append(parts, "duration "+dur)
	}
	return strings.Join(parts, " · ")
}

// clipError trims a long error string to a single-line preview.
func clipError(s string) string {
	const max = 120
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > max {
		s = s[:max] + "…"
	}
	return s
}

// formatDur renders a duration as a tight human label. Picked the
// simplest path — uses time.Duration.String and post-truncates so
// "1m30s" becomes "1m30s" but "543.123ms" becomes "543ms".
func formatDur(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	return d.Truncate(time.Second).String()
}

// statusGlyph picks a one-character icon to pair with the status text
// on the pill. Pure-glyph signifier so colour isn't the only channel.
func statusGlyph(s string) string {
	switch s {
	case "running":
		return "›"
	case "completed":
		return "✓"
	case "failed":
		return "!"
	case "queued":
		return "·"
	case "skipped":
		return "—"
	case "cancelled":
		return "×"
	case "recovered":
		return "↺"
	}
	return "•"
}
