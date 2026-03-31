// ui/gantt.go
// Server-side SVG Gantt chart renderer. Shows step execution
// timelines as horizontal bars to visualize parallelism and duration.
package ui

import (
	"fmt"
	"html/template"
	"strings"
	"time"

	"github.com/danmestas/dagnats/protocol"
)

// ganttBar represents one step's execution timeline.
type ganttBar struct {
	StepID   string
	Status   string
	StartMs  int64
	EndMs    int64
	Duration time.Duration
}

// ganttSVG holds the complete Gantt chart for template rendering.
type ganttSVG struct {
	Width    int
	Height   int
	Bars     []ganttBar
	TotalMs  int64
	BarScale float64
}

// Gantt layout constants.
const (
	ganttBarHeight  = 28
	ganttBarGap     = 8
	ganttLabelWidth = 120
	ganttPadding    = 20
	ganttChartWidth = 600
	ganttMaxBars    = 100
)

// buildGanttSVG creates a Gantt chart from event history.
// Events are processed to extract start/end times per step.
func buildGanttSVG(events []protocol.Event) ganttSVG {
	if len(events) == 0 {
		return ganttSVG{
			Width:  ganttLabelWidth + ganttChartWidth,
			Height: 60,
		}
	}

	// Find run start time.
	runStart := events[0].Timestamp
	for _, e := range events {
		if e.Timestamp.Before(runStart) {
			runStart = e.Timestamp
		}
	}

	// Track per-step start/end times and status.
	type stepTimes struct {
		start  time.Time
		end    time.Time
		status string
	}
	steps := make(map[string]*stepTimes, ganttMaxBars)
	order := make([]string, 0, ganttMaxBars)

	for _, e := range events {
		sid := e.StepID
		if sid == "" {
			continue
		}
		st, exists := steps[sid]
		if !exists {
			if len(order) >= ganttMaxBars {
				continue
			}
			st = &stepTimes{status: "pending"}
			steps[sid] = st
			order = append(order, sid)
		}
		switch e.Type {
		case protocol.EventStepStarted,
			protocol.EventStepQueued:
			if st.start.IsZero() {
				st.start = e.Timestamp
			}
			if e.Type == protocol.EventStepQueued {
				st.status = "queued"
			} else {
				st.status = "running"
			}
		case protocol.EventStepCompleted:
			st.end = e.Timestamp
			st.status = "completed"
		case protocol.EventStepFailed:
			st.end = e.Timestamp
			st.status = "failed"
		}
	}

	// Find max end time for scaling.
	runEnd := runStart
	for _, st := range steps {
		if !st.end.IsZero() && st.end.After(runEnd) {
			runEnd = st.end
		}
		if !st.start.IsZero() && st.start.After(runEnd) {
			runEnd = st.start
		}
	}
	// If run is still active, use now.
	totalMs := runEnd.Sub(runStart).Milliseconds()
	if totalMs <= 0 {
		totalMs = 1000
	}

	barScale := float64(ganttChartWidth) / float64(totalMs)

	bars := make([]ganttBar, 0, len(order))
	for _, sid := range order {
		st := steps[sid]
		startMs := int64(0)
		if !st.start.IsZero() {
			startMs = st.start.Sub(runStart).Milliseconds()
		}
		endMs := totalMs
		if !st.end.IsZero() {
			endMs = st.end.Sub(runStart).Milliseconds()
		}
		dur := time.Duration(endMs-startMs) * time.Millisecond
		bars = append(bars, ganttBar{
			StepID:   sid,
			Status:   st.status,
			StartMs:  startMs,
			EndMs:    endMs,
			Duration: dur,
		})
	}

	svgH := ganttPadding*2 + len(bars)*(ganttBarHeight+ganttBarGap)
	if svgH < 60 {
		svgH = 60
	}

	return ganttSVG{
		Width:    ganttLabelWidth + ganttChartWidth + ganttPadding*2,
		Height:   svgH,
		Bars:     bars,
		TotalMs:  totalMs,
		BarScale: barScale,
	}
}

// renderGanttSVG returns the SVG markup as template.HTML.
func renderGanttSVG(g ganttSVG) template.HTML {
	var b strings.Builder
	b.Grow(2048)

	fmt.Fprintf(&b,
		`<svg xmlns="http://www.w3.org/2000/svg" `+
			`viewBox="0 0 %d %d" class="gantt-svg">`,
		g.Width, g.Height,
	)

	for i, bar := range g.Bars {
		y := ganttPadding + i*(ganttBarHeight+ganttBarGap)
		x := ganttLabelWidth + ganttPadding +
			int(float64(bar.StartMs)*g.BarScale)
		w := int(
			float64(bar.EndMs-bar.StartMs) * g.BarScale,
		)
		if w < 4 {
			w = 4
		}

		// Label.
		fmt.Fprintf(&b,
			`<text x="%d" y="%d" `+
				`class="gantt-label">%s</text>`,
			ganttPadding, y+ganttBarHeight/2+4, bar.StepID,
		)

		// Bar.
		fmt.Fprintf(&b,
			`<rect x="%d" y="%d" width="%d" height="%d" `+
				`rx="4" class="gantt-bar dag-status-%s"/>`,
			x, y, w, ganttBarHeight, bar.Status,
		)

		// Duration text.
		durText := formatDuration(bar.Duration)
		fmt.Fprintf(&b,
			`<text x="%d" y="%d" `+
				`class="gantt-duration">%s</text>`,
			x+w+6, y+ganttBarHeight/2+4, durText,
		)
	}

	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}

// formatDuration returns a human-readable compact duration string.
func formatDuration(d time.Duration) string {
	if d < time.Second {
		return fmt.Sprintf("%dms", d.Milliseconds())
	}
	if d < time.Minute {
		return fmt.Sprintf("%.1fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fm", d.Minutes())
}
