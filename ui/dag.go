// ui/dag.go
// Server-side SVG DAG renderer. Lays out workflow steps left-to-right
// by dependency depth, draws bezier edges, and colors nodes by status.
// Pure Go — no JS graph library required.
package ui

import (
	"fmt"
	"html/template"
	"strings"

	"github.com/danmestas/dagnats/dag"
)

// dagNode holds layout coordinates for a single step in the SVG.
type dagNode struct {
	ID     string
	Task   string
	Type   dag.StepType
	Status string
	X      int
	Y      int
	Width  int
	Height int
	// AgentLoop iteration count for badge display.
	Iterations int
}

// dagEdge connects two nodes with a bezier curve.
type dagEdge struct {
	FromX int
	FromY int
	ToX   int
	ToY   int
}

// dagSVG holds the complete SVG layout for template rendering.
type dagSVG struct {
	Width  int
	Height int
	Nodes  []dagNode
	Edges  []dagEdge
}

// Constants for SVG layout geometry.
const (
	dagNodeWidth   = 160
	dagNodeHeight  = 48
	dagColGap      = 80
	dagRowGap      = 24
	dagPadding     = 40
	dagMaxColumns  = 50
	dagMaxSteps    = 200
)

// buildDAGSVG computes layout and returns a dagSVG ready for
// template rendering. Steps are arranged left-to-right by
// dependency depth using iterative topological layering.
func buildDAGSVG(
	def dag.WorkflowDef,
	steps map[string]dag.StepState,
) dagSVG {
	if len(def.Steps) == 0 {
		return dagSVG{Width: 100, Height: 100}
	}
	stepCount := len(def.Steps)
	if stepCount > dagMaxSteps {
		stepCount = dagMaxSteps
	}

	// Build adjacency and compute depth per step.
	depMap := make(map[string][]string, stepCount)
	stepByID := make(map[string]dag.StepDef, stepCount)
	for _, s := range def.Steps[:stepCount] {
		depMap[s.ID] = s.DependsOn
		stepByID[s.ID] = s
	}
	depth := computeDepths(depMap, stepCount)

	// Group steps by column (depth).
	columns := make(map[int][]string, dagMaxColumns)
	maxCol := 0
	for id, d := range depth {
		columns[d] = append(columns[d], id)
		if d > maxCol {
			maxCol = d
		}
	}

	// Assign X,Y coordinates.
	nodes := make([]dagNode, 0, stepCount)
	nodePos := make(map[string]dagNode, stepCount)
	for col := 0; col <= maxCol; col++ {
		ids := columns[col]
		x := dagPadding + col*(dagNodeWidth+dagColGap)
		for row, id := range ids {
			y := dagPadding + row*(dagNodeHeight+dagRowGap)
			status := "pending"
			iters := 0
			if st, ok := steps[id]; ok {
				status = st.Status.String()
				iters = st.Iterations
			}
			sd := stepByID[id]
			n := dagNode{
				ID:         id,
				Task:       sd.Task,
				Type:       sd.Type,
				Status:     status,
				X:          x,
				Y:          y,
				Width:      dagNodeWidth,
				Height:     dagNodeHeight,
				Iterations: iters,
			}
			nodes = append(nodes, n)
			nodePos[id] = n
		}
	}

	// Compute SVG dimensions.
	maxRow := 0
	for _, ids := range columns {
		if len(ids) > maxRow {
			maxRow = len(ids)
		}
	}
	svgW := dagPadding*2 + (maxCol+1)*dagNodeWidth +
		maxCol*dagColGap
	svgH := dagPadding*2 + maxRow*dagNodeHeight +
		(maxRow-1)*dagRowGap
	if svgW < 200 {
		svgW = 200
	}
	if svgH < 100 {
		svgH = 100
	}

	// Build edges.
	edges := make([]dagEdge, 0, stepCount*2)
	for _, s := range def.Steps[:stepCount] {
		to, ok := nodePos[s.ID]
		if !ok {
			continue
		}
		for _, depID := range s.DependsOn {
			from, ok := nodePos[depID]
			if !ok {
				continue
			}
			edges = append(edges, dagEdge{
				FromX: from.X + from.Width,
				FromY: from.Y + from.Height/2,
				ToX:   to.X,
				ToY:   to.Y + to.Height/2,
			})
		}
	}

	return dagSVG{
		Width:  svgW,
		Height: svgH,
		Nodes:  nodes,
		Edges:  edges,
	}
}

// computeDepths assigns a column depth to each step using
// iterative BFS. Steps with no dependencies get depth 0.
func computeDepths(
	depMap map[string][]string, maxSteps int,
) map[string]int {
	depth := make(map[string]int, len(depMap))
	// Initialize all at 0.
	for id := range depMap {
		depth[id] = 0
	}
	// Iterative relaxation (bounded).
	const maxIterations = 100
	for iter := 0; iter < maxIterations; iter++ {
		changed := false
		for id, deps := range depMap {
			for _, dep := range deps {
				if d, ok := depth[dep]; ok {
					if d+1 > depth[id] {
						depth[id] = d + 1
						changed = true
					}
				}
			}
		}
		if !changed {
			break
		}
	}
	return depth
}

// renderDAGSVG returns the SVG markup as template.HTML.
func renderDAGSVG(svg dagSVG) template.HTML {
	var b strings.Builder
	b.Grow(4096)

	fmt.Fprintf(&b,
		`<svg xmlns="http://www.w3.org/2000/svg" `+
			`viewBox="0 0 %d %d" class="dag-svg">`,
		svg.Width, svg.Height,
	)

	// Draw edges first (behind nodes).
	for _, e := range svg.Edges {
		cx1 := e.FromX + (e.ToX-e.FromX)/3
		cx2 := e.ToX - (e.ToX-e.FromX)/3
		fmt.Fprintf(&b,
			`<path d="M %d %d C %d %d, %d %d, %d %d" `+
				`class="dag-edge"/>`,
			e.FromX, e.FromY,
			cx1, e.FromY,
			cx2, e.ToY,
			e.ToX, e.ToY,
		)
	}

	// Draw nodes.
	for _, n := range svg.Nodes {
		fmt.Fprintf(&b,
			`<g class="dag-node dag-status-%s">`+
				`<rect x="%d" y="%d" width="%d" height="%d" `+
				`rx="6" class="dag-node-rect"/>`+
				`<text x="%d" y="%d" `+
				`class="dag-node-label">%s</text>`+
				`<text x="%d" y="%d" `+
				`class="dag-node-task">%s</text>`,
			n.Status,
			n.X, n.Y, n.Width, n.Height,
			n.X+n.Width/2, n.Y+18, n.ID,
			n.X+n.Width/2, n.Y+36, n.Task,
		)
		// Agent loop badge.
		if n.Type == dag.StepTypeAgentLoop {
			fmt.Fprintf(&b,
				`<circle cx="%d" cy="%d" r="10" `+
					`class="dag-loop-badge"/>`+
					`<text x="%d" y="%d" `+
					`class="dag-loop-count">%d</text>`,
				n.X+n.Width-8, n.Y+8,
				n.X+n.Width-8, n.Y+12, n.Iterations,
			)
		}
		b.WriteString(`</g>`)
	}

	b.WriteString(`</svg>`)
	return template.HTML(b.String())
}
