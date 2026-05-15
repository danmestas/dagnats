// Package dagviz renders a WorkflowDef into inline SVG using a
// longest-path-leveled vertical layout. No third-party JS or render
// libraries — pure Go, server-rendered, e-ink aesthetic.
//
// Design:
//
//   - Each step occupies one cell on a levels x lanes grid. Level is
//     the longest path from any root step; lane is assigned greedily
//     left-to-right per level. The leveling pass uses Kahn's
//     topological sort with an explicit stack — no recursion, satisfies
//     the project rule.
//   - Edges are straight lines from one step's right edge to the
//     dependent step's left edge. We do not attempt orthogonal routing
//     to keep the renderer small; level-monotonic layout keeps
//     crossings rare.
//   - Run-state overlay tints each step rectangle by StepStatus. Hover
//     details (attempts/duration/error preview) live in <title> child
//     elements so the browser surfaces them as native tooltips
//     without any JS.
//
// Limits and fallbacks:
//
//   - Step count cap = 30. Beyond that the visualisation degrades to
//     a stacked-card list (see Render's fallback path); a DAG with 30
//     steps already strains screen real estate.
//   - Cycle detection emits a clean error rather than infinite-looping;
//     callers render the error message in place of the SVG.
package dagviz

import (
	"errors"
	"fmt"

	"github.com/danmestas/dagnats/dag"
)

// MaxSteps is the upper bound for SVG rendering. Workflows with more
// steps fall back to a list rendering at the call site.
const MaxSteps = 30

// ErrTooManySteps is returned by Layout when the workflow exceeds
// MaxSteps. Callers should render the list fallback.
var ErrTooManySteps = errors.New("workflow exceeds dagviz step cap")

// ErrCycle is returned when the workflow contains a cycle. Render
// surfaces this as a "cannot visualise" message.
var ErrCycle = errors.New("workflow definition has a cycle")

// LayoutResult is the geometric placement of each step. Cells are
// keyed by step id; the renderer consumes this directly to emit SVG.
type LayoutResult struct {
	// Levels is the per-step level (column index, zero-origin).
	Levels map[string]int
	// Lanes is the per-step lane (row index within a level).
	Lanes map[string]int
	// LevelWidths holds the number of steps assigned to each level;
	// used to size the SVG canvas.
	LevelWidths []int
	// MaxLevel is the largest level index (zero-origin); equals
	// len(LevelWidths)-1.
	MaxLevel int
}

// Layout assigns each step a (level, lane) position. Returns an error
// when the workflow has a cycle (Kahn's topological sort terminates
// with unsatisfied nodes) or exceeds MaxSteps. The algorithm:
//
//  1. Build adjacency + in-degree from DependsOn.
//  2. Kahn's topological sort using an explicit stack-as-queue.
//  3. Each step's level = max(level of dependency) + 1.
//  4. Within each level, lanes are assigned in topological-output
//     order. Stable across re-renders because step iteration order
//     mirrors the WorkflowDef.Steps slice order.
//
// No recursion — the worklist is an explicit []string slice we
// manipulate iteratively.
func Layout(def dag.WorkflowDef) (LayoutResult, error) {
	if len(def.Steps) == 0 {
		return LayoutResult{
			Levels: map[string]int{}, Lanes: map[string]int{},
		}, nil
	}
	if len(def.Steps) > MaxSteps {
		return LayoutResult{}, ErrTooManySteps
	}
	graph := buildGraph(def)
	levels, err := topoLevels(graph)
	if err != nil {
		return LayoutResult{}, err
	}
	lanes, levelWidths := assignLanes(def.Steps, levels)
	maxLevel := 0
	for _, lv := range levels {
		if lv > maxLevel {
			maxLevel = lv
		}
	}
	return LayoutResult{
		Levels:      levels,
		Lanes:       lanes,
		LevelWidths: levelWidths,
		MaxLevel:    maxLevel,
	}, nil
}

// stepGraph holds adjacency info derived from a WorkflowDef. ids
// preserves the original step iteration order so lane assignment is
// stable across renders.
type stepGraph struct {
	ids      []string
	deps     map[string][]string // step -> deps it waits on
	rdeps    map[string][]string // step -> steps that depend on it
	indegree map[string]int
}

// buildGraph extracts the dependency graph from def.Steps. Missing
// dependencies (referenced ids not in the step set) are silently
// dropped — the engine catches those at admission time; the
// visualiser shouldn't crash on a half-valid definition.
func buildGraph(def dag.WorkflowDef) stepGraph {
	g := stepGraph{
		ids:      make([]string, 0, len(def.Steps)),
		deps:     make(map[string][]string, len(def.Steps)),
		rdeps:    make(map[string][]string, len(def.Steps)),
		indegree: make(map[string]int, len(def.Steps)),
	}
	present := make(map[string]bool, len(def.Steps))
	for _, s := range def.Steps {
		present[s.ID] = true
	}
	for _, s := range def.Steps {
		g.ids = append(g.ids, s.ID)
		realDeps := make([]string, 0, len(s.DependsOn))
		for _, d := range s.DependsOn {
			if !present[d] {
				continue
			}
			realDeps = append(realDeps, d)
		}
		g.deps[s.ID] = realDeps
		g.indegree[s.ID] = len(realDeps)
		for _, d := range realDeps {
			g.rdeps[d] = append(g.rdeps[d], s.ID)
		}
	}
	return g
}

// topoLevels runs Kahn's algorithm with an explicit FIFO worklist
// (slice + index). Returns the per-step level. Detects a cycle when
// the worklist drains before assigning every step a level.
func topoLevels(g stepGraph) (map[string]int, error) {
	levels := make(map[string]int, len(g.ids))
	// queue is the worklist; head is the read cursor. Bounded by len(ids).
	queue := make([]string, 0, len(g.ids))
	for _, id := range g.ids {
		if g.indegree[id] == 0 {
			queue = append(queue, id)
			levels[id] = 0
		}
	}
	head := 0
	// Bound the loop to prevent any accidental runaway. Tight bound is
	// len(g.ids)^2 (one item per id, processed once each).
	maxIters := len(g.ids) * len(g.ids)
	if maxIters < 256 {
		maxIters = 256
	}
	for iter := 0; iter < maxIters; iter++ {
		if head >= len(queue) {
			break
		}
		cur := queue[head]
		head++
		curLevel := levels[cur]
		for _, child := range g.rdeps[cur] {
			if l, ok := levels[child]; !ok || l < curLevel+1 {
				levels[child] = curLevel + 1
			}
			g.indegree[child]--
			if g.indegree[child] == 0 {
				queue = append(queue, child)
			}
		}
	}
	if len(levels) != len(g.ids) {
		return nil, ErrCycle
	}
	return levels, nil
}

// assignLanes converts the per-step level map into per-step lane
// indices. Iterates step slice in original order so a re-render with
// no graph change produces identical output. Returns the lane map +
// the lane count per level.
func assignLanes(
	steps []dag.StepDef, levels map[string]int,
) (map[string]int, []int) {
	maxLevel := 0
	for _, lv := range levels {
		if lv > maxLevel {
			maxLevel = lv
		}
	}
	widths := make([]int, maxLevel+1)
	lanes := make(map[string]int, len(steps))
	for _, s := range steps {
		lv := levels[s.ID]
		lanes[s.ID] = widths[lv]
		widths[lv]++
	}
	return lanes, widths
}

// MaxLaneWidth returns the largest lane count across all levels.
// Used to size the SVG height.
func (r LayoutResult) MaxLaneWidth() int {
	max := 0
	for _, w := range r.LevelWidths {
		if w > max {
			max = w
		}
	}
	return max
}

// Stable string for error reporting; "%w"-friendly.
func (r LayoutResult) String() string {
	return fmt.Sprintf("layout: levels=%d max_lane=%d",
		r.MaxLevel+1, r.MaxLaneWidth())
}
