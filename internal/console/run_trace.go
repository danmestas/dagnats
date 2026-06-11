// run_trace.go
// Console run-trace read path: collect OTLP spans for a run, build the
// per-trace span trees via internal/observe/spanread, and flatten them
// into the depth-indented row model the Trace tab renders. The web
// counterpart of the `dagnats trace <run-id>` CLI view.
package console

import (
	"context"
	"sort"

	"github.com/danmestas/dagnats/internal/observe/spanread"
	"github.com/nats-io/nats.go/jetstream"
)

// TraceRow is one span flattened into the console run-trace tree:
// Depth indents the name; DurationMs / Status come from the span.
type TraceRow struct {
	Depth      int
	Name       string
	DurationMs float64
	Status     string // "ok" | "error" | "unset"
	SpanID     string
}

// traceFlattenMax bounds the flatten loop so a malformed tree (which
// the builder shouldn't produce) can never spin unbounded.
const traceFlattenMax = spanread.MaxSpans

// GetRunTrace reads the run's spans from the TELEMETRY stream and
// flattens them into pre-order TraceRows. Returns (nil, nil) when no
// NATS connection is wired so the Trace tab paints the empty state.
func (a *apiServiceAdapter) GetRunTrace(
	ctx context.Context, runID string,
) ([]TraceRow, error) {
	if ctx == nil {
		panic("apiServiceAdapter.GetRunTrace: ctx is nil")
	}
	if runID == "" {
		panic("apiServiceAdapter.GetRunTrace: runID is empty")
	}
	if a.nc == nil {
		return nil, nil
	}
	js, err := jetstream.New(a.nc)
	if err != nil {
		return nil, err
	}
	spans, err := spanread.CollectRunSpans(
		ctx, js, runID, spanread.MaxSpans,
	)
	if err != nil {
		return nil, err
	}
	roots := spanread.BuildSpanTrees(spans)
	return flattenSpanTree(roots), nil
}

// flattenSpanTree walks every trace's span tree in pre-order and emits
// one TraceRow per span with its tree depth. Roots across distinct
// trace IDs are emitted in start-time order; within a subtree, children
// appear in start-time order (the builder already sorted them). Uses an
// explicit stack — no recursion — per the repo's no-recursion rule.
func flattenSpanTree(
	roots map[string][]*spanread.SpanNode,
) []TraceRow {
	allRoots := make([]*spanread.SpanNode, 0, len(roots))
	for _, nodes := range roots {
		allRoots = append(allRoots, nodes...)
	}
	if len(allRoots) > traceFlattenMax {
		panic("flattenSpanTree: roots exceeds max bound")
	}
	sort.SliceStable(allRoots, func(i, j int) bool {
		return allRoots[i].Span.StartTimeUnixNano <
			allRoots[j].Span.StartTimeUnixNano
	})

	type frame struct {
		node  *spanread.SpanNode
		depth int
	}
	// Seed the stack so roots pop in start-time order (reverse push).
	stack := make([]frame, 0, traceFlattenMax)
	for i := len(allRoots) - 1; i >= 0; i-- {
		stack = append(stack, frame{node: allRoots[i], depth: 0})
	}

	rows := make([]TraceRow, 0, len(allRoots))
	for i := 0; i < traceFlattenMax && len(stack) > 0; i++ {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		sp := top.node.Span
		rows = append(rows, TraceRow{
			Depth:      top.depth,
			Name:       sp.Name,
			DurationMs: float64(spanread.DurationMs(sp)),
			Status:     spanread.StatusLabel(sp),
			SpanID:     spanread.HexSpanID(sp),
		})
		// Reverse push so children pop in their sorted order.
		kids := top.node.Children
		for k := len(kids) - 1; k >= 0; k-- {
			stack = append(stack, frame{
				node: kids[k], depth: top.depth + 1,
			})
		}
	}
	return rows
}
