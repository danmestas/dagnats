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
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// TraceRow is one span flattened into the console run-trace tree. Depth
// indents the name; DurationMs / Status come from the span. OffsetPct /
// WidthPct place the span's waterfall bar within the trace window (both
// 0 when the span has no honest duration — the template then omits the
// bar). ParentSpanID and the KV fields back the clickable span-detail
// panel; each KV field is the empty string when the span carries no such
// attribute, so the panel honestly omits a missing datum.
type TraceRow struct {
	Depth        int
	Name         string
	DurationMs   float64
	Status       string // "ok" | "error" | "unset"
	SpanID       string
	ParentSpanID string
	OffsetPct    float64
	WidthPct     float64
	RunID        string
	StepID       string
	TaskName     string
	Workflow     string
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

// flatSpan is one pre-flattened span carrying its pre-order tree depth.
type flatSpan struct {
	node  *spanread.SpanNode
	depth int
}

// flattenSpanTree walks every trace's span tree in pre-order once into a
// flat slice (explicit stack, no recursion), derives the trace window
// from that single pass, then emits one TraceRow per span carrying its
// depth and waterfall geometry. Roots across distinct trace IDs are
// emitted in start-time order; within a subtree, children appear in
// start-time order (the builder already sorted them).
func flattenSpanTree(
	roots map[string][]*spanread.SpanNode,
) []TraceRow {
	flat := preflatten(roots)
	var traceStart, traceEnd uint64
	for i := 0; i < len(flat); i++ {
		sp := flat[i].node.Span
		if i == 0 || sp.StartTimeUnixNano < traceStart {
			traceStart = sp.StartTimeUnixNano
		}
		if sp.EndTimeUnixNano > traceEnd { // 0 (in-flight) never extends it
			traceEnd = sp.EndTimeUnixNano
		}
	}
	rows := make([]TraceRow, 0, len(flat))
	for i := 0; i < len(flat); i++ {
		sp := flat[i].node.Span
		offset, width := spanGeometry(sp, traceStart, traceEnd)
		rows = append(rows, TraceRow{
			Depth:        flat[i].depth,
			Name:         sp.Name,
			DurationMs:   float64(spanread.DurationMs(sp)),
			Status:       spanread.StatusLabel(sp),
			SpanID:       spanread.HexSpanID(sp),
			ParentSpanID: spanread.HexParentID(sp),
			OffsetPct:    offset,
			WidthPct:     width,
			RunID:        spanread.SpanAttr(sp, "run_id"),
			StepID:       spanread.SpanAttr(sp, "step_id"),
			TaskName:     spanread.SpanAttr(sp, "task_name"),
			Workflow:     spanread.SpanAttr(sp, "workflow"),
		})
	}
	return rows
}

// preflatten pre-orders every trace's roots (start-time sorted) into one
// bounded flat slice via an explicit stack — no recursion, no second walk.
func preflatten(roots map[string][]*spanread.SpanNode) []flatSpan {
	allRoots := make([]*spanread.SpanNode, 0, len(roots))
	for _, nodes := range roots {
		allRoots = append(allRoots, nodes...)
	}
	if len(allRoots) > traceFlattenMax {
		panic("preflatten: roots exceeds max bound")
	}
	sort.SliceStable(allRoots, func(i, j int) bool {
		return allRoots[i].Span.StartTimeUnixNano <
			allRoots[j].Span.StartTimeUnixNano
	})
	stack := make([]flatSpan, 0, traceFlattenMax)
	for i := len(allRoots) - 1; i >= 0; i-- { // reverse push → sorted pop
		stack = append(stack, flatSpan{node: allRoots[i], depth: 0})
	}
	flat := make([]flatSpan, 0, len(allRoots))
	for i := 0; i < traceFlattenMax && len(stack) > 0; i++ {
		top := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		flat = append(flat, top)
		kids := top.node.Children
		for k := len(kids) - 1; k >= 0; k-- {
			stack = append(stack, flatSpan{node: kids[k], depth: top.depth + 1})
		}
	}
	return flat
}

// spanGeometry maps a span onto the [traceStart, traceEnd] window as a
// (offsetPct, widthPct) pair. Returns (0, 0) when the trace window is
// empty or the span has no honest duration (mirrors DurationMs's guard),
// so an in-flight or zero-width span draws no fabricated bar. Offset is
// clamped to [0, 100] and width is clipped so offset+width never exceeds
// 100 — a bar whose raw extent overshoots the window stays inside the
// track's right edge rather than painting past it.
func spanGeometry(sp *tracepb.Span, traceStart, traceEnd uint64) (float64, float64) {
	if sp == nil {
		panic("spanGeometry: span must not be nil")
	}
	if traceEnd <= traceStart {
		return 0, 0
	}
	if sp.EndTimeUnixNano <= sp.StartTimeUnixNano {
		return 0, 0
	}
	window := float64(traceEnd - traceStart)
	offset := clampPct(float64(sp.StartTimeUnixNano-traceStart) / window * 100)
	width := clampPct(float64(sp.EndTimeUnixNano-sp.StartTimeUnixNano) / window * 100)
	if offset+width > 100 { // clip the bar to the track's right edge
		width = 100 - offset
	}
	return offset, width
}
