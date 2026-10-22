// Package chaos is the test-only crash seam of the durability
// verification harness (#809). The worker SDK, the journal and the
// engine call At at each boundary where a crash leaves distinct durable
// state behind; a test installs a Hook that decides, per site, whether
// the code proceeds, stalls or dies there. Production never installs a
// hook, so each boundary costs one atomic load and a nil check, and no
// behaviour changes: the package is internal, so nothing outside this
// module can install one.
//
// A kill is modelled as the handling goroutine returning ErrKilled up
// its stack without acknowledging, negatively acknowledging or
// publishing anything further, exactly what a dead process leaves
// behind: an unacknowledged message JetStream redelivers after AckWait,
// and whatever records landed before the point. A stall is the hook
// blocking inside the boundary and then returning nil, so the zombie
// resumes past its AckWait and must be fenced by its successor.
package chaos

import (
	"context"
	"errors"
	"sync/atomic"
)

// Point is a boundary where a crash leaves distinct durable state.
type Point int

const (
	// WorkerBeforeClaim: the task is delivered, no claim is written.
	WorkerBeforeClaim Point = iota + 1
	// WorkerAfterClaim: the claim landed, nothing else did.
	WorkerAfterClaim
	// WorkerAfterToolIntent: tool.intent landed, the tool has not run.
	WorkerAfterToolIntent
	// WorkerInsideTool: the tool's effect is done, tool.result is not
	// written.
	WorkerInsideTool
	// WorkerAfterToolResult: tool.result landed.
	WorkerAfterToolResult
	// WorkerAfterCommit: the commit landed, its lifecycle event is not
	// published.
	WorkerAfterCommit
	// WorkerAfterEvent: the lifecycle event is published, the task is
	// not acknowledged.
	WorkerAfterEvent
	// EngineEnqueueSaved: the dispatch save landed (DispatchUnsent set),
	// the task is not published (enqueueReady).
	EngineEnqueueSaved
	// EngineEnqueueSent: the task is published, the sent marker is not
	// saved (enqueueReady); a redelivered event resends the task.
	EngineEnqueueSent
	// EngineContinueSaved: the iteration save landed, the next task is
	// not published (step.continue).
	EngineContinueSaved
	// EngineContinueSent: the next task is published, the sent marker
	// is not saved (step.continue); a redelivered event resends it.
	EngineContinueSent
	pointMax
)

// String names the point for logs and subtest names.
func (p Point) String() string {
	if p <= 0 || p >= pointMax {
		panic("chaos.Point: out of range")
	}
	names := [...]string{
		"", "worker.before_claim", "worker.after_claim",
		"worker.after_tool_intent", "worker.inside_tool",
		"worker.after_tool_result", "worker.after_commit",
		"worker.after_event", "engine.enqueue_saved",
		"engine.enqueue_sent", "engine.continue_saved",
		"engine.continue_sent",
	}
	if len(names) != int(pointMax) {
		panic("chaos.Point: names out of date")
	}
	return names[p]
}

// Points lists every point, in order.
func Points() []Point {
	points := make([]Point, 0, pointMax-1)
	for p := Point(1); p < pointMax; p++ {
		points = append(points, p)
	}
	if len(points) != int(pointMax)-1 {
		panic("chaos.Points: wrong count")
	}
	return points
}

// Site identifies where a point is reached: the dispatch the worker
// holds, or the run and step the engine is dispatching (Delivered is
// 0 for engine points, and Iteration and Attempt are what the engine
// has saved).
type Site struct {
	RunID     string
	StepID    string
	Iteration int
	Attempt   int
	Delivered int
}

// Hook decides what happens at a point. A nil error proceeds; ErrKilled
// (or an error wrapping it) kills the holder there. The hook stalls by
// blocking before it returns; ctx is the holder's context, so a stall
// can end early when the holder is cancelled.
type Hook func(ctx context.Context, p Point, site Site) error

// ErrKilled is the error a killed holder carries up its stack.
var ErrKilled = errors.New("chaos: killed at a crash point")

var installed atomic.Pointer[Hook]

// Install sets the hook; nil removes it. Tests install one hook per
// process and route by Site inside it.
func Install(h Hook) {
	if h == nil {
		installed.Store(nil)
		return
	}
	installed.Store(&h)
}

// Installed reports whether a hook is installed: callers that would
// pay to build a Site check it first, so the production cost of a
// boundary stays one atomic load.
func Installed() bool {
	return installed.Load() != nil
}

// At runs the installed hook, when there is one, at p.
func At(ctx context.Context, p Point, site Site) error {
	if p <= 0 || p >= pointMax {
		panic("chaos.At: point out of range")
	}
	if site.RunID == "" {
		panic("chaos.At: site must name a run")
	}
	h := installed.Load()
	if h == nil {
		return nil
	}
	return (*h)(ctx, p, site)
}

// Killed reports whether err is a kill.
func Killed(err error) bool {
	return err != nil && errors.Is(err, ErrKilled)
}
