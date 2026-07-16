// engine/sticky.go
// Sticky worker routing: binds workflow runs to specific workers.
// Binding is created on first step completion, read on subsequent
// step dispatch. The engine owns all routing decisions — workers
// just include their WorkerID in completion events.
package engine

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// CreateBinding writes a sticky binding if the workflow is sticky
// and no binding exists yet. Safe to call on nil receiver.
func (sr *StickyRouter) CreateBinding(
	ctx context.Context,
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	evt protocol.Event,
) {
	if sr == nil {
		return
	}
	if wfDef.Sticky == dag.StickyNone {
		return
	}
	if evt.WorkerID == "" {
		return
	}

	// Only create binding once per run
	_, err := sr.kv.Get(ctx, run.RunID)
	if err == nil {
		return // binding already exists
	}

	// Atomic create — if another step completes concurrently,
	// the first one wins.
	_, _ = sr.kv.Create(
		ctx, run.RunID, []byte(evt.WorkerID),
	)
}

// GetWorker returns the bound worker ID for a run, or empty
// string if no binding exists. Safe to call on nil receiver.
func (sr *StickyRouter) GetWorker(
	ctx context.Context, runID string,
) string {
	if sr == nil {
		return ""
	}
	entry, err := sr.kv.Get(ctx, runID)
	if err != nil {
		return ""
	}
	return string(entry.Value())
}

// DeleteBinding removes the binding for a run. Called on
// workflow completion, failure, or cancellation.
// Safe to call on nil receiver.
func (sr *StickyRouter) DeleteBinding(
	ctx context.Context, runID string,
) {
	if sr == nil {
		return
	}
	_ = sr.kv.Delete(ctx, runID)
}

// PublishTask encapsulates all sticky routing complexity.
// Hard: publish only to worker-specific subject.
// Soft: publish to worker-specific subject, schedule fallback timer
// that re-publishes to normal subject if unclaimed.
func (sr *StickyRouter) PublishTask(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	attempt int,
	workerID string,
	strategy dag.StickyStrategy,
	dispatchNonce string,
	workflowName string,
) error {
	if sr == nil {
		panic("StickyRouter.PublishTask: called on nil receiver")
	}
	if runID == "" {
		panic("StickyRouter.PublishTask: runID must not be empty")
	}
	if workerID == "" {
		panic(
			"StickyRouter.PublishTask: workerID must not be empty",
		)
	}

	ctx, span := sr.tracer.Start(ctx,
		"dagnats.engine publishStickyTask",
		trace.WithAttributes(
			attribute.String("run_id", runID),
			attribute.String("worker_id", workerID),
			attribute.String("strategy", string(strategy)),
		),
	)
	defer span.End()

	// Build the task payload
	payload := protocol.TaskPayload{
		TaskID:        runID + "." + step.ID,
		RunID:         runID,
		StepID:        step.ID,
		Attempt:       attempt,
		Input:         input,
		WorkflowName:  workflowName,
		DispatchNonce: dispatchNonce,
	}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal TaskPayload: %w", err)
	}

	// Worker-specific subject on STICKY_TASKS stream
	stickySubject := "sticky." + step.Task + "." +
		workerID + "." + runID
	msgID := runID + "." + step.ID + ".queued.sticky"

	stickyMsg := &nats.Msg{
		Subject: stickySubject,
		Data:    data,
		Header:  nats.Header{"Nats-Msg-Id": {msgID}},
	}
	_, err = sr.tp.JSPublishMsg(ctx, stickyMsg)
	if err != nil {
		return fmt.Errorf("publish sticky task: %w", err)
	}
	sr.stepEnqueueCount.Add(ctx, 1)

	if strategy == dag.StickySoft && sr.sleepTimer != nil {
		sr.scheduleSoftFallback(
			ctx, runID, step, input, attempt, dispatchNonce, workflowName,
		)
	}

	return nil
}

// scheduleSoftFallback schedules the soft-sticky fallback timer: if
// the sticky worker doesn't claim the task within 5 seconds, it
// re-publishes to the normal (non-sticky) subject. Split out of
// PublishTask to keep that function within the house function-length
// budget.
func (sr *StickyRouter) scheduleSoftFallback(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	attempt int,
	dispatchNonce string,
	workflowName string,
) {
	if sr.sleepTimer == nil {
		panic("scheduleSoftFallback: sleepTimer must not be nil")
	}
	if runID == "" {
		panic("scheduleSoftFallback: runID must not be empty")
	}
	sr.sleepTimer.Schedule(ctx, TimerMessage{
		Action:       TimerActionRateRetry, // reuses rate retry
		RunID:        runID,
		StepID:       step.ID,
		DurationMs:   5000,
		TaskType:     step.Task,
		Input:        input,
		Attempt:      attempt,
		WorkflowName: workflowName,
		// Carry the run-binding nonce so the fallback re-publish (#380)
		// stays run-bound. Sticky steps carry no control-plane capability,
		// so no caps need stripping here.
		DispatchNonce: dispatchNonce,
	})
}
