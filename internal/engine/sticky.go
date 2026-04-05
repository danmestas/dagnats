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
	"github.com/danmestas/dagnats/observe"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// createStickyBinding writes a sticky binding if the workflow is
// sticky and no binding exists yet. Called from handleStepCompleted.
func (o *Orchestrator) createStickyBinding(
	wfDef dag.WorkflowDef,
	run dag.WorkflowRun,
	evt protocol.Event,
) {
	if wfDef.Sticky == dag.StickyNone {
		return
	}
	if o.stickyKV == nil {
		return
	}
	if evt.WorkerID == "" {
		return
	}

	// Only create binding once per run
	_, err := o.stickyKV.Get(
		context.Background(), run.RunID,
	)
	if err == nil {
		return // binding already exists
	}

	// Atomic create — if another step completes concurrently,
	// the first one wins.
	_, _ = o.stickyKV.Create(
		context.Background(), run.RunID, []byte(evt.WorkerID),
	)
}

// getStickyWorker returns the bound worker ID for a run, or empty
// string if no binding exists.
func (o *Orchestrator) getStickyWorker(runID string) string {
	if o.stickyKV == nil {
		return ""
	}
	entry, err := o.stickyKV.Get(
		context.Background(), runID,
	)
	if err != nil {
		return ""
	}
	return string(entry.Value())
}

// deleteStickyBinding removes the binding for a run. Called on
// workflow completion, failure, or cancellation.
func (o *Orchestrator) deleteStickyBinding(runID string) {
	if o.stickyKV == nil {
		return
	}
	_ = o.stickyKV.Delete(context.Background(), runID)
}

// publishStickyTask encapsulates all sticky routing complexity.
// Hard: publish only to worker-specific subject.
// Soft: publish to worker-specific subject, schedule fallback timer
// that re-publishes to normal subject if unclaimed.
func (o *Orchestrator) publishStickyTask(
	ctx context.Context,
	runID string,
	step dag.StepDef,
	input []byte,
	attempt int,
	workerID string,
	strategy dag.StickyStrategy,
) error {
	if runID == "" {
		panic("publishStickyTask: runID must not be empty")
	}
	if workerID == "" {
		panic("publishStickyTask: workerID must not be empty")
	}

	ctx, span := o.tel.Tracer.Start(ctx,
		"orchestrator.publishStickyTask",
		observe.WithAttributes(
			observe.StringAttr("run_id", runID),
			observe.StringAttr("worker_id", workerID),
			observe.StringAttr("strategy", string(strategy)),
		),
	)
	defer span.End()

	// Build the task payload
	payload := protocol.TaskPayload{
		TaskID:  runID + "." + step.ID,
		RunID:   runID,
		StepID:  step.ID,
		Attempt: attempt,
		Input:   input,
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
	injectTraceCtx(ctx, span, stickyMsg)
	_, err = o.js.PublishMsg(
		context.Background(), stickyMsg,
	)
	if err != nil {
		return fmt.Errorf("publish sticky task: %w", err)
	}
	o.stepEnqueueCount.Inc()

	if strategy == dag.StickySoft && o.sleepTimer != nil {
		// Schedule fallback: if sticky worker doesn't claim
		// within 5 seconds, re-publish to normal subject.
		o.sleepTimer.Schedule(TimerMessage{
			Action:     TimerActionRateRetry, // reuses rate retry
			RunID:      runID,
			StepID:     step.ID,
			DurationMs: 5000,
			TaskType:   step.Task,
			Input:      input,
			Attempt:    attempt,
		})
	}

	return nil
}
