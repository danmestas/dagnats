package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/protocol"
)

// A run is cancellable from the moment the API acknowledges its start
// (#1006), but its snapshot appears only when the orchestrator handles
// workflow.started. The cancel is not lost in that window either way
// round: a start that finds a cancel already behind it in history, and
// a cancel handled before the start (a redelivered start, or another
// engine taking the cancel first), both create the run's snapshot as
// cancelled from the start event. The snapshot is the tombstone: the
// start's existence check (startExistingRun) then skips it, and since
// every creating write is a KV Create, a start and a cancel racing on
// two engines cannot both create it.

// cancelBeforeStart handles workflow.cancelled for a run with no
// snapshot. A run the history stream never saw start, or whose snapshot
// was deleted, has nothing to cancel: the event is dropped rather than
// retried into DEAD_LETTERS.
func (o *Orchestrator) cancelBeforeStart(ctx context.Context, runID string) error {
	if ctx == nil {
		panic("cancelBeforeStart: ctx must not be nil")
	}
	if runID == "" {
		panic("cancelBeforeStart: runID must not be empty")
	}
	run, err := o.store.LoadAccepted(ctx, runID)
	if errors.Is(err, ErrRunNotFound) {
		slog.InfoContext(ctx, "dropping workflow.cancelled for a run that "+
			"never started or was pruned", "run_id", runID)
		return nil
	}
	if err != nil {
		return err
	}
	if run.Revision != nil {
		// The start was handled since the caller's Load: cancel the
		// snapshot through the normal path, from a fresh Load.
		return fmt.Errorf("%w: run %q was saved after its cancel arrived",
			ErrSnapshotConflict, runID)
	}
	return o.persistCancelledBeforeStart(ctx, run)
}

// startCancelledRun finalizes a starting run as cancelled, before
// admission or dispatch, when a cancel already waits behind its start in
// history. handled reports that it did.
func (o *Orchestrator) startCancelledRun(
	ctx context.Context, evt protocol.Event,
) (handled bool, err error) {
	if evt.RunID == "" {
		panic("startCancelledRun: RunID must not be empty")
	}
	if evt.Type != protocol.EventWorkflowStarted {
		panic("startCancelledRun: event must be workflow.started")
	}
	cancelled, err := cancelInHistory(ctx, o.nc, evt.RunID)
	if err != nil || !cancelled {
		return false, err
	}
	run, ok := pendingRunFromStart(evt)
	if !ok {
		// An unreadable start: the invalid-start path records it.
		return false, nil
	}
	return true, o.persistCancelledBeforeStart(ctx, run)
}

// persistCancelledBeforeStart creates run's snapshot as cancelled. Its
// creating write is a KV Create: losing it to the run's start (another
// engine) is a snapshot conflict, so the cancel is handled again from a
// fresh Load through the normal path. No admission was claimed and no
// step dispatched, so there is nothing to release.
func (o *Orchestrator) persistCancelledBeforeStart(
	ctx context.Context, run dag.WorkflowRun,
) error {
	if run.RunID == "" {
		panic("persistCancelledBeforeStart: RunID must not be empty")
	}
	if run.Revision != nil {
		panic("persistCancelledBeforeStart: run must not have a snapshot yet")
	}
	slog.InfoContext(ctx, "cancelling a run before it started",
		"run_id", run.RunID, "workflow_id", run.WorkflowID)
	cancelUnfinishedSteps(run)
	_, err := finalizeRun(
		ctx, o.tp, o.store, o.createSnapshotOnce, run, dag.RunStatusCancelled, "", nil,
	)
	return err
}

// createSnapshotOnce is a SaveSnapshotFunc for a run's creating write:
// a KV Create, reporting a lost race as ErrSnapshotConflict.
func (o *Orchestrator) createSnapshotOnce(
	ctx context.Context, run dag.WorkflowRun, _ string,
) error {
	if run.RunID == "" {
		panic("createSnapshotOnce: RunID must not be empty")
	}
	if o.store == nil {
		panic("createSnapshotOnce: store must not be nil")
	}
	created, err := o.store.CreateSnapshot(ctx, run)
	if err != nil {
		return err
	}
	if !created {
		return fmt.Errorf("%w: run %q already has a snapshot", ErrSnapshotConflict, run.RunID)
	}
	return nil
}
