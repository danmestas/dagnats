// engine/reconciler.go
// Periodic janitor that recovers wedged workflow runs — entries
// in the workflow_runs KV stuck at RunStatusRunning despite
// having no in-flight step and no path to terminal state. The
// production symptom (#185) was a workflow_runs counter that
// monotonically grew on workflows whose runs sometimes finish
// without ever invoking the run-completion path.
//
// The janitor's predicates are KV-only (no JetStream queue
// introspection): if all steps are in completedSet semantics
// (Completed / Skipped / Recovered), promote the run to
// Completed; if no step is in flight (Pending / Queued /
// Running) but IsComplete is false, mark the run Failed with a
// synthetic step state so operators see the wedge.
package engine

import (
	"context"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	reconcileInterval = 60 * time.Second
	reconcileMinAge   = 5 * time.Minute

	// reconcileWedgedReason is stamped on the synthetic
	// StepState used when forcing a wedged run to terminal
	// state. Visible in DLQ entries from the janitor sweep.
	reconcileWedgedReason = "wedged: no in-flight work and " +
		"no path to completion"
)

// reconcileMaxRunsScan caps the per-cycle workflow_runs scan.
// var rather than const for test injection — tests lower it to
// exercise the cap-hit transition logic without seeding 1000
// runs. Production callers must not mutate.
var reconcileMaxRunsScan = 1000

// startReconciler launches the periodic janitor goroutine.
// The loop exits when ctx is cancelled. Safe to call exactly
// once — the orchestrator's Start guards this with the cc nil
// check.
func (o *Orchestrator) startReconciler(ctx context.Context) {
	if ctx == nil {
		panic("startReconciler: ctx must not be nil")
	}
	go func() {
		ticker := time.NewTicker(reconcileInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.reconcileRunningRuns(ctx)
			}
		}
	}()
}

// reconcileRunningRuns walks the workflow_runs KV for entries
// stuck at RunStatusRunning and forces them to a terminal
// state when warranted. Skips runs younger than
// reconcileMinAge as a safety guard against in-flight
// dispatch races.
func (o *Orchestrator) reconcileRunningRuns(ctx context.Context) {
	if ctx == nil {
		panic("reconcileRunningRuns: ctx must not be nil")
	}

	runs, err := o.store.ListAll(ctx, reconcileMaxRunsScan)
	if err != nil {
		slog.ErrorContext(ctx,
			"reconciler: list runs", "error", err)
		return
	}
	o.logScanCapTransition(ctx, len(runs))
	cutoff := time.Now().Add(-reconcileMinAge)
	for _, run := range runs {
		if run.Status != dag.RunStatusRunning {
			continue
		}
		if run.CreatedAt.IsZero() ||
			run.CreatedAt.After(cutoff) {
			continue
		}
		o.reconcileOneRun(ctx, run.RunID)
	}
}

// logScanCapTransition emits the scan-cap log line at a level
// chosen by the cycle-over-cycle transition (#260):
//   - not-capped → capped: WARN (operator-visible cold start /
//     new saturation).
//   - capped → still-capped: DEBUG (steady state; would be pure
//     noise at WARN every cycle).
//   - capped → not-capped: INFO (recovery edge; operators see
//     when the backlog has drained).
//   - not-capped → not-capped: nothing.
//
// Mutates o.capHitPrev. Called once per reconcile cycle from
// the single reconciler goroutine.
func (o *Orchestrator) logScanCapTransition(
	ctx context.Context, runCount int,
) {
	capped := runCount >= reconcileMaxRunsScan
	switch {
	case capped && !o.capHitPrev:
		slog.WarnContext(ctx,
			"reconciler: scan hit cap; older runs may "+
				"not be reconciled this cycle",
			"cap", reconcileMaxRunsScan,
		)
	case capped && o.capHitPrev:
		slog.DebugContext(ctx,
			"reconciler: scan still at cap",
			"cap", reconcileMaxRunsScan,
		)
	case !capped && o.capHitPrev:
		slog.InfoContext(ctx,
			"reconciler: scan-cap cleared",
			"cap", reconcileMaxRunsScan,
			"runs", runCount,
		)
	}
	o.capHitPrev = capped
}

// reconcileOneRun inspects a single run under its per-run
// mutex, re-loads to get fresh state, re-checks predicates,
// and transitions the run to a terminal state when warranted.
func (o *Orchestrator) reconcileOneRun(
	ctx context.Context, runID string,
) {
	if runID == "" {
		panic("reconcileOneRun: runID must not be empty")
	}
	if ctx == nil {
		panic("reconcileOneRun: ctx must not be nil")
	}

	lock := o.getRunLock(runID)
	lock.Lock()
	defer lock.Unlock()

	wfDef, run, err := o.loadRunAndDef(ctx, runID)
	if err != nil {
		slog.ErrorContext(ctx,
			"reconciler: load run",
			"run_id", runID, "error", err)
		return
	}
	// Re-check status under lock — concurrent step completion
	// may have already advanced the run while we were waiting.
	if run.Status != dag.RunStatusRunning {
		return
	}

	if dag.IsComplete(wfDef, completedSet(run)) {
		o.reconcileComplete(ctx, run)
		return
	}
	if hasInFlightStep(run) {
		return
	}
	o.reconcileWedged(ctx, run)
}

// reconcileComplete promotes a run whose steps are all in
// completedSet semantics but whose Status was never advanced
// to Completed. Recovers from a missed completion event,
// which is the production-observed cause of #185.
func (o *Orchestrator) reconcileComplete(
	ctx context.Context, run dag.WorkflowRun,
) {
	if err := o.completeWorkflow(ctx, run); err != nil {
		slog.ErrorContext(ctx,
			"reconciler: complete wedged run",
			"run_id", run.RunID, "error", err)
		return
	}
	o.metrics.runsReconciled.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("outcome", "completed"),
		),
	)
}

// reconcileWedged force-fails a run that has no in-flight
// step and no path to IsComplete. Synthesizes a step
// reference so failWorkflow's downstream consumers
// (DLQ publish, parent notification) have something coherent
// to record.
func (o *Orchestrator) reconcileWedged(
	ctx context.Context, run dag.WorkflowRun,
) {
	syntheticStep := dag.StepDef{ID: "<reconciler>"}
	syntheticState := dag.StepState{
		Status: dag.StepStatusFailed,
		Error:  reconcileWedgedReason,
	}
	if err := o.failWorkflow(
		ctx, run, syntheticStep, syntheticState,
	); err != nil {
		slog.ErrorContext(ctx,
			"reconciler: fail wedged run",
			"run_id", run.RunID, "error", err)
		return
	}
	o.metrics.runsReconciled.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("outcome", "wedged_failed"),
		),
	)
	slog.WarnContext(ctx,
		"reconciler: forced wedged run to Failed",
		"run_id", run.RunID,
		"workflow_id", run.WorkflowID,
	)
}

// hasInFlightStep returns true if any step is in a state that
// implies live work: Pending (awaiting dispatch), Queued
// (dispatched, waiting for worker pickup), or Running (worker
// has started). Skipped/Cancelled/Recovered/Failed/Completed
// are terminal-ish from the dispatch perspective.
func hasInFlightStep(run dag.WorkflowRun) bool {
	for _, st := range run.Steps {
		switch st.Status {
		case dag.StepStatusPending,
			dag.StepStatusQueued,
			dag.StepStatusRunning:
			return true
		}
	}
	return false
}
