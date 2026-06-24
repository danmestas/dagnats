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
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go/jetstream"
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

// prunePassInterval is the cadence of the opt-in run-retention
// sweeper (#453). var rather than const for test injection —
// tests lower it so the background pass fires without waiting the
// production default. Production callers must not mutate.
var prunePassInterval = 10 * time.Minute

// pruneMaxRunsScan caps deletions per prune pass so a single sweep
// never blocks the goroutine on an unbounded delete storm. The next
// pass picks up where this one stopped.
const pruneMaxRunsScan = 10_000

// ephemeralDefPrefix is the ONLY def-key prefix the reaper may touch
// (#377). A scoped runtime def is keyed "agent.<root>.<name>"; promoted
// defs ("promoted.*") and ordinary author defs carry no agent. prefix,
// so the prefix gate alone renders them reaper-invisible.
const ephemeralDefPrefix = "agent."

// defReaperMaxScan bounds the workflow_defs key set a single reaper pass
// will tolerate. A def population beyond this points to a leak; we panic
// rather than silently degrade, mirroring runKeyScanMax.
const defReaperMaxScan = 1_000_000

// defReaperMaxDelete caps deletions per reaper pass so a single sweep
// never blocks the goroutine on an unbounded delete storm. The next pass
// picks up where this one stopped. Passed as a parameter to the pass fn
// (Ousterhout fix 5) rather than read from a mutable package var.
const defReaperMaxDelete = 10_000

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

// startRunPruner launches the opt-in run-retention sweeper
// goroutine (#453). The loop exits when ctx is cancelled (from
// Stop). Callers must only invoke this when o.runsMaxAge > 0 —
// Start guards that, so the ticker never runs in the default
// OFF-by-default posture.
func (o *Orchestrator) startRunPruner(ctx context.Context) {
	if ctx == nil {
		panic("startRunPruner: ctx must not be nil")
	}
	if o.runsMaxAge <= 0 {
		panic("startRunPruner: runsMaxAge must be positive")
	}
	go func() {
		ticker := time.NewTicker(prunePassInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.pruneTerminalRuns(ctx)
			}
		}
	}()
}

// pruneTerminalRuns runs one drop-only retention pass and logs the
// deleted count. Bounded by pruneMaxRunsScan deletions per pass.
func (o *Orchestrator) pruneTerminalRuns(ctx context.Context) {
	if ctx == nil {
		panic("pruneTerminalRuns: ctx must not be nil")
	}
	if o.runsMaxAge <= 0 {
		panic("pruneTerminalRuns: runsMaxAge must be positive")
	}
	deleted, err := o.store.PruneTerminal(
		ctx, o.runsMaxAge, pruneMaxRunsScan,
	)
	if err != nil {
		slog.ErrorContext(ctx,
			"pruner: prune terminal runs", "error", err)
		return
	}
	if deleted > 0 {
		slog.InfoContext(ctx,
			"pruner: dropped aged terminal runs",
			"deleted", deleted,
			"max_age", o.runsMaxAge.String(),
		)
	}
}

// startDefReaper launches the opt-in ephemeral-def reaper goroutine
// (#377). The loop exits when ctx is cancelled (from Stop). Callers must
// only invoke this when o.defReaperGrace > 0 — Start guards that AND the
// runsMaxAge >= defReaperGrace orphan-safety invariant, so the ticker
// never runs in the default OFF-by-default posture.
func (o *Orchestrator) startDefReaper(ctx context.Context) {
	if ctx == nil {
		panic("startDefReaper: ctx must not be nil")
	}
	if o.defReaperGrace <= 0 {
		panic("startDefReaper: defReaperGrace must be positive")
	}
	go func() {
		ticker := time.NewTicker(prunePassInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				o.reapEphemeralDefs(ctx, defReaperMaxDelete)
			}
		}
	}()
}

// reapEphemeralDefs runs one two-phase ephemeral-def GC pass (#377),
// mirroring the run-pruner's collect-then-delete shape. Phase 1 collects
// reapable keys (fail-safe: any read error aborts the pass with zero
// collected); phase 2 deletes them, bounded by maxDelete. Logs the count.
func (o *Orchestrator) reapEphemeralDefs(
	ctx context.Context, maxDelete int,
) {
	if ctx == nil {
		panic("reapEphemeralDefs: ctx must not be nil")
	}
	if maxDelete <= 0 {
		panic("reapEphemeralDefs: maxDelete must be positive")
	}
	doomed, err := o.collectReapable(ctx, maxDelete)
	if err != nil {
		slog.ErrorContext(ctx,
			"def-reaper: collect reapable defs", "error", err)
		return // phase 1 failed → zero deletions (fail-safe)
	}
	deleted := 0
	for _, key := range doomed {
		if err := o.defKV.Delete(ctx, key); err != nil {
			slog.ErrorContext(ctx,
				"def-reaper: delete def", "key", key, "error", err)
			break
		}
		deleted++
	}
	if deleted > 0 {
		slog.InfoContext(ctx,
			"def-reaper: dropped ephemeral defs",
			"deleted", deleted,
			"grace", o.defReaperGrace.String(),
		)
	}
}

// collectReapable is phase 1 of the def-reaper (#377): scan workflow_defs,
// select up to maxDelete agent.-prefixed keys whose tree-root run is
// terminal+grace-elapsed (or a true orphan). FAIL-SAFE, exactly like
// collectPrunable: on ANY read/load error it ABORTS the whole pass
// returning the error with ZERO collected — it never `continue`s past a
// bad read, so a transient store fault can never trigger a partial sweep.
func (o *Orchestrator) collectReapable(
	ctx context.Context, maxDelete int,
) ([]string, error) {
	if ctx == nil {
		panic("collectReapable: ctx must not be nil")
	}
	if maxDelete <= 0 {
		panic("collectReapable: maxDelete must be positive")
	}
	keys, err := o.defKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil // empty bucket → nothing to reap
		}
		return nil, err
	}
	if len(keys) > defReaperMaxScan {
		// A pathological key population is a fail-safe condition, not a
		// programmer error: return it through the pass's error contract so
		// the caller aborts with zero deletions rather than killing the
		// reaper goroutine (and the process) on a 1M-key bucket.
		return nil, fmt.Errorf(
			"def key set exceeds bound (%d)", defReaperMaxScan)
	}
	doomed := make([]string, 0, maxDelete)
	for _, key := range keys {
		if len(doomed) >= maxDelete {
			break
		}
		root, ok := rootFromDefKey(key)
		if !ok {
			continue // non-agent. or malformed → never swept
		}
		reap, err := o.defShouldBeReaped(ctx, root)
		if err != nil {
			return nil, err // ANY load error → abort WHOLE pass
		}
		if reap {
			doomed = append(doomed, key)
		}
	}
	return doomed, nil
}

// rootFromDefKey parses an ephemeral def key "agent.<root>.<name>" and
// returns its <root>. ok is false for any non-conforming key — a missing
// agent. prefix, an empty root, or fewer than three dot-separated
// segments — so such keys are never swept. Pure, total, no panic (#377).
func rootFromDefKey(key string) (string, bool) {
	if !strings.HasPrefix(key, ephemeralDefPrefix) {
		return "", false
	}
	rest := strings.TrimPrefix(key, ephemeralDefPrefix)
	// rest must be "<root>.<name>" with a non-empty root and a non-empty
	// name; SplitN(.,2) isolates the root as the first segment.
	parts := strings.SplitN(rest, ".", 2)
	if len(parts) != 2 {
		return "", false
	}
	root, name := parts[0], parts[1]
	if root == "" || name == "" {
		return "", false
	}
	return root, true
}

// defShouldBeReaped decides whether the def for tree-root `root` is GC
// eligible (#377). Load the root run:
//   - ErrRunNotFound → true orphan; under the Start-time invariant
//     (runsMaxAge >= defReaperGrace) a missing root means BOTH the run-
//     retention and def-grace windows elapsed, so it is safe to reap.
//   - other load error → propagate (collect phase aborts the pass).
//   - ParentRunID != "" → never sweep a non-root (defense in depth).
//   - !IsTerminal() → root still live, keep.
//   - CompletedAt == nil → terminal but unstamped, keep.
//   - else → reap iff time since completion exceeds the grace.
func (o *Orchestrator) defShouldBeReaped(
	ctx context.Context, root string,
) (bool, error) {
	if ctx == nil {
		panic("defShouldBeReaped: ctx must not be nil")
	}
	if root == "" {
		panic("defShouldBeReaped: root must not be empty")
	}
	run, err := o.store.Load(ctx, root)
	if err != nil {
		if errors.Is(err, ErrRunNotFound) {
			return true, nil
		}
		return false, err
	}
	if run.ParentRunID != "" {
		return false, nil
	}
	if !run.Status.IsTerminal() {
		return false, nil
	}
	if run.CompletedAt == nil {
		return false, nil
	}
	return time.Since(*run.CompletedAt) > o.defReaperGrace, nil
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
