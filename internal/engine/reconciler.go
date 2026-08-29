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

	// releaseAttemptsMax bounds reconcileReleasePending's recovery
	// attempts per run (#648 PR review round 3). Without a bound, a
	// run whose release keeps failing for a structural (not
	// transient) reason would retry -- and WARN-log -- every
	// reconciler pass forever. Reaching the cap abandons the run:
	// ReleasePending is cleared, one ERROR is logged, and
	// engine.finalize.release_abandoned is incremented, rather than
	// retrying unboundedly.
	releaseAttemptsMax = 10
)

// reconcileMaxRunsScan caps the per-cycle workflow_runs scan.
// var rather than const for test injection — tests lower it to
// exercise the cap-hit transition logic without seeding 1000
// runs. Production callers must not mutate.
var reconcileMaxRunsScan = 1000

// pendingRunScanMax caps the run.* GETs findOldestPendingRun issues
// on each run completion (#523). Without it, a per-completion scan of a
// 228k-key workflow_runs bucket parallel-GETs the entire population just
// to find one pending run — one slow GET then stalls ALL execution. var
// for test injection; production callers must not mutate.
var pendingRunScanMax = 1000

// bestEffortGetTimeout bounds each KV GET in the best-effort scans
// (findOldestPendingRun, ListAll) so a single slow key cannot wedge a
// whole tick. Well under nats.go's 5s default so a degraded bucket
// degrades quickly and observably rather than blocking.
const bestEffortGetTimeout = 2 * time.Second

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
				o.runRepairRunIndexPass(ctx)
			}
		}
	}()
}

// runRepairRunIndexPass runs one bounded RepairRunIndex call and logs
// + counts the outcome when nonzero (#659). Called once per
// reconciler tick and once, synchronously, from Orchestrator.Start
// before the history consumer begins processing, so a store upgraded
// from a pre-#659 build has ordered-scan visibility into its existing
// runs immediately rather than only after the first tick.
func (o *Orchestrator) runRepairRunIndexPass(ctx context.Context) {
	if ctx == nil {
		panic("runRepairRunIndexPass: ctx must not be nil")
	}
	if o.store == nil {
		panic("runRepairRunIndexPass: store must not be nil")
	}
	stats, err := o.store.RepairRunIndex(ctx, repairPageMax)
	if err != nil {
		slog.ErrorContext(ctx,
			"reconciler: repair run index", "error", err)
		return
	}
	if stats.Repaired == 0 && stats.OrphansRemoved == 0 {
		return
	}
	slog.InfoContext(ctx, "reconciler: repaired run index",
		"repaired", stats.Repaired,
		"orphans_removed", stats.OrphansRemoved,
	)
	o.metrics.runIndexRepaired.Add(ctx, int64(stats.Repaired))
	o.metrics.runIndexOrphans.Add(ctx, int64(stats.OrphansRemoved))
}

// repairRunIndexMaxIterations bounds repairRunIndexToConvergenceWith's
// loop. At repairPageMax entries handled per RepairRunIndex call,
// this many iterations cover a population up to runKeyScanMax with
// headroom for runs created concurrently with the loop. Exceeding it
// is an OPERATING condition -- a store too large or too actively
// written to converge in this bound -- not a programmer error, so
// repairRunIndexToConvergenceWith returns an error rather than
// panicking (review round 2).
const repairRunIndexMaxIterations = runKeyScanMax/repairPageMax + 100

// repairRunIndexRetryBackoff is the delay schedule between retries of
// a single failing RepairRunIndex call: 5 attempts total, 200ms then
// doubling to 3.2s. A production failure here is almost always
// transient (a momentary KV hiccup) and worth a few seconds of retry
// before giving up -- but giving up must still happen, and loudly
// (see repairRunIndexOnceWithRetry).
var repairRunIndexRetryBackoff = []time.Duration{
	200 * time.Millisecond, 400 * time.Millisecond, 800 * time.Millisecond,
	1600 * time.Millisecond, 3200 * time.Millisecond,
}

// runIndexRepairer is the narrow store surface
// repairRunIndexToConvergenceWith needs -- *SnapshotStore satisfies it
// via RepairRunIndex. The seam exists so the retry/convergence LOGIC
// is unit-testable against a scriptable fake instead of requiring a
// real NATS server to exercise transient-failure and non-convergence
// paths (review round 2).
type runIndexRepairer interface {
	RepairRunIndex(ctx context.Context, pageMax int) (RepairStats, error)
}

// repairRunIndexOnceWithRetry calls repairer.RepairRunIndex, retrying
// on error per the backoff schedule (one call, then len(backoff)
// retries) before giving up. Returns the error from the LAST attempt,
// wrapped, if every attempt failed.
func repairRunIndexOnceWithRetry(
	ctx context.Context, repairer runIndexRepairer,
	pageMax int, backoff []time.Duration,
) (RepairStats, error) {
	var lastErr error
	for attempt := 0; attempt <= len(backoff); attempt++ {
		stats, err := repairer.RepairRunIndex(ctx, pageMax)
		if err == nil {
			return stats, nil
		}
		lastErr = err
		if attempt >= len(backoff) {
			break
		}
		select {
		case <-time.After(backoff[attempt]):
		case <-ctx.Done():
			return RepairStats{}, ctx.Err()
		}
	}
	return RepairStats{}, fmt.Errorf(
		"run index repair failed after %d attempts: %w",
		len(backoff)+1, lastErr,
	)
}

// repairRunIndexToConvergenceWith loops repairRunIndexOnceWithRetry
// until a pass reports nothing left to do (Repaired==0 AND
// OrphansRemoved==0), bounded by maxIterations convergence passes.
// Pure and store-agnostic (parameterized on runIndexRepairer) so it is
// unit-testable without a real NATS server. Returns an error -- NEVER
// panics -- when either a single pass exhausts its retry budget or
// the iteration bound is hit without converging: both are OPERATING
// conditions (a transient KV blip, or a store too large/too actively
// written to converge in the configured bound), not programmer
// errors (review round 2).
func repairRunIndexToConvergenceWith(
	ctx context.Context, repairer runIndexRepairer,
	pageMax, maxIterations int, backoff []time.Duration,
) (totalRepaired, totalOrphans, passes int, err error) {
	if repairer == nil {
		panic("repairRunIndexToConvergenceWith: repairer must not be nil")
	}
	if pageMax <= 0 {
		panic("repairRunIndexToConvergenceWith: pageMax must be positive")
	}
	if maxIterations <= 0 {
		panic("repairRunIndexToConvergenceWith: maxIterations must be positive")
	}
	for ; passes < maxIterations; passes++ {
		stats, callErr := repairRunIndexOnceWithRetry(ctx, repairer, pageMax, backoff)
		if callErr != nil {
			return totalRepaired, totalOrphans, passes, callErr
		}
		totalRepaired += stats.Repaired
		totalOrphans += stats.OrphansRemoved
		if stats.Repaired == 0 && stats.OrphansRemoved == 0 {
			return totalRepaired, totalOrphans, passes + 1, nil
		}
	}
	return totalRepaired, totalOrphans, passes, fmt.Errorf(
		"run index did not converge within %d passes"+
			" (repaired=%d orphans_removed=%d)",
		maxIterations, totalRepaired, totalOrphans,
	)
}

// repairRunIndexToConvergence is the Orchestrator.Start entry point
// (review round: a single bounded pass left a store with more than
// repairPageMax unindexed runs -- e.g. a pre-#659 upgrade -- with its
// backlog only partially backfilled; any run Saved before a LATER
// reconciler tick finished the rest would sit newer in the index than
// still-unindexed old runs, so ScanNewestFirst would return stale
// runs as "newest" until that tick caught up, and any run repaired
// mid-backlog would land in the WRONG relative position permanently).
// It calls repairRunIndexToConvergenceWith and returns its error
// UNCHANGED (wrapped) so Start fails loudly and never opens the
// history consumer over a partially-converged index (review round 2).
// The reconciler's periodic tick deliberately keeps the single
// bounded pass (runRepairRunIndexPass) — it only needs to close small
// gaps left by an occasional lost writeRunIndexEntry race, not
// converge a whole backlog under the tick's time budget.
//
// Known limitation (HA): if a second orchestrator starts against the
// same store while a live peer is actively Saving runs, this loop's
// snapshot-then-diff (loadRunAndIndexIDSets) can race that peer's
// writes -- a run created between the diff and the backfill Create
// looks "missing" on this pass and simply gets caught on the next
// one. Not unsafe (RepairRunIndex's Create is idempotent and a
// pending pass just repeats), but not accounted for in the passes
// bound either; today's deployment model is single-orchestrator, so
// this is a documented gap, not a fix.
func (o *Orchestrator) repairRunIndexToConvergence(ctx context.Context) error {
	if ctx == nil {
		panic("repairRunIndexToConvergence: ctx must not be nil")
	}
	if o.store == nil {
		panic("repairRunIndexToConvergence: store must not be nil")
	}
	totalRepaired, totalOrphans, passes, err := repairRunIndexToConvergenceWith(
		ctx, o.store, repairPageMax,
		repairRunIndexMaxIterations, repairRunIndexRetryBackoff,
	)
	if err != nil {
		return fmt.Errorf("startup: repair run index: %w", err)
	}
	if totalRepaired == 0 && totalOrphans == 0 {
		return nil
	}
	slog.InfoContext(ctx, "startup: repaired run index to convergence",
		"repaired", totalRepaired,
		"orphans_removed", totalOrphans,
		"passes", passes,
	)
	o.metrics.runIndexRepaired.Add(ctx, int64(totalRepaired))
	o.metrics.runIndexOrphans.Add(ctx, int64(totalOrphans))
	return nil
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
		if run.Status == dag.RunStatusRunning {
			if run.CreatedAt.IsZero() ||
				run.CreatedAt.After(cutoff) {
				continue
			}
			o.reconcileOneRun(ctx, run.RunID)
			continue
		}
		// Terminal runs whose admission release didn't complete at
		// finalize time (#648) surface here too -- same sweep, same
		// once-per-pass bound (reconcileMaxRunsScan), just a different
		// predicate than the wedged-Running check above.
		if run.Status.IsTerminal() && run.ReleasePending {
			o.reconcileReleasePending(ctx, run)
		}
	}
}

// reconcileReleasePending retries the admission release for a
// terminal run whose finalizeRun left WorkflowRun.ReleasePending set
// (#648) -- afterPersist failed after the terminal snapshot was
// already saved, so the singleton lock / concurrency slot is stuck
// and no ordinary redelivery will ever retry it (the caller's own
// Status != Running early-return fires first). One attempt per run
// per reconciler pass: on failure, reconcileReleaseFailed records the
// attempt and either lets this run be seen again next cycle or, past
// releaseAttemptsMax, abandons it; on success the flag is cleared
// with a save so it is not retried forever.
//
// Idempotent with the normal finalize path by construction:
// releaseAdmission's own idempotence (see its doc comment) means a
// run that actually already released cleanly -- e.g. this run's debt
// was recorded, then ALSO released successfully before this sweep ran
// -- is safe to release again here; nothing double-frees.
//
// Best-effort, not a guarantee: reconcileRunningRuns' ListAll scan is
// capped at reconcileMaxRunsScan and unordered, so a ReleasePending
// run outside that window is simply not seen this pass (self-heals
// next pass once older runs rotate out, same bound
// findOldestPendingRun already documents) -- and PruneTerminal (#453,
// opt-in retention) can delete a terminal run, ReleasePending flag and
// all, before this sweep ever reaches it, in which case the leak is
// no longer visible to recover at all. Neither failure mode is new
// here; both already bound every other terminal-run scan this
// reconciler runs.
//
// Not ReleasePending-first ordered: ListAll caps by KV key count
// before any value is fetched, and ReleasePending lives inside each
// run's JSON value, not the key -- prioritizing it would mean
// fetching every run's value up front just to sort, which defeats
// the whole point of capping the scan.
func (o *Orchestrator) reconcileReleasePending(
	ctx context.Context, run dag.WorkflowRun,
) {
	if ctx == nil {
		panic("reconcileReleasePending: ctx must not be nil")
	}
	if !run.Status.IsTerminal() {
		panic("reconcileReleasePending: run must be terminal")
	}
	// releaseAdmission panics on an empty RunID/WorkflowID -- a
	// programmer-error invariant at ITS call boundary, appropriate
	// for callers that construct run in-process. This sweep instead
	// loads run from the KV, so a corrupt/malformed snapshot is
	// attacker-adjacent input, not a programmer error: validate here
	// and skip rather than let one bad snapshot panic the reconcile
	// goroutine (#648 PR review round 4).
	if run.RunID == "" || run.WorkflowID == "" {
		slog.ErrorContext(ctx,
			"reconciler: skipping release-pending recovery for a "+
				"malformed run (empty RunID or WorkflowID)",
			"run_id", run.RunID,
			"workflow_id", run.WorkflowID,
		)
		if finalizeReleaseMalformedSkipped != nil {
			finalizeReleaseMalformedSkipped.Add(ctx, 1)
		}
		return
	}
	if err := o.releaseAdmission(ctx, run); err != nil {
		o.reconcileReleaseFailed(ctx, run, err)
		return
	}
	run.ReleasePending = false
	run.ReleaseAttempts = 0
	if err := o.saveSnapshot(ctx, run, ""); err != nil {
		// The release itself succeeded; only the flag-clear write
		// failed. Documented residual window (same shape as
		// finalizeWithReleaseDebt's own second-save failure,
		// run_event.go): the NEXT pass sees ReleasePending still
		// true and retries releaseAdmission, which is safe (see its
		// idempotence doc) but redundant.
		slog.ErrorContext(ctx,
			"reconciler: clear release_pending after successful "+
				"recovery failed",
			"run_id", run.RunID,
			"workflow_id", run.WorkflowID,
			"error", err,
		)
		return
	}
	slog.InfoContext(ctx,
		"reconciler: recovered admission release for terminal run",
		"run_id", run.RunID,
		"workflow_id", run.WorkflowID,
	)
	if finalizeReleaseRecovered != nil {
		finalizeReleaseRecovered.Add(ctx, 1)
	}
}

// reconcileReleaseFailed handles one failed releaseAdmission attempt
// for a ReleasePending run: increments and persists ReleaseAttempts,
// then either leaves the run flagged for another pass (WARN) or, past
// releaseAttemptsMax, abandons it -- clears ReleasePending so the
// sweep stops touching it, logs once at ERROR, and increments
// engine.finalize.release_abandoned (#648 PR review round 3). Split
// out of reconcileReleasePending to keep that function under
// TigerStyle's 70-line limit.
//
// If the attempts-count save itself fails, the count simply doesn't
// advance this pass -- the next pass re-attempts the release and
// tries to persist the count again. This cannot loop forever any
// worse than the underlying save failure already can elsewhere; it is
// not a new unbounded-retry path.
func (o *Orchestrator) reconcileReleaseFailed(
	ctx context.Context, run dag.WorkflowRun, releaseErr error,
) {
	if ctx == nil {
		panic("reconcileReleaseFailed: ctx must not be nil")
	}
	if releaseErr == nil {
		panic("reconcileReleaseFailed: releaseErr must not be nil")
	}
	run.ReleaseAttempts++
	abandon := run.ReleaseAttempts >= releaseAttemptsMax
	if abandon {
		run.ReleasePending = false
	}
	if err := o.saveSnapshot(ctx, run, ""); err != nil {
		slog.ErrorContext(ctx,
			"reconciler: persist release_attempts after failed "+
				"recovery failed",
			"run_id", run.RunID,
			"workflow_id", run.WorkflowID,
			"attempts", run.ReleaseAttempts,
			"error", err,
		)
		return
	}
	if !abandon {
		slog.WarnContext(ctx,
			"reconciler: release-pending recovery failed, will retry "+
				"next pass",
			"run_id", run.RunID,
			"workflow_id", run.WorkflowID,
			"attempts", run.ReleaseAttempts,
			"error", releaseErr,
		)
		return
	}
	slog.ErrorContext(ctx,
		"reconciler: abandoning release-pending recovery after "+
			"max attempts -- admission slot/lock may remain leaked",
		"run_id", run.RunID,
		"workflow_id", run.WorkflowID,
		"attempts", run.ReleaseAttempts,
		"error", releaseErr,
	)
	if finalizeReleaseAbandoned != nil {
		finalizeReleaseAbandoned.Add(ctx, 1)
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
