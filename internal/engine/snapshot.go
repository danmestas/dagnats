package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrRunNotFound is returned by Load when no snapshot exists for the given run ID.
// Callers can distinguish missing-run from other NATS errors with errors.Is.
var ErrRunNotFound = errors.New("workflow run not found")

// runKeyScanMax bounds the workflow_runs key set a single ListAll /
// CountAll / RepairRunIndex scan will tolerate. A run population beyond
// this points to missing retention (#453); we panic rather than
// silently degrade.
const runKeyScanMax = 1_000_000

// runIndexPrefix names the creation-ordered index key space (#659).
// "runidx.<runID>" mirrors "run.<runID>" but is written EXACTLY ONCE,
// via Create, the first time a run's snapshot key exists -- so its
// revision never changes and ListKeysFiltered's replay order over
// "runidx.>" is therefore the true run-creation order, unlike Keys()
// on "run.*" (which sorts lexicographically and carries no time
// signal at all).
const runIndexPrefix = "runidx."

// scanBatchSize bounds a single ParallelGetJSBestEffort call inside
// ScanNewestFirst: large enough to amortize round trips, small enough
// that a match near the front of a deep scan isn't stuck waiting on a
// full ScanFetchMax batch of unrelated fetches.
const scanBatchSize = 256

// ScanFetchMax is the hard ceiling on values ScanNewestFirst will ever
// fetch in a single call, regardless of the caller-supplied fetchMax --
// defense against a caller passing a fetchMax so large it amounts to
// "fetch everything."
const ScanFetchMax = 10_000

// runActivePrefix names the liveness index key space (#664). The
// invariant (review round 4): "runactive.<runID>" is present IFF the
// reconciler still owns runID -- run is non-terminal, OR
// run.ReleasePending is true (a terminal run whose admission release
// failed and is owed a retry, #648). One marker, one predicate: there
// is no separate index for the release-debt case (an earlier version
// of this had one, "runpending" -- removed; see the RepairStats and
// ListActive doc comments for why the unified invariant is simpler
// AND safer than tracking the two cases with separate markers that
// can drift out of sync with each other).
//
// Created the moment a run's first snapshot is written
// (createEntryIndexes, same choke point that creates runidx.<runID>,
// only if the run starts non-terminal -- a run is never created
// already ReleasePending). Deleted by finalizeRun the moment a run is
// persisted terminal WITHOUT an owed release (#625's funnel), or by
// the reconciler once an owed release is recovered or abandoned
// (reconciler.go) -- see finalizeRun's and reconcileReleasePending's
// doc comments. Unlike runidx (write-once, never removed), runactive
// tracks LIVE ownership, so ListKeysFiltered over "runactive.>" is
// bounded by the number of runs the reconciler currently owns --
// which admission/concurrency limits (for the non-terminal case) and
// the rarity of release failures (for the ReleasePending case) both
// bound -- regardless of how large the total run.* population grows.
const runActivePrefix = "runactive."

// activeOrphanGrace bounds how recently a terminal, not-ReleasePending
// run must have completed before repairActiveOrphans trusts its
// runactive marker is genuinely stale rather than possibly mid-finalize
// (#664 review round 5, reviewer-reproduced). finalizeRun's two saves --
// the terminal saveFn call, then (on an afterPersist failure)
// finalizeWithReleaseDebt's ReleasePending=true save -- fall inside this
// window: between them, the persisted run is genuinely terminal AND
// ReleasePending is still false, so isReconcilerOwned correctly says
// "not owned" even though the marker is about to become correct again
// milliseconds later. Set to the reconciler's own tick interval: any
// finalize that has not completed its second save within one full tick
// is not "unlucky timing" anymore, and the marker is genuinely
// removable. See repairActiveOrphans and finalizeWithReleaseDebt's doc
// comments for the read-side/write-side halves of the fix.
const activeOrphanGrace = reconcileInterval

// ActiveFetchMax is the hard ceiling on values ListActive will ever fetch
// in a single call, regardless of the caller-supplied fetchMax -- defense
// in depth mirroring ScanFetchMax, even though the active-run population
// is expected to stay far below it in practice (bounded by admission).
const ActiveFetchMax = 10_000

// repairPageMax bounds how many index entries a single RepairRunIndex
// call will backfill or remove. A store with more damage than that
// needs more than one call -- see RepairRunIndex's doc comment.
const repairPageMax = 1000

// SnapshotStore persists and retrieves WorkflowRun state in the NATS KV store.
// The workflow_runs bucket must exist before NewSnapshotStore is called.
type SnapshotStore struct {
	kv jetstream.KeyValue
}

// NewSnapshotStore binds a SnapshotStore to the workflow_runs KV bucket.
// Panics if the bucket has not been created — callers must call SetupKVBuckets first.
func NewSnapshotStore(js jetstream.JetStream) *SnapshotStore {
	if js == nil {
		panic("NewSnapshotStore: JetStream must not be nil")
	}
	kv, err := js.KeyValue(
		context.Background(), "workflow_runs",
	)
	if err != nil {
		panic("NewSnapshotStore: workflow_runs bucket not found: " +
			err.Error())
	}
	return &SnapshotStore{kv: kv}
}

// Save serializes the WorkflowRun and writes it to the KV store under key "run.<RunID>".
// Overwrites any existing entry — callers are responsible for optimistic concurrency if needed.
//
// Save touches NEITHER derived index (#664 review round 2). Index
// writes happen exactly once, at genuine creation -- SaveInitial or
// CreateSnapshot -- so the hot per-advance save path (every step
// completion, every mid-run status change) costs zero extra KV round
// trips. The terminal-transition delete of runactive is issued
// explicitly by finalizeRun, the single funnel every terminal
// transition routes through (#625) -- NOT inferred here from
// run.Status, which would put an index write back on every save
// regardless of whether liveness actually changed.
func (s *SnapshotStore) Save(ctx context.Context, run dag.WorkflowRun) error {
	if run.RunID == "" {
		panic("SnapshotStore.Save: RunID must not be empty")
	}
	if s.kv == nil {
		panic("SnapshotStore.Save: kv bucket must not be nil")
	}
	data, err := json.Marshal(run)
	if err != nil {
		return err
	}
	_, err = s.kv.Put(
		ctx, "run."+run.RunID, data,
	)
	return err
}

// SaveInitial is Save's counterpart for a run's GENUINE first
// persistence via the Put-based admission path (createOrHealRun's
// non-terminal branch -- every trigger type except run_terminal chain
// starts, which use CreateSnapshot's atomic Create instead). Unlike
// Save, SaveInitial ALSO writes both derived indexes (#664): this is
// the only other place a "run." key transitions from not-existing to
// existing, so it is the only other place the indexes need creating.
//
// Callers must only call this for a run they know has no existing
// snapshot (createOrHealRun's early guard already proves this for its
// caller) -- SaveInitial does not itself check. Calling it on an
// existing run's advance would not be incorrect (Put still overwrites,
// the index writes are idempotent Creates) but would waste two KV
// round trips the design intends to avoid outside genuine creation.
func (s *SnapshotStore) SaveInitial(ctx context.Context, run dag.WorkflowRun) error {
	if run.RunID == "" {
		panic("SnapshotStore.SaveInitial: RunID must not be empty")
	}
	if s.kv == nil {
		panic("SnapshotStore.SaveInitial: kv bucket must not be nil")
	}
	data, err := json.Marshal(run)
	if err != nil {
		return err
	}
	if _, err := s.kv.Put(ctx, "run."+run.RunID, data); err != nil {
		return err
	}
	return s.createEntryIndexes(ctx, run)
}

// writeRunIndexEntry Create-writes the "runidx.<runID>" marker for a
// run whose "run.<runID>" snapshot key was JUST written successfully.
// Called only at genuine creation (SaveInitial, CreateSnapshot, and
// the crash-gap/full-build repair passes) -- Create is the mechanism
// that makes "written once" hold: a repeat call loses the race with
// ErrKeyExists and is a silent no-op, so the marker's revision is
// forever its creation revision regardless of which write happened to
// be first.
//
// A non-ErrKeyExists failure here is logged and swallowed, NOT
// propagated: the run snapshot -- the source of truth -- already
// landed, and failing the whole save over a derived index write would
// make the index MORE load-bearing than the data it derives from.
// RepairRunIndex backfills a run that lost this race against a crash
// or a transient KV error (the bounded-visibility window #659
// documents).
func (s *SnapshotStore) writeRunIndexEntry(ctx context.Context, runID string) {
	if runID == "" {
		panic("writeRunIndexEntry: runID must not be empty")
	}
	if s.kv == nil {
		panic("writeRunIndexEntry: kv bucket must not be nil")
	}
	_, err := s.kv.Create(ctx, runIndexPrefix+runID, []byte{})
	if err == nil || errors.Is(err, jetstream.ErrKeyExists) {
		return
	}
	slog.WarnContext(ctx,
		"run index write failed; RepairRunIndex will backfill it",
		"run_id", runID,
		"error", err,
	)
}

// createEntryIndexes writes BOTH derived per-run indexes for a run
// being created RIGHT NOW -- runactive (#664, liveness), FIRST and
// only if isReconcilerOwned(run) says so (equivalent to non-terminal at
// genuine creation time -- a run is never created already
// ReleasePending -- but routed through the shared predicate rather than
// spelling it out by hand, so this call site cannot drift from
// ListActive/repairActiveOrphans/buildActiveIndexOnce about what
// "owned" means), THEN runidx (#659, write-once, creation order). This
// is the SOLE creation-time choke point:
// SaveInitial, CreateSnapshot's success path, and the repair passes
// (crash-gap backfill and the one-time full build) all route through
// it. It is deliberately NOT called from Save -- see Save's doc
// comment for why index writes must not ride the per-advance path.
//
// Order is load-bearing (review round 3): runactive MUST be written
// before runidx, and its error MUST propagate (not be swallowed),
// because RepairRunIndex's crash-gap backfill (backfillMissingIndex)
// keys ENTIRELY off "run has a snapshot but no runidx entry" --
// repairActiveOrphans only ever VALIDATES existing runactive markers,
// it never creates one. So the ONLY way a non-terminal run's runactive
// gap is ever healed is by still being caught in the runidx-missing
// crash-gap set. Writing runidx first would let a lost/failed
// runactive Create (or a crash between the two writes) leave a
// RUNNING run with runidx present and runactive missing forever --
// repair would see nothing to do (runidx already exists) and
// ListActive would never find it. Writing runactive first and
// propagating its error means the ONLY way runidx can exist without a
// matching runactive is if the run is genuinely terminal (correct) --
// any other gap is caught because runidx is what's missing, and
// backfillMissingIndex recreates BOTH together.
func (s *SnapshotStore) createEntryIndexes(ctx context.Context, run dag.WorkflowRun) error {
	if run.RunID == "" {
		panic("createEntryIndexes: RunID must not be empty")
	}
	if s.kv == nil {
		panic("createEntryIndexes: kv bucket must not be nil")
	}
	if isReconcilerOwned(run) {
		if err := s.createActiveEntry(ctx, run.RunID); err != nil {
			return fmt.Errorf("create active-run index entry: %w", err)
		}
	}
	s.writeRunIndexEntry(ctx, run.RunID)
	return nil
}

// createActiveEntry Create-writes "runactive.<runID>". Idempotent: a
// repeat Create on a run that is still active loses the race with
// ErrKeyExists and no-ops, so calling this on every non-terminal
// write is always safe. Unlike writeRunIndexEntry, a non-ErrKeyExists
// failure here is NOT swallowed -- see createEntryIndexes' doc
// comment for why: there is no backfill path for a missing runactive
// entry independent of runidx also being missing, so silently
// swallowing this error would let the marker simply never exist. The
// caller's write fails loudly instead, so a redelivery/retry gets
// another chance at both writes.
func (s *SnapshotStore) createActiveEntry(ctx context.Context, runID string) error {
	if runID == "" {
		panic("createActiveEntry: runID must not be empty")
	}
	if s.kv == nil {
		panic("createActiveEntry: kv bucket must not be nil")
	}
	_, err := s.kv.Create(ctx, runActivePrefix+runID, []byte{})
	if err == nil || errors.Is(err, jetstream.ErrKeyExists) {
		return nil
	}
	return err
}

// deleteActiveEntry removes "runactive.<runID>" -- called only when
// the reconciler no longer owns runID (review round 4 invariant: run
// is terminal AND ReleasePending is false). THREE call sites:
// finalizeRun (#625's funnel), right after it persists a run terminal
// with NO owed release; reconcileReleasePending/reconcileReleaseFailed
// (reconciler.go), right after they clear an owed release (recovered
// or abandoned); and, defensively, PruneTerminal, for a run pruned
// without ever going through either path (a legacy/direct write).
// Idempotent at the NATS layer (deleting an absent key is not an
// error), so a redundant call costs nothing. A failure is logged and
// swallowed -- RepairRunIndex's active-index pass removes any entry
// this leaves stale.
func (s *SnapshotStore) deleteActiveEntry(ctx context.Context, runID string) {
	if runID == "" {
		panic("deleteActiveEntry: runID must not be empty")
	}
	if s.kv == nil {
		panic("deleteActiveEntry: kv bucket must not be nil")
	}
	if err := s.kv.Delete(ctx, runActivePrefix+runID); err != nil {
		slog.WarnContext(ctx,
			"active-run index delete failed; RepairRunIndex will remove it",
			"run_id", runID,
			"error", err,
		)
	}
}

// CreateSnapshot atomically persists run's FULL, final initial state
// via KV CREATE (not Put) -- the FIRST caller to succeed wins; every
// other caller, including one racing at the same instant, gets
// created=false and MUST NOT treat that as "nothing to do." #634
// review round 2: an earlier version of this claimed a separate,
// minimal PLACEHOLDER key before doing any of the real
// validation/admission work, so an error or crash between the claim
// and the real Save stranded that placeholder forever (a redelivery
// saw claimed=false and silently ack'd, losing the run). There is no
// separate claim step anymore -- the CALLER builds the complete run
// (through admission) first, and THIS call is simultaneously the
// claim and the real, final initial write. A caller that loses the
// race must load the existing row and resume from it (see
// handleWorkflowStarted's createOrHealRun) rather than skip, since
// "already exists" can mean either "a concurrent/earlier delivery
// fully succeeded" or "an earlier delivery saved but crashed before
// finishing (e.g. before enqueueReady)."
//
// This exists for run-ID schemes that are DETERMINISTIC across
// redeliveries -- today, exclusively internal/trigger's run_terminal
// chain-start run IDs (runTerminalChainRunID). A time-bounded publish
// dedup window (WORKFLOW_HISTORY's Nats-Msg-Id Duplicates, a few
// seconds) cannot survive a crash/restart gap of minutes; an atomic
// KV write can, because it has no expiry. Every OTHER trigger type
// still mints a fresh run ID per fire and relies on the existing
// Load-then-skip idempotency guard in handleWorkflowStarted (#196) --
// that guard is correct for THEM because a genuine redelivery of the
// exact same stored workflow.started message always carries the same
// RunID; CreateSnapshot only matters when two SEPARATE publish
// attempts might independently compute the same target RunID.
func (s *SnapshotStore) CreateSnapshot(
	ctx context.Context, run dag.WorkflowRun,
) (bool, error) {
	if run.RunID == "" {
		panic("SnapshotStore.CreateSnapshot: RunID must not be empty")
	}
	if s.kv == nil {
		panic("SnapshotStore.CreateSnapshot: kv bucket must not be nil")
	}
	data, err := json.Marshal(run)
	if err != nil {
		return false, err
	}
	_, err = s.kv.Create(ctx, "run."+run.RunID, data)
	if err == nil {
		if idxErr := s.createEntryIndexes(ctx, run); idxErr != nil {
			return false, idxErr
		}
		return true, nil
	}
	if errors.Is(err, jetstream.ErrKeyExists) {
		return false, nil
	}
	return false, err
}

// Delete removes the snapshot for the given run ID under key "run.<RunID>".
// Idempotent at the NATS layer — deleting an absent key is not an error.
// Drop-only retention (#453) is built on this; there is no archive path.
func (s *SnapshotStore) Delete(ctx context.Context, runID string) error {
	if s.kv == nil {
		panic("SnapshotStore.Delete: kv bucket must not be nil")
	}
	if runID == "" {
		panic("SnapshotStore.Delete: runID must not be empty")
	}
	return s.kv.Delete(ctx, "run."+runID)
}

// PruneTerminal is the opt-in, drop-only run-retention sweep (#453). It
// deletes a run ONLY IF it is terminal, its CompletedAt is strictly
// older than olderThan, AND it is not ReleasePending (#664 review round
// 5 -- see isPrunable). Non-terminal runs (even ancient ones), terminal
// runs younger than the window, and terminal runs still owing an
// admission release are never touched. At most maxPrune runs are
// deleted per call; the key scan is bounded by runKeyScanMax. Returns the
// number of runs deleted.
//
// Callers must guarantee retention is enabled (olderThan > 0) before
// invoking — both bounds are asserted as programmer errors.
//
// Fail-safe by construction: it runs in two phases. Phase one scans keys
// and loads each candidate to build a bounded delete list (≤ maxPrune),
// returning an error BEFORE any deletion if a value is corrupt or a Get
// fails. Phase two then deletes the collected keys. So a corrupt run.*
// value aborts the whole pass with zero deletions, regardless of scan
// order — the sweeper never commits a partial prune. The candidate buffer
// is bounded by maxPrune, so the full ~146k-value population is never
// materialized at once.
func (s *SnapshotStore) PruneTerminal(
	ctx context.Context, olderThan time.Duration, maxPrune int,
) (int, error) {
	if olderThan <= 0 {
		panic("SnapshotStore.PruneTerminal: olderThan must be positive")
	}
	if maxPrune <= 0 {
		panic("SnapshotStore.PruneTerminal: maxPrune must be positive")
	}
	cutoff := time.Now().Add(-olderThan)
	doomed, err := s.collectPrunable(ctx, cutoff, maxPrune)
	if err != nil {
		return 0, err
	}
	deleted := 0
	for _, key := range doomed {
		if err := s.kv.Delete(ctx, key); err != nil {
			return deleted, err
		}
		// Delete is idempotent (see Delete's doc comment) so an
		// index key that never made it (writeRunIndexEntry logged a
		// failure) or was already repaired away costs nothing here.
		runID := strings.TrimPrefix(key, "run.")
		if err := s.kv.Delete(ctx, runIndexPrefix+runID); err != nil {
			return deleted, err
		}
		// Defense-in-depth: a pruned run is by definition terminal
		// (collectPrunable's predicate), so its runactive entry should
		// already be gone via finalizeRun's explicit delete -- but a
		// run pruned without ever going through finalizeRun (a
		// legacy/direct write) could still carry one. deleteActiveEntry
		// swallows its own errors, so this never turns a clean prune
		// into a failure.
		s.deleteActiveEntry(ctx, runID)
		deleted++
	}
	return deleted, nil
}

// collectPrunable scans run.* keys and returns up to maxPrune keys whose
// runs are terminal with a CompletedAt strictly before cutoff. Returns an
// error on any corrupt value or Get failure (so the caller deletes nothing
// on a bad read). A key that vanished between scan and load is skipped.
func (s *SnapshotStore) collectPrunable(
	ctx context.Context, cutoff time.Time, maxPrune int,
) ([]string, error) {
	if cutoff.IsZero() {
		panic("SnapshotStore.collectPrunable: cutoff must not be zero")
	}
	if maxPrune <= 0 {
		panic("SnapshotStore.collectPrunable: maxPrune must be positive")
	}
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	if runKeyCountOf(keys) > runKeyScanMax {
		panic("SnapshotStore.collectPrunable: key set exceeds bound")
	}
	doomed := make([]string, 0, maxPrune)
	for _, key := range keys {
		if len(doomed) >= maxPrune {
			break
		}
		if !isRunKey(key) {
			continue
		}
		drop, err := s.isPrunable(ctx, key, cutoff)
		if err != nil {
			return nil, err
		}
		if drop {
			doomed = append(doomed, key)
		}
	}
	return doomed, nil
}

// isPrunable loads one snapshot key and reports whether the run is terminal
// with a CompletedAt strictly before cutoff. A key that vanished between
// scan and load is treated as already-gone (no error, not prunable). A run
// with ReleasePending=true is never prunable regardless of age (#664 review
// round 5): it still owes the reconciler an admission release, and deleting
// its snapshot out from under that debt would erase the only record of it,
// silently abandoning a leaked singleton lock/concurrency slot forever
// instead of letting the reconciler eventually recover or abandon it
// through reconcileReleasePending/reconcileReleaseFailed.
func (s *SnapshotStore) isPrunable(
	ctx context.Context, key string, cutoff time.Time,
) (bool, error) {
	if key == "" {
		panic("SnapshotStore.isPrunable: key must not be empty")
	}
	if cutoff.IsZero() {
		panic("SnapshotStore.isPrunable: cutoff must not be zero")
	}
	entry, err := s.kv.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return false, nil
		}
		return false, err
	}
	var run dag.WorkflowRun
	if err := json.Unmarshal(entry.Value(), &run); err != nil {
		return false, err
	}
	if !run.Status.IsTerminal() || run.CompletedAt == nil {
		return false, nil
	}
	if run.ReleasePending {
		return false, nil
	}
	return run.CompletedAt.Before(cutoff), nil
}

// Load retrieves and deserializes the WorkflowRun for the given run ID.
// Returns ErrRunNotFound when no entry exists, allowing callers to handle
// missing runs distinctly from NATS infrastructure errors.
func (s *SnapshotStore) Load(ctx context.Context, runID string) (dag.WorkflowRun, error) {
	if runID == "" {
		panic("SnapshotStore.Load: runID must not be empty")
	}
	if s.kv == nil {
		panic("SnapshotStore.Load: kv bucket must not be nil")
	}
	entry, err := s.kv.Get(
		ctx, "run."+runID,
	)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			return dag.WorkflowRun{}, ErrRunNotFound
		}
		return dag.WorkflowRun{}, err
	}
	var run dag.WorkflowRun
	err = json.Unmarshal(entry.Value(), &run)
	return run, err
}

// ListActive returns every run the reconciler still owns (#664 review
// round 4 invariant): its runactive.<runID> liveness marker is present
// AND its fetched snapshot is genuinely non-terminal OR still
// ReleasePending (#648 -- a terminal run whose admission release
// failed still holds a singleton lock / concurrency slot until the
// reconciler recovers it, so it belongs in this result exactly as
// much as a Running run does). This is the keys-first primitive for
// "all runs matching a cheap predicate" callers -- reconcileRunningRuns
// (must visit EVERY running run AND every run owing a release, from
// ONE listing) and countActiveRunsForRoot (an active-run quota count,
// for which a ReleasePending run correctly still counts: its
// concurrency slot has not actually been freed yet) -- that ListAll's
// arbitrary lexicographic-cap sample under-served on a large store:
// all three need the WHOLE owned population, not a sample.
//
// fetchMax is clamped to ActiveFetchMax regardless of what the caller
// passes, mirroring ScanNewestFirst's fetchMax clamp. Unlike
// ScanNewestFirst, order does not matter here, so the whole (bounded)
// ID list is fetched in one batch rather than walked backward.
//
// A stale runactive entry -- terminal AND NOT ReleasePending (a lost
// deleteActiveEntry race, or a pre-#664 entry) -- is fetched, found
// stale, and counted in ScanStats.Skipped rather than returned --
// RepairRunIndex's active-index pass is what actually removes it.
func (s *SnapshotStore) ListActive(
	ctx context.Context, fetchMax int,
) ([]dag.WorkflowRun, ScanStats, error) {
	if s.kv == nil {
		panic("ListActive: kv bucket must not be nil")
	}
	if fetchMax <= 0 {
		panic("ListActive: fetchMax must be positive")
	}
	if fetchMax > ActiveFetchMax {
		fetchMax = ActiveFetchMax
	}
	activeIDs, err := s.listActiveRunIDs(ctx)
	if err != nil {
		return nil, ScanStats{}, err
	}
	var stats ScanStats
	if len(activeIDs) > fetchMax {
		stats.Truncated = true
		activeIDs = activeIDs[:fetchMax]
	}
	if len(activeIDs) == 0 {
		return []dag.WorkflowRun{}, stats, nil
	}
	stats.Scanned = len(activeIDs)
	stats.Attempted = len(activeIDs)

	fetched, skipped, err := s.fetchRunsByID(ctx, activeIDs)
	if err != nil {
		return nil, ScanStats{}, err
	}
	stats.Skipped = skipped

	runs := make([]dag.WorkflowRun, 0, len(fetched))
	for _, run := range fetched {
		if !isReconcilerOwned(run) {
			stats.Skipped++
			continue
		}
		runs = append(runs, run)
		stats.Matched++
	}
	return runs, stats, nil
}

// isReconcilerOwned is THE unified runactive invariant (#664 review
// round 4): the reconciler owns run iff it is non-terminal, or it is
// terminal but still owes an admission release (WorkflowRun.
// ReleasePending, #648). Every place that decides whether a runactive
// marker should exist, or whether an existing one is still correct,
// goes through this single predicate -- ListActive's filter, the
// crash-gap backfill's create-or-not decision (backfillMissingIndex),
// the orphan-validation pass (repairActiveOrphans), and the one-time
// full build (buildActiveIndexOnce) -- so there is exactly one place
// to look to know what "active" means, and no way for two call sites
// to silently disagree about it.
func isReconcilerOwned(run dag.WorkflowRun) bool {
	return !run.Status.IsTerminal() || run.ReleasePending
}

// listActiveRunIDs drains the runactive.> filtered key listing into a
// deduped slice of run IDs (dedup for the same nats.go
// duplicate-during-listing reason listRunIndexKeys documents). Order is
// irrelevant to ListActive, unlike listRunIndexKeys' creation-order
// contract for ScanNewestFirst.
func (s *SnapshotStore) listActiveRunIDs(ctx context.Context) ([]string, error) {
	if s.kv == nil {
		panic("listActiveRunIDs: kv bucket must not be nil")
	}
	lister, err := s.kv.ListKeysFiltered(ctx, runActivePrefix+">")
	if err != nil {
		return nil, err
	}
	raw := make([]string, 0, 256)
	for key := range lister.Keys() {
		// An exceeded bound here is an OPERATING condition, not a
		// programmer error (review round 3): this runs inside
		// Orchestrator.Start's repair loop (repairActiveOrphans), so a
		// panic would crash-loop the process on every restart rather
		// than let Start return a diagnosable error naming the bound.
		if len(raw) >= runKeyScanMax {
			return nil, fmt.Errorf(
				"listActiveRunIDs: active-marker key set exceeds bound (%d)",
				runKeyScanMax,
			)
		}
		raw = append(raw, strings.TrimPrefix(key, runActivePrefix))
	}
	return dedupeOrderedKeys(raw), nil
}

// ScanStats reports what a ScanNewestFirst pass actually did, so a
// caller can report or log a partial/degraded result honestly instead
// of returning it as if it were complete.
type ScanStats struct {
	// Scanned is the number of index entries examined.
	Scanned int
	// Attempted is the number of run.* GETs issued -- the batch sizes
	// summed, i.e. what fetchMax actually bounds. NOT the count of
	// successful fetches: a GET counted here can still fail (pruned
	// key, timeout) and be counted again in Skipped. Attempted minus
	// Skipped is the number that actually resolved to a value.
	Attempted int
	// Matched is the number of fetched runs pred accepted.
	Matched int
	// Skipped is the number of index entries whose run.* value could
	// not be fetched (pruned between the index list and the fetch, or
	// a transient/timeout GET failure).
	Skipped int
	// Truncated is true iff the scan stopped WITHOUT collecting limit
	// matches AND without exhausting the index -- i.e. it hit fetchMax
	// first, so the result may be missing real matches OLDER than what
	// was actually scanned. Reaching limit (even in the same batch
	// that also happened to hit fetchMax) is success, not truncation.
	Truncated bool
}

// ScanNewestFirst walks the creation-ordered run index (runidx.>)
// from newest to oldest, fetching run values in bounded batches and
// applying pred as each batch arrives, until limit matches are
// collected, the index is exhausted, or fetchMax values have been
// fetched. fetchMax is clamped to ScanFetchMax regardless of what the
// caller passes -- defense against "fetch everything" callers (#659).
//
// This is the SOLE store-level scan primitive: ScanRuns, bulk cancel,
// ListRecent, and the filtered CountRuns path all route through it
// rather than each re-implementing "scan until enough matches."
//
// A run.* key missing for an index entry (pruned between the index
// list and the fetch, or a transient GET failure/timeout) is skipped
// and counted in stats -- not an error. Results are newest-first.
func (s *SnapshotStore) ScanNewestFirst(
	ctx context.Context, pred func(dag.WorkflowRun) bool,
	limit, fetchMax int,
) ([]dag.WorkflowRun, ScanStats, error) {
	if pred == nil {
		panic("ScanNewestFirst: pred must not be nil")
	}
	if limit <= 0 {
		panic("ScanNewestFirst: limit must be positive")
	}
	if fetchMax < limit {
		panic("ScanNewestFirst: fetchMax must be >= limit")
	}
	if fetchMax > ScanFetchMax {
		fetchMax = ScanFetchMax
	}
	indexKeys, err := s.listRunIndexKeys(ctx)
	if err != nil {
		return nil, ScanStats{}, err
	}

	matches := make([]dag.WorkflowRun, 0, limit)
	var stats ScanStats
	pos := len(indexKeys) // exclusive end of the next batch, walking backward
	for pos > 0 && len(matches) < limit && stats.Attempted < fetchMax {
		batchSize := scanBatchSize
		if batchSize > pos {
			batchSize = pos
		}
		if stats.Attempted+batchSize > fetchMax {
			batchSize = fetchMax - stats.Attempted
		}
		start := pos - batchSize
		batchIDs := make([]string, batchSize)
		for i, key := range indexKeys[start:pos] {
			batchIDs[i] = strings.TrimPrefix(key, runIndexPrefix)
		}
		pos = start
		stats.Scanned += len(batchIDs)

		fetched, skipped, err := s.fetchRunsByID(ctx, batchIDs)
		if err != nil {
			return nil, ScanStats{}, err
		}
		stats.Attempted += len(batchIDs)
		stats.Skipped += skipped

		// fetched preserves batchIDs' ascending (oldest-first) order;
		// walk it backward for newest-first within the batch.
		for i := len(fetched) - 1; i >= 0 && len(matches) < limit; i-- {
			if pred(fetched[i]) {
				matches = append(matches, fetched[i])
				stats.Matched++
			}
		}
	}
	// pos > 0 (index not exhausted) after the loop can only follow
	// from EITHER the fetchMax budget running out OR limit being
	// satisfied -- distinguish them by whether limit was actually
	// reached, not by re-checking Attempted against fetchMax (which
	// can coincidentally equal fetchMax in the very batch that also
	// satisfied limit, which is success, not truncation).
	stats.Truncated = pos > 0 && len(matches) < limit
	return matches, stats, nil
}

// listRunIndexKeys drains the runidx.> filtered key listing into a
// deduped slice in creation order (oldest first). Every runidx.<runID>
// key is written exactly once via Create and never updated
// (writeRunIndexEntry), so JetStream's replay order for the filtered
// watch behind ListKeysFiltered IS creation order -- unlike Keys(),
// which explicitly slices.Sort()s its result lexicographically before
// returning (nats.go jetstream/kv.go, kvs.Keys, ~line 1403) and
// therefore carries no time signal at all. ListKeysFiltered's own
// watcher, by contrast, delivers keys in the order the underlying
// WatchFiltered stream replays them -- unsorted, i.e. write order --
// which is exactly the property this index depends on.
//
// nats.go documents that "[o]n buckets with a large number of keys and
// frequent writes, duplicate keys may be reported during listing" for
// both ListKeys and ListKeysFiltered (kv.go ~1428, ~1457) -- a repeat
// would double-count an index entry's batch position in
// ScanNewestFirst, so every key is deduped (first occurrence kept,
// preserving order) before being returned.
func (s *SnapshotStore) listRunIndexKeys(ctx context.Context) ([]string, error) {
	if s.kv == nil {
		panic("listRunIndexKeys: kv bucket must not be nil")
	}
	lister, err := s.kv.ListKeysFiltered(ctx, runIndexPrefix+">")
	if err != nil {
		return nil, err
	}
	raw := make([]string, 0, 256)
	for key := range lister.Keys() {
		if len(raw) >= runKeyScanMax {
			panic("listRunIndexKeys: key set exceeds bound")
		}
		raw = append(raw, key)
	}
	return dedupeOrderedKeys(raw), nil
}

// dedupeOrderedKeys removes duplicate keys while preserving
// first-occurrence order. See listRunIndexKeys' doc comment for why
// this is needed -- ListKeysFiltered's underlying watcher may repeat a
// key on a bucket with many keys and frequent writes. Bounded by
// len(keys), itself already bounded by runKeyScanMax at the call site.
func dedupeOrderedKeys(keys []string) []string {
	seen := make(map[string]bool, len(keys))
	out := make([]string, 0, len(keys))
	for _, key := range keys {
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, key)
	}
	return out
}

// fetchRunsByID batch-fetches run.<id> for every id in runIDs (all
// members of a SINGLE ScanNewestFirst batch), preserving runIDs' order
// in the result by iterating runIDs rather than trusting map iteration
// order from the underlying best-effort fetch. An id whose run.* key
// no longer exists (pruned, or a slow/timed-out GET) is simply absent
// from the result and counted in the returned skip count.
func (s *SnapshotStore) fetchRunsByID(
	ctx context.Context, runIDs []string,
) ([]dag.WorkflowRun, int, error) {
	if s.kv == nil {
		panic("fetchRunsByID: kv bucket must not be nil")
	}
	if len(runIDs) == 0 {
		panic("fetchRunsByID: runIDs must not be empty")
	}
	keys := make([]string, len(runIDs))
	for i, id := range runIDs {
		keys[i] = "run." + id
	}
	values, skipped, err := natsutil.ParallelGetJSBestEffort(
		ctx, s.kv, keys, natsutil.DefaultParallelism, bestEffortGetTimeout,
	)
	if err != nil {
		return nil, 0, err
	}
	runs := make([]dag.WorkflowRun, 0, len(keys))
	for _, key := range keys {
		value, ok := values[key]
		if !ok {
			continue
		}
		var run dag.WorkflowRun
		if err := json.Unmarshal(value, &run); err != nil {
			return nil, 0, err
		}
		runs = append(runs, run)
	}
	return runs, skipped, nil
}

// ListRecent returns the genuinely most-recent limit workflow runs,
// newest first (CreatedAt order via the creation-ordered index, not a
// re-derived sort) -- O(limit) via ScanNewestFirst rather than the
// O(population) full-fetch-then-sort this used before #659.
func (s *SnapshotStore) ListRecent(
	ctx context.Context, limit int,
) ([]dag.WorkflowRun, error) {
	if s.kv == nil {
		panic("SnapshotStore.ListRecent: kv bucket must not be nil")
	}
	if limit <= 0 {
		panic("SnapshotStore.ListRecent: limit must be positive")
	}
	runs, _, err := s.ScanNewestFirst(
		ctx, alwaysMatch, limit, ScanFetchMax,
	)
	return runs, err
}

// alwaysMatch is the ScanNewestFirst predicate for callers (ListRecent)
// that want the newest N runs unconditionally.
func alwaysMatch(dag.WorkflowRun) bool { return true }

// CountAll returns the number of run.* entries without fetching any
// values — a cheap keys-only scan for aggregate counts (#452).
func (s *SnapshotStore) CountAll(ctx context.Context) (int, error) {
	if s.kv == nil {
		panic("SnapshotStore.CountAll: kv bucket must not be nil")
	}
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return 0, nil
		}
		return 0, err
	}
	if runKeyCountOf(keys) > runKeyScanMax {
		panic("SnapshotStore.CountAll: key set exceeds bound")
	}
	count := 0
	for _, key := range keys {
		if isRunKey(key) {
			count++
		}
	}
	return count, nil
}

// isRunKey reports whether a KV key names a workflow run snapshot.
func isRunKey(key string) bool {
	return len(key) >= 4 && key[:4] == "run."
}

// isRunIndexKey reports whether a KV key names a run-index marker.
func isRunIndexKey(key string) bool {
	return len(key) > len(runIndexPrefix) &&
		key[:len(runIndexPrefix)] == runIndexPrefix
}

// runKeyCountOf counts how many of keys are run.<id> snapshot keys,
// ignoring runidx.<id> index markers and anything else. The bucket
// now holds two keys per run (run.<id> + runidx.<id>), so bounding a
// raw len(keys) against runKeyScanMax would silently halve the run
// population the bound was meant to cover -- every caller checking
// runKeyScanMax against an UNFILTERED s.kv.Keys(ctx) result must
// count through this first.
func runKeyCountOf(keys []string) int {
	count := 0
	for _, key := range keys {
		if isRunKey(key) {
			count++
		}
	}
	return count
}

// RepairStats reports what one RepairRunIndex call did.
type RepairStats struct {
	// Repaired is the number of runidx.<runID> entries backfilled for
	// runs that had a run.<runID> snapshot but no index entry -- the
	// crash-gap set (#664 review round 3): createEntryIndexes writes
	// runactive FIRST (propagating its error), THEN runidx, so a run
	// can only be missing runidx if the process crashed or a Create
	// failed strictly AFTER runactive was already durably written (for
	// a run isReconcilerOwned considers owned at creation) or, for a
	// run that is not owned at creation, between the snapshot write
	// and writeRunIndexEntry. Every OTHER write path either writes
	// both indexes at creation (in this order) or writes neither
	// (Save) -- this backfill therefore also recreates runactive for
	// any candidate isReconcilerOwned says should have one, closing
	// the gap writing runidx-first would have left open (an owned run
	// with runidx but no runactive, invisible to ListActive forever).
	Repaired int
	// OrphansRemoved is the number of runidx.<runID> entries deleted
	// because no run.<runID> snapshot exists for them.
	OrphansRemoved int
	// ActiveRepaired is the number of runactive.<runID> entries
	// created as a SIDE EFFECT of backfilling a crash-gap run that
	// isReconcilerOwned says should have one (#664) -- a subset of
	// Repaired, never counted independently: the crash-gap set is the
	// only place a missing runactive entry is ever backfilled from.
	ActiveRepaired int
	// ActiveOrphansRemoved is the number of runactive.<runID> entries
	// removed because the run they name is no longer isReconcilerOwned
	// (terminal AND not ReleasePending) or is gone (#664), found by
	// validating the CURRENT active markers against their run's real
	// state -- bounded by the active population, not by the total run
	// count.
	ActiveOrphansRemoved int
	// ActiveValidationTruncated is true when this pass validated FEWER
	// than the full current runactive population -- more markers exist
	// beyond pageMax (review round 3 nit). Convergence must never be
	// declared while this is true, even when ActiveOrphansRemoved is 0
	// for the markers actually checked: markers beyond the window were
	// never examined and may still be stale.
	ActiveValidationTruncated bool
}

// Add returns the field-wise combination of s and other: the int
// counters are summed, and the truncation flag is OR'd (truncated in
// either call means the combined pass is truncated).
// repairRunIndexToConvergenceWith uses this -- not manual per-field
// addition -- to accumulate stats across bounded passes specifically
// so a human adding a new RepairStats field cannot silently leave it
// out of the accumulation (#664 review round 4 MEDIUM: an earlier
// version of this loop dropped two fields this way).
// TestRepairStatsAddCoversEveryField (active_index_test.go) uses
// reflection over RepairStats' fields to catch the next such
// omission -- it fails if a new field is added to the struct without
// a matching line here.
func (s RepairStats) Add(other RepairStats) RepairStats {
	return RepairStats{
		Repaired:                  s.Repaired + other.Repaired,
		OrphansRemoved:            s.OrphansRemoved + other.OrphansRemoved,
		ActiveRepaired:            s.ActiveRepaired + other.ActiveRepaired,
		ActiveOrphansRemoved:      s.ActiveOrphansRemoved + other.ActiveOrphansRemoved,
		ActiveValidationTruncated: s.ActiveValidationTruncated || other.ActiveValidationTruncated,
	}
}

// RepairRunIndex reconciles both derived indexes against the run.*
// population in one bounded pass:
//
//   - runidx backfill (#659): a run with a snapshot but no runidx
//     entry -- the crash-gap set described on RepairStats.Repaired --
//     gets both runidx and, if isReconcilerOwned says so, runactive
//     Created. Order does not matter here (unlike the historical
//     large-backlog case, which buildActiveIndexOnce now owns as a
//     one-time full pass at startup): the crash-gap set is always
//     small in steady state, so there is no cross-call ordering
//     invariant to preserve.
//   - runidx orphan removal: an index entry with no matching run.*
//     value (the run was deleted directly, bypassing PruneTerminal, or
//     a partial delete raced) is removed. Cheap: no value fetch, just
//     a keys-only set diff followed by Deletes on definite orphans.
//   - runactive validation (#664 review round 2, predicate updated
//     round 4): EVERY CURRENT runactive marker is checked against
//     isReconcilerOwned(run) and removed if that is now false (stale)
//     or the run is gone (orphaned). This never enumerates the run.*
//     population -- it lists runactive.> directly (bounded by the
//     OWNED population, which admission/concurrency limits AND the
//     rarity of release failures both bound) and fetches only those
//     values. This is what closes the bug a prior version of this
//     repair had: scanning "runs missing an active marker" out of the
//     FULL run population put terminal runs in the candidate pool
//     forever (correctly absent, so never repaired, so the pass never
//     made progress and a terminal-heavy store could spend its whole
//     pageMax budget on terminal runs every single call). Under the
//     new write model runactive is never "missing-and-should-exist"
//     outside the crash-gap case above, so there is nothing left to
//     backfill here -- only to validate.
//
// Each phase is capped at pageMax per call; a store with more damage
// than that needs more than one call -- callers (the reconciler tick,
// orchestrator startup) call this repeatedly on a schedule rather than
// in a loop within one call.
//
// Plainly, stated without euphemism (review round 2, #659): until a
// run is backfilled, ScanNewestFirst cannot see it at all -- it is
// simply absent from every scan. And a backfilled run is Created at
// the CURRENT tail of the index (this call's write position), not
// spliced into its true chronological position among already-indexed
// entries -- there is no re-sort, ever, in THIS pass (the one-time
// buildActiveIndexOnce full pass DOES sort, once, at startup). So a
// run whose original createEntryIndexes write was lost (a crash, or a
// transient KV error) and is later backfilled here will read as one
// of the NEWEST entries in the index, however old its real CreatedAt
// is, for as long as it remains at the tail. Orchestrator.Start
// converges the index to zero missing runs before serving, so in
// normal operation this window never reaches a caller; it can only
// reopen if a LATER creation's createEntryIndexes loses its race after
// startup, in which case the next reconciler tick's single bounded
// pass closes it.
func (s *SnapshotStore) RepairRunIndex(
	ctx context.Context, pageMax int,
) (RepairStats, error) {
	if s.kv == nil {
		panic("RepairRunIndex: kv bucket must not be nil")
	}
	if pageMax <= 0 {
		panic("RepairRunIndex: pageMax must be positive")
	}
	runIDs, indexIDs, err := s.loadRunAndIndexIDSets(ctx)
	if err != nil {
		return RepairStats{}, err
	}
	stats := RepairStats{}
	stats.Repaired, stats.ActiveRepaired, err =
		s.backfillMissingIndex(ctx, runIDs, indexIDs, pageMax)
	if err != nil {
		return stats, err
	}
	stats.OrphansRemoved, err = s.removeOrphanIndexEntries(ctx, runIDs, indexIDs, pageMax)
	if err != nil {
		return stats, err
	}
	stats.ActiveOrphansRemoved, stats.ActiveValidationTruncated, err =
		s.repairActiveOrphans(ctx, pageMax)
	if err != nil {
		return stats, err
	}
	return stats, nil
}

// loadRunAndIndexIDSets does one keys-only Keys() scan (bounded by
// runKeyScanMax, same bound CountAll uses) and splits the result into
// the run-ID and runidx-ID sets the runidx backfill/orphan passes
// diff. No value fetch here -- this is the cheap half of repair; the
// runactive validation pass (repairActiveOrphans) does not use this at
// all, since it works from the active-marker listing instead.
func (s *SnapshotStore) loadRunAndIndexIDSets(
	ctx context.Context,
) (map[string]bool, map[string]bool, error) {
	if s.kv == nil {
		panic("loadRunAndIndexIDSets: kv bucket must not be nil")
	}
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return map[string]bool{}, map[string]bool{}, nil
		}
		return nil, nil, err
	}
	// An exceeded bound here is an OPERATING condition, not a
	// programmer error (review round 4 nit): this runs from
	// RepairRunIndex, reachable from Orchestrator.Start's steady-state
	// convergence loop even AFTER index.meta exists -- a panic would
	// crash-loop the process on every restart instead of letting Start
	// return a diagnosable error naming the bound.
	if runKeyCountOf(keys) > runKeyScanMax {
		return nil, nil, fmt.Errorf(
			"loadRunAndIndexIDSets: run key set exceeds bound (%d)",
			runKeyScanMax,
		)
	}
	runIDs := make(map[string]bool, len(keys))
	indexIDs := make(map[string]bool, len(keys))
	for _, key := range keys {
		switch {
		case isRunKey(key):
			runIDs[key[len("run."):]] = true
		case isRunIndexKey(key):
			indexIDs[key[len(runIndexPrefix):]] = true
		}
	}
	return runIDs, indexIDs, nil
}

// backfillMissingIndex Creates runidx.<runID> -- and, if
// isReconcilerOwned(run) says so, runactive.<runID> too -- for the
// crash-gap set: runs with a snapshot but no runidx entry. Candidates
// are capped at pageMax BEFORE fetching any value (unlike the historical
// large-backlog version this replaces): under the new write model this
// set is always small in steady state (see RepairRunIndex's doc
// comment), so there is no ordering invariant across bounded calls to
// preserve here -- that concern now belongs entirely to
// buildActiveIndexOnce's one-time full pass.
func (s *SnapshotStore) backfillMissingIndex(
	ctx context.Context, runIDs, indexIDs map[string]bool, pageMax int,
) (repaired, activeRepaired int, err error) {
	if runIDs == nil || indexIDs == nil {
		panic("backfillMissingIndex: id sets must not be nil")
	}
	if pageMax <= 0 {
		panic("backfillMissingIndex: pageMax must be positive")
	}
	missing := make([]string, 0, pageMax)
	for id := range runIDs {
		if len(missing) >= pageMax {
			break
		}
		if !indexIDs[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return 0, 0, nil
	}
	keys := make([]string, len(missing))
	for i, id := range missing {
		keys[i] = "run." + id
	}
	values, _, err := natsutil.ParallelGetJSBestEffort(
		ctx, s.kv, keys, natsutil.DefaultParallelism, bestEffortGetTimeout,
	)
	if err != nil {
		return 0, 0, err
	}
	for _, id := range missing {
		value, ok := values["run."+id]
		if !ok {
			continue // vanished between the key scan and this fetch
		}
		var run dag.WorkflowRun
		if err := json.Unmarshal(value, &run); err != nil {
			return repaired, activeRepaired, err
		}
		// ErrKeyExists means a concurrent writer (another creation,
		// another RepairRunIndex pass) already created this entry
		// between loadRunAndIndexIDSets' diff and this Create -- the
		// entry is correct, but THIS call did no work, so it must not
		// count as repaired (review round 2): counting it would waste
		// a convergence pass and inflate the repaired metric with no
		// real index write behind it.
		if _, err := s.kv.Create(ctx, runIndexPrefix+id, []byte{}); err != nil {
			if !errors.Is(err, jetstream.ErrKeyExists) {
				return repaired, activeRepaired, err
			}
		} else {
			repaired++
		}
		if !isReconcilerOwned(run) {
			continue
		}
		if _, err := s.kv.Create(ctx, runActivePrefix+id, []byte{}); err != nil {
			if !errors.Is(err, jetstream.ErrKeyExists) {
				return repaired, activeRepaired, err
			}
		} else {
			activeRepaired++
		}
	}
	return repaired, activeRepaired, nil
}

// removeOrphanIndexEntries deletes up to pageMax runidx.<runID>
// entries whose run.<runID> snapshot no longer exists.
func (s *SnapshotStore) removeOrphanIndexEntries(
	ctx context.Context, runIDs, indexIDs map[string]bool, pageMax int,
) (int, error) {
	if runIDs == nil || indexIDs == nil {
		panic("removeOrphanIndexEntries: id sets must not be nil")
	}
	if pageMax <= 0 {
		panic("removeOrphanIndexEntries: pageMax must be positive")
	}
	removed := 0
	for id := range indexIDs {
		if removed >= pageMax {
			break
		}
		if runIDs[id] {
			continue
		}
		if err := s.kv.Delete(ctx, runIndexPrefix+id); err != nil {
			return removed, err
		}
		removed++
	}
	return removed, nil
}

// repairActiveOrphans validates up to pageMax of the CURRENT
// runactive.<runID> markers against isReconcilerOwned(run), deleting
// any that are stale (no longer owned -- terminal AND not
// ReleasePending, AND has been for longer than activeOrphanGrace) or
// orphaned (run missing) (#664 review round 2, predicate updated round
// 4, grace window added round 5). It lists runactive.> directly
// (listActiveRunIDs, the same primitive ListActive uses) rather than
// diffing against the full run.* population -- so its cost is bounded
// by the OWNED population, which admission/concurrency limits AND the
// rarity of release failures both bound, not by the total run count.
// This is deliberately NOT a backfill pass: under the new write model
// (createEntryIndexes at creation only) an owned run can only be
// missing its runactive entry via the crash-gap path
// backfillMissingIndex already covers, so there is nothing to
// backfill here -- only to validate and clean up.
//
// The grace window (review round 5, reviewer-reproduced): finalizeRun
// persists a run terminal (ReleasePending still false) BEFORE it knows
// whether afterPersist will fail and hand off to
// finalizeWithReleaseDebt, which is what actually sets ReleasePending=
// true. A run observed here in that narrow window looks exactly like a
// genuinely stale marker -- terminal, ReleasePending false -- but is
// not; deleting it would strand finalizeWithReleaseDebt's later save
// with no marker to have kept correct, since that function does not
// unconditionally recreate one on every call path (see its own
// defensive re-Create for the write-side half of this fix). A run
// whose CompletedAt is within activeOrphanGrace is left alone here on
// this pass -- a genuinely stale marker past the window is still
// removed exactly as before; a marker for a run that has vanished
// entirely (run.* missing) has no CompletedAt to consult and is always
// removed immediately, grace does not apply to orphans.
func (s *SnapshotStore) repairActiveOrphans(
	ctx context.Context, pageMax int,
) (removed int, truncated bool, err error) {
	if s.kv == nil {
		panic("repairActiveOrphans: kv bucket must not be nil")
	}
	if pageMax <= 0 {
		panic("repairActiveOrphans: pageMax must be positive")
	}
	activeIDs, err := s.listActiveRunIDs(ctx)
	if err != nil {
		return 0, false, err
	}
	if len(activeIDs) > pageMax {
		// Review round 3 nit: more markers exist than this pass will
		// validate -- the caller must NOT treat a subsequent
		// removed==0 as "nothing to do" here, since the untouched tail
		// was never examined.
		truncated = true
		activeIDs = activeIDs[:pageMax]
	}
	if len(activeIDs) == 0 {
		return 0, truncated, nil
	}
	keys := make([]string, len(activeIDs))
	for i, id := range activeIDs {
		keys[i] = "run." + id
	}
	values, _, err := natsutil.ParallelGetJSBestEffort(
		ctx, s.kv, keys, natsutil.DefaultParallelism, bestEffortGetTimeout,
	)
	if err != nil {
		return 0, truncated, err
	}
	for i, id := range activeIDs {
		value, ok := values[keys[i]]
		stale := !ok // the run.* snapshot is gone entirely -- orphaned
		if ok {
			var run dag.WorkflowRun
			if err := json.Unmarshal(value, &run); err != nil {
				return removed, truncated, err
			}
			stale = !isReconcilerOwned(run) &&
				!withinActiveOrphanGrace(run)
		}
		if !stale {
			continue
		}
		if err := s.kv.Delete(ctx, runActivePrefix+id); err != nil {
			return removed, truncated, err
		}
		removed++
	}
	return removed, truncated, nil
}

// withinActiveOrphanGrace reports whether run completed recently enough
// that repairActiveOrphans must NOT yet treat its (terminal,
// not-ReleasePending) runactive marker as stale -- see
// repairActiveOrphans' doc comment for the finalizeRun ordering race
// this guards against. A run with no CompletedAt (should not happen for
// a genuinely terminal run -- markTerminal always sets it) gets no
// grace: there is no timestamp to bound the window by, so it is treated
// as immediately eligible for removal, matching pre-round-5 behavior.
func withinActiveOrphanGrace(run dag.WorkflowRun) bool {
	if run.CompletedAt == nil {
		return false
	}
	return time.Since(*run.CompletedAt) < activeOrphanGrace
}

// indexMetaKey names the single meta key recording whether the
// one-time active-index full build (buildActiveIndexOnce) has ever
// run against this bucket (#664 review round 2).
const indexMetaKey = "index.meta"

// indexMeta is indexMetaKey's JSON value.
type indexMeta struct {
	// ActiveBuilt is true once buildActiveIndexOnce has completed a
	// full pass over the run.* population at least once. Absent (key
	// not found) is equivalent to false -- a store that predates #664
	// or has never finished the pass.
	ActiveBuilt bool `json:"active_built"`
}

// buildActiveIndexOnce performs the ONE-TIME full-population pass that
// makes both derived indexes correct on a store that predates them (a
// pre-#659/#664 upgrade) or was left with a large backlog by an
// earlier partial run -- exactly the scenario the steady-state
// crash-gap repair (RepairRunIndex) is deliberately NOT sized for
// (review round 2: scanning the whole population on every call is the
// bug this redesign removes from the steady-state path; it still has
// to happen SOMEWHERE, exactly once).
//
// Idempotent by construction: if indexMetaKey is already present with
// ActiveBuilt=true, this returns immediately (built=false, meaning "no
// work done") without reading a single run.* value. Otherwise it pages
// through the ENTIRE run.* population (bounded by runKeyScanMax, one
// GET per run, logging progress every pageMax-sized batch), builds
// runidx (Created in ascending CreatedAt order, preserving the GLOBAL
// creation-order invariant ScanNewestFirst depends on -- the property
// the old backfillMissingIndex used to defend across bounded calls,
// now only needed here, once) for every run, and runactive for every
// run isReconcilerOwned(run) says the reconciler owns -- non-terminal,
// OR terminal with ReleasePending=true (review round 4 BLOCKER: a run
// already carrying ReleasePending=true on an upgrading store MUST get
// a runactive marker too, or its leaked slot/lock is invisible to the
// reconciler forever; this build is the one place that can find every
// such run, since it already fetches every value) -- then writes
// indexMetaKey so no future startup ever repeats this pass.
//
// Called once from Orchestrator.Start, BEFORE the steady-state
// crash-gap convergence loop -- see repairRunIndexToConvergence.
func (s *SnapshotStore) buildActiveIndexOnce(ctx context.Context) (bool, error) {
	if s.kv == nil {
		panic("buildActiveIndexOnce: kv bucket must not be nil")
	}
	if _, err := s.kv.Get(ctx, indexMetaKey); err == nil {
		return false, nil
	} else if !errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, err
	}
	metas, err := s.loadAllRunMetasForBuild(ctx)
	if err != nil {
		return false, err
	}
	sort.Slice(metas, func(i, j int) bool {
		return metas[i].createdAt.Before(metas[j].createdAt)
	})
	for _, m := range metas {
		if _, err := s.kv.Create(ctx, runIndexPrefix+m.runID, []byte{}); err != nil &&
			!errors.Is(err, jetstream.ErrKeyExists) {
			return false, err
		}
		if !m.active {
			continue
		}
		if _, err := s.kv.Create(ctx, runActivePrefix+m.runID, []byte{}); err != nil &&
			!errors.Is(err, jetstream.ErrKeyExists) {
			return false, err
		}
	}
	metaData, err := json.Marshal(indexMeta{ActiveBuilt: true})
	if err != nil {
		return false, err
	}
	if _, err := s.kv.Put(ctx, indexMetaKey, metaData); err != nil {
		return false, err
	}
	return true, nil
}

// runBuildMeta pairs a run ID with the fields buildActiveIndexOnce
// needs: CreatedAt (for the ordering sort) and whether the reconciler
// owns it (isReconcilerOwned -- the runactive decision).
type runBuildMeta struct {
	runID     string
	createdAt time.Time
	active    bool
}

// loadAllRunMetasForBuild pages through the WHOLE run.* population
// (bounded by runKeyScanMax) fetching every value exactly once, in
// pageMax-sized batches, logging progress per batch so a large
// one-time build is operator-visible rather than a silent multi-minute
// pause. A value that vanished between the key scan and its fetch is
// skipped -- the run is gone, nothing to index.
func (s *SnapshotStore) loadAllRunMetasForBuild(
	ctx context.Context,
) ([]runBuildMeta, error) {
	if s.kv == nil {
		panic("loadAllRunMetasForBuild: kv bucket must not be nil")
	}
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	runIDs := make([]string, 0, len(keys))
	for _, key := range keys {
		if isRunKey(key) {
			runIDs = append(runIDs, key[len("run."):])
		}
	}
	// An exceeded bound here is an OPERATING condition, not a
	// programmer error (review round 3): this runs inside
	// Orchestrator.Start, BEFORE index.meta is ever written -- a panic
	// would crash-loop the process on every single restart rather than
	// let Start return a diagnosable error naming the bound.
	if len(runIDs) > runKeyScanMax {
		return nil, fmt.Errorf(
			"loadAllRunMetasForBuild: run key set exceeds bound (%d)",
			runKeyScanMax,
		)
	}
	metas := make([]runBuildMeta, 0, len(runIDs))
	for start := 0; start < len(runIDs); start += repairPageMax {
		end := start + repairPageMax
		if end > len(runIDs) {
			end = len(runIDs)
		}
		batch := runIDs[start:end]
		fetchKeys := make([]string, len(batch))
		for i, id := range batch {
			fetchKeys[i] = "run." + id
		}
		values, _, err := natsutil.ParallelGetJSBestEffort(
			ctx, s.kv, fetchKeys, natsutil.DefaultParallelism, bestEffortGetTimeout,
		)
		if err != nil {
			return nil, err
		}
		for _, id := range batch {
			value, ok := values["run."+id]
			if !ok {
				continue // vanished between the key scan and this fetch
			}
			var run dag.WorkflowRun
			if err := json.Unmarshal(value, &run); err != nil {
				return nil, err
			}
			metas = append(metas, runBuildMeta{
				runID: id, createdAt: run.CreatedAt,
				active: isReconcilerOwned(run),
			})
		}
		slog.InfoContext(ctx,
			"startup: active-index full build progress",
			"scanned", end, "total", len(runIDs),
		)
	}
	return metas, nil
}
