package engine

import (
	"context"
	"encoding/json"
	"errors"
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
	if err != nil {
		return err
	}
	s.writeRunIndexEntry(ctx, run.RunID)
	return nil
}

// writeRunIndexEntry Create-writes the "runidx.<runID>" marker for a
// run whose "run.<runID>" snapshot key was JUST written successfully.
// Called from every snapshot write (Save and CreateSnapshot), not only
// the first -- Create is the mechanism that makes "written once" hold:
// every write after the first loses the race with ErrKeyExists and is
// a silent no-op, so the marker's revision is forever its creation
// revision regardless of which write happened to be first.
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
		s.writeRunIndexEntry(ctx, run.RunID)
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
// deletes a run ONLY IF it is terminal AND its CompletedAt is strictly
// older than olderThan. Non-terminal runs (even ancient ones) and terminal
// runs younger than the window are never touched. At most maxPrune runs are
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
// scan and load is treated as already-gone (no error, not prunable).
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

// ListAll returns all workflow runs from the KV bucket.
// Scans all keys with prefix "run." bounded at maxRuns.
// Uses parallel fetches for throughput on large key sets.
//
// This is the cheap, order-agnostic primitive: it caps DURING the key
// scan (no global sort, no full-population fetch). Order-sensitive
// callers that need the genuine most-recent N must use ListRecent;
// callers here (reconciler, bulk retry/cancel, REST list) only need a
// bounded, unordered slice. See #452.
func (s *SnapshotStore) ListAll(
	ctx context.Context, maxRuns int,
) ([]dag.WorkflowRun, error) {
	if s.kv == nil {
		panic("SnapshotStore.ListAll: kv bucket must not be nil")
	}
	if maxRuns <= 0 {
		panic("SnapshotStore.ListAll: maxRuns must be positive")
	}
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return []dag.WorkflowRun{}, nil
		}
		return nil, err
	}

	// Filter to run.* keys and apply limit.
	filtered := make([]string, 0, len(keys))
	for _, key := range keys {
		if len(key) < 4 || key[:4] != "run." {
			continue
		}
		if len(filtered) >= maxRuns {
			break
		}
		filtered = append(filtered, key)
	}

	if len(filtered) == 0 {
		return []dag.WorkflowRun{}, nil
	}

	// Best-effort fetch (#523): a slow key on a large bucket is skipped
	// rather than discarding the whole batch, so the 60s reconciler tick
	// still reconciles the runs it CAN read instead of the all-or-nothing
	// cliff. Skips are logged LOUDLY so operators can prune/tune retention.
	entries, skipped, err := natsutil.ParallelGetJSBestEffort(
		ctx, s.kv, filtered,
		natsutil.DefaultParallelism, bestEffortGetTimeout,
	)
	if err != nil {
		return nil, err
	}
	if skipped > 0 {
		slog.WarnContext(ctx,
			"ListAll skipped slow run keys; prune or tune retention",
			"skipped", skipped,
			"fetched", len(entries),
			"scanned", len(filtered),
		)
	}

	runs := make([]dag.WorkflowRun, 0, len(entries))
	for _, value := range entries {
		var run dag.WorkflowRun
		if err := json.Unmarshal(value, &run); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
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
	// runs that had a run.<runID> snapshot but no index entry.
	Repaired int
	// OrphansRemoved is the number of runidx.<runID> entries deleted
	// because no run.<runID> snapshot exists for them.
	OrphansRemoved int
}

// RepairRunIndex reconciles the runidx.> index against the run.*
// population in one bounded pass (#659):
//
//   - Backfill: a run with a snapshot but no index entry (a pre-#659
//     run, or one that lost the writeRunIndexEntry race against a
//     crash/transient error) gets one Created, in ascending CreatedAt
//     order so a multi-call repair of a large backlog preserves the
//     GLOBAL creation-order invariant ScanNewestFirst depends on --
//     the oldest missing runs are always backfilled before newer
//     ones, across calls, not just within one.
//   - Orphan removal: an index entry with no matching run.* value
//     (the run was deleted directly, bypassing PruneTerminal, or a
//     partial delete raced) is removed.
//
// Both phases are capped at pageMax per call; a store with more
// damage than that needs more than one call -- callers (the
// reconciler tick, orchestrator startup) call this repeatedly on a
// schedule rather than in a loop within one call. Until a run is
// backfilled, ScanNewestFirst simply cannot see it -- a documented,
// bounded visibility window, not a correctness bug.
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
	repaired, err := s.backfillMissingIndex(ctx, runIDs, indexIDs, pageMax)
	if err != nil {
		return RepairStats{Repaired: repaired}, err
	}
	orphansRemoved, err := s.removeOrphanIndexEntries(ctx, runIDs, indexIDs, pageMax)
	if err != nil {
		return RepairStats{Repaired: repaired}, err
	}
	return RepairStats{Repaired: repaired, OrphansRemoved: orphansRemoved}, nil
}

// loadRunAndIndexIDSets does one keys-only Keys() scan (bounded by
// runKeyScanMax, same bound CountAll uses) and splits the result into
// the run-ID and index-ID sets RepairRunIndex diffs.
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
	if runKeyCountOf(keys) > runKeyScanMax {
		panic("loadRunAndIndexIDSets: key set exceeds bound")
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

// runCreatedAt pairs a run ID with its CreatedAt for the backfill sort.
type runCreatedAt struct {
	runID     string
	createdAt time.Time
}

// backfillMissingIndex Creates a runidx.<runID> entry for every run
// missing one, in ascending CreatedAt order, capped at pageMax per
// call. The full missing set's CreatedAt values are fetched (bounded
// by runKeyScanMax via the caller's key scan) and sorted BEFORE
// truncating to pageMax so repeated bounded calls converge on a
// backlog in true creation order rather than an arbitrary subset each
// time -- see RepairRunIndex's doc comment.
func (s *SnapshotStore) backfillMissingIndex(
	ctx context.Context, runIDs, indexIDs map[string]bool, pageMax int,
) (int, error) {
	if runIDs == nil || indexIDs == nil {
		panic("backfillMissingIndex: id sets must not be nil")
	}
	if pageMax <= 0 {
		panic("backfillMissingIndex: pageMax must be positive")
	}
	missing := make([]string, 0, len(runIDs))
	for id := range runIDs {
		if !indexIDs[id] {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return 0, nil
	}
	keys := make([]string, len(missing))
	for i, id := range missing {
		keys[i] = "run." + id
	}
	values, _, err := natsutil.ParallelGetJSBestEffort(
		ctx, s.kv, keys, natsutil.DefaultParallelism, bestEffortGetTimeout,
	)
	if err != nil {
		return 0, err
	}
	candidates := make([]runCreatedAt, 0, len(missing))
	for _, id := range missing {
		value, ok := values["run."+id]
		if !ok {
			continue // vanished between the key scan and this fetch
		}
		var run dag.WorkflowRun
		if err := json.Unmarshal(value, &run); err != nil {
			return 0, err
		}
		candidates = append(candidates, runCreatedAt{id, run.CreatedAt})
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].createdAt.Before(candidates[j].createdAt)
	})
	if len(candidates) > pageMax {
		candidates = candidates[:pageMax]
	}
	repaired := 0
	for _, c := range candidates {
		_, err := s.kv.Create(ctx, runIndexPrefix+c.runID, []byte{})
		if err != nil && !errors.Is(err, jetstream.ErrKeyExists) {
			return repaired, err
		}
		repaired++
	}
	return repaired, nil
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
