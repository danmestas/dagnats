package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrRunNotFound is returned by Load when no snapshot exists for the given run ID.
// Callers can distinguish missing-run from other NATS errors with errors.Is.
var ErrRunNotFound = errors.New("workflow run not found")

// runKeyScanMax bounds the workflow_runs key set a single ListAll /
// CountAll scan will tolerate. A run population beyond this points to
// missing retention (#453); we panic rather than silently degrade.
const runKeyScanMax = 1_000_000

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
	return err
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
	if len(keys) > runKeyScanMax {
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

	entries, err := natsutil.ParallelGetJS(
		s.kv, filtered, natsutil.DefaultParallelism,
	)
	if err != nil {
		return nil, err
	}

	runs := make([]dag.WorkflowRun, 0, len(entries))
	for _, entry := range entries {
		var run dag.WorkflowRun
		if err := json.Unmarshal(entry.Value(), &run); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// ListRecent returns the genuinely most-recent limit workflow runs,
// newest first (CreatedAt descending). It fetches the full run.*
// population, sorts globally, THEN truncates — so callers get the real
// latest N rather than an arbitrary unordered subset (#452). The
// full-population fetch is bounded by the number of run.* keys; #453
// tracks a time-ordered index to avoid the scan.
func (s *SnapshotStore) ListRecent(
	ctx context.Context, limit int,
) ([]dag.WorkflowRun, error) {
	if s.kv == nil {
		panic("SnapshotStore.ListRecent: kv bucket must not be nil")
	}
	if limit <= 0 {
		panic("SnapshotStore.ListRecent: limit must be positive")
	}
	runs, err := s.fetchAllRuns(ctx)
	if err != nil {
		return nil, err
	}
	sort.Slice(runs, func(i, j int) bool {
		return runs[i].CreatedAt.After(runs[j].CreatedAt)
	})
	if len(runs) > limit {
		runs = runs[:limit]
	}
	return runs, nil
}

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
	if len(keys) > runKeyScanMax {
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

// fetchAllRuns loads every run.* value from the bucket using bounded
// parallel Get. The loop is bounded by the number of run.* keys.
func (s *SnapshotStore) fetchAllRuns(
	ctx context.Context,
) ([]dag.WorkflowRun, error) {
	if s.kv == nil {
		panic("SnapshotStore.fetchAllRuns: kv bucket must not be nil")
	}
	keys, err := s.kv.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return []dag.WorkflowRun{}, nil
		}
		return nil, err
	}
	if len(keys) > runKeyScanMax {
		panic("SnapshotStore.fetchAllRuns: key set exceeds bound")
	}
	filtered := make([]string, 0, len(keys))
	for _, key := range keys {
		if isRunKey(key) {
			filtered = append(filtered, key)
		}
	}
	if len(filtered) == 0 {
		return []dag.WorkflowRun{}, nil
	}
	entries, err := natsutil.ParallelGetJS(
		s.kv, filtered, natsutil.DefaultParallelism,
	)
	if err != nil {
		return nil, err
	}
	runs := make([]dag.WorkflowRun, 0, len(entries))
	for _, entry := range entries {
		var run dag.WorkflowRun
		if err := json.Unmarshal(entry.Value(), &run); err != nil {
			return nil, err
		}
		runs = append(runs, run)
	}
	return runs, nil
}

// isRunKey reports whether a KV key names a workflow run snapshot.
func isRunKey(key string) bool {
	return len(key) >= 4 && key[:4] == "run."
}
