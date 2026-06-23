package engine

import (
	"context"
	"encoding/json"
	"errors"
	"sort"

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
