package engine

import (
	"context"
	"encoding/json"
	"errors"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// ErrRunNotFound is returned by Load when no snapshot exists for the given run ID.
// Callers can distinguish missing-run from other NATS errors with errors.Is.
var ErrRunNotFound = errors.New("workflow run not found")

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
