package engine

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// runsLowWaterPercent is where a size pass stops, as a percent of the
// share: trimming below the share rather than to it keeps every few new
// runs from triggering another pass.
const runsLowWaterPercent = 90

// PruneTerminalBySize keeps the workflow_runs bucket under shareBytes.
// The bucket has no MaxBytes, because a full KV rejects writes and a
// rejected run write is a lost run, so this pass is what bounds it. When
// the bucket's stored bytes exceed shareBytes it deletes the OLDEST
// prunable runs by CompletedAt until the freed value bytes bring it under
// runsLowWaterPercent of the share. Prunable means exactly what it means
// for PruneTerminal: terminal, stamped, not ReleasePending, so in-flight
// runs are never touched. At most maxPrune runs go per call.
//
// Fail-safe like PruneTerminal: every candidate is read before anything
// is deleted, and a corrupt value or failed read aborts the pass with
// zero deletions.
func (s *SnapshotStore) PruneTerminalBySize(
	ctx context.Context, shareBytes int64, maxPrune int,
) (int, error) {
	if shareBytes <= 0 {
		panic("SnapshotStore.PruneTerminalBySize: shareBytes must be positive")
	}
	if maxPrune <= 0 {
		panic("SnapshotStore.PruneTerminalBySize: maxPrune must be positive")
	}
	status, err := s.kv.Status(ctx)
	if err != nil {
		return 0, fmt.Errorf("workflow_runs status: %w", err)
	}
	used := int64(status.Bytes())
	if used <= shareBytes {
		return 0, nil
	}
	excess := used - shareBytes*runsLowWaterPercent/100
	oldest, err := s.collectOldestPrunable(ctx, maxPrune)
	if err != nil {
		return 0, err
	}
	return s.deletePrunedRuns(ctx, keysToFree(oldest, excess))
}

// collectOldestPrunable scans run.* keys and returns the maxPrune
// prunable runs that completed first, oldest first. Only maxPrune
// candidates are held at once, never the whole population.
func (s *SnapshotStore) collectOldestPrunable(
	ctx context.Context, maxPrune int,
) ([]pruneCandidate, error) {
	if maxPrune <= 0 {
		panic("SnapshotStore.collectOldestPrunable: maxPrune must be positive")
	}
	keys, err := natsutil.KVKeys(ctx, s.kv)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	if runKeyCountOf(keys) > runKeyScanMax {
		panic("SnapshotStore.collectOldestPrunable: key set exceeds bound")
	}
	kept := make(newestFirst, 0, maxPrune+1)
	for _, key := range keys {
		if !isRunKey(key) {
			continue
		}
		candidate, ok, err := s.loadPrunable(ctx, key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		heap.Push(&kept, candidate)
		if kept.Len() > maxPrune {
			heap.Pop(&kept) // drop the newest: it is the last to go
		}
	}
	sort.Slice(kept, func(i, j int) bool { return kept.Less(j, i) })
	return kept, nil
}

// keysToFree takes candidates, oldest first, until their value bytes
// cover excess (or the candidates run out).
func keysToFree(oldestFirst []pruneCandidate, excess int64) []string {
	if excess <= 0 {
		panic("keysToFree: excess must be positive")
	}
	if !sort.SliceIsSorted(oldestFirst, func(i, j int) bool {
		return oldestFirst[i].completedAt.Before(oldestFirst[j].completedAt)
	}) {
		panic("keysToFree: candidates must be oldest first")
	}
	var freed int64
	keys := make([]string, 0, len(oldestFirst))
	for _, candidate := range oldestFirst {
		if freed >= excess {
			break
		}
		keys = append(keys, candidate.key)
		freed += candidate.valueBytes
	}
	return keys
}

// newestFirst is a heap whose root is the most recently completed
// candidate, so a bounded collection can evict it in O(log n).
type newestFirst []pruneCandidate

func (h newestFirst) Len() int { return len(h) }

func (h newestFirst) Less(i, j int) bool {
	if h[i].completedAt.Equal(h[j].completedAt) {
		return h[i].key > h[j].key
	}
	return h[i].completedAt.After(h[j].completedAt)
}

func (h newestFirst) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *newestFirst) Push(x any) { *h = append(*h, x.(pruneCandidate)) }

func (h *newestFirst) Pop() any {
	old := *h
	last := old[len(old)-1]
	*h = old[:len(old)-1]
	return last
}
