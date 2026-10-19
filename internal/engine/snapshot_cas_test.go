// engine/snapshot_cas_test.go
// Methodology: real embedded NATS server per test. Two SnapshotStores
// stand in for two engines sharing one workflow_runs bucket (cluster
// mode runs one orchestrator per node on the same durable consumer).
// The interleavings are forced by the test itself -- load, load, save,
// save -- so the lost update is reproduced deterministically rather
// than hoped for under real concurrency. Each test asserts both the
// rejected stale write and the surviving newer state.
package engine

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// casStores returns two independent stores over one bucket, plus a
// run already persisted through the creation path.
func casStores(t *testing.T) (*SnapshotStore, *SnapshotStore, dag.WorkflowRun) {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	if err := natsutil.SetupKVBuckets(js, 1); err != nil {
		t.Fatalf("SetupKVBuckets: %v", err)
	}
	run := dag.WorkflowRun{
		RunID: "run-cas", WorkflowID: "wf", Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"a":   {Status: dag.StepStatusRunning},
			"nap": {Status: dag.StepStatusPending},
		},
		CreatedAt: time.Now().UTC(),
	}
	storeA := NewSnapshotStore(js)
	if err := storeA.SaveInitial(context.Background(), run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}
	return storeA, NewSnapshotStore(js), run
}

// TestSnapshotSave_StaleWriterIsRejected is the cluster wedge in
// miniature: engine B loaded the run before engine A advanced it, and
// B's save must not erase A's advance.
func TestSnapshotSave_StaleWriterIsRejected(t *testing.T) {
	ctx := context.Background()
	storeA, storeB, run := casStores(t)
	runA, err := storeA.Load(ctx, run.RunID)
	if err != nil {
		t.Fatalf("load A: %v", err)
	}
	runB, err := storeB.Load(ctx, run.RunID)
	if err != nil {
		t.Fatalf("load B: %v", err)
	}
	runA.Steps["nap"] = dag.StepState{Status: dag.StepStatusCompleted}
	if err := storeA.Save(ctx, runA); err != nil {
		t.Fatalf("save A: %v", err)
	}
	runB.Steps["a"] = dag.StepState{Status: dag.StepStatusCompleted}
	err = storeB.Save(ctx, runB)
	if !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("stale save: err=%v, want ErrSnapshotConflict", err)
	}
	got, err := storeB.Load(ctx, run.RunID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got.Steps["nap"].Status != dag.StepStatusCompleted {
		t.Fatalf("nap = %v: the newer write was overwritten",
			got.Steps["nap"].Status)
	}
	if got.Steps["a"].Status != dag.StepStatusRunning {
		t.Fatalf("a = %v: the stale write landed", got.Steps["a"].Status)
	}
}

// TestSnapshotSave_CopiesOfOneLoadShareTheRevision covers the common
// in-handler shape: a run is loaded once, then saved more than once
// through value copies (save, then enqueueReady saves again). Those
// saves are one writer, not rivals, and must all succeed.
func TestSnapshotSave_CopiesOfOneLoadShareTheRevision(t *testing.T) {
	ctx := context.Background()
	store, _, run := casStores(t)
	loaded, err := store.Load(ctx, run.RunID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Revision == nil || loaded.Revision.Seq() == 0 {
		t.Fatal("Load must attach the snapshot revision")
	}
	later := loaded
	loaded.Steps["a"] = dag.StepState{Status: dag.StepStatusCompleted}
	if err := store.Save(ctx, loaded); err != nil {
		t.Fatalf("first save: %v", err)
	}
	later.Steps["nap"] = dag.StepState{Status: dag.StepStatusRunning}
	if err := store.Save(ctx, later); err != nil {
		t.Fatalf("second save through a copy: %v", err)
	}
	got, err := store.Load(ctx, run.RunID)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if got.Steps["nap"].Status != dag.StepStatusRunning {
		t.Fatalf("nap = %v, want running", got.Steps["nap"].Status)
	}
}

// TestSnapshotSave_DeletedRunIsAConflict: a run pruned between load
// and save must not be resurrected by the save.
func TestSnapshotSave_DeletedRunIsAConflict(t *testing.T) {
	ctx := context.Background()
	store, _, run := casStores(t)
	loaded, err := store.Load(ctx, run.RunID)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if err := store.Delete(ctx, run.RunID); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := store.Load(ctx, run.RunID); !errors.Is(err, ErrRunNotFound) {
		t.Fatalf("load after delete: err=%v, want ErrRunNotFound", err)
	}
	err = store.Save(ctx, loaded)
	if !errors.Is(err, ErrSnapshotConflict) {
		t.Fatalf("save after delete: err=%v, want ErrSnapshotConflict", err)
	}
}

func TestRetrySnapshotConflict(t *testing.T) {
	calls := 0
	err := retrySnapshotConflict(context.Background(), func() error {
		calls++
		if calls < 3 {
			return ErrSnapshotConflict
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d, want nil after 3", err, calls)
	}
	calls = 0
	err = retrySnapshotConflict(context.Background(), func() error {
		calls++
		return ErrSnapshotConflict
	})
	if !errors.Is(err, ErrSnapshotConflict) || calls != snapshotConflictAttemptsMax {
		t.Fatalf("err=%v calls=%d, want conflict after %d",
			err, calls, snapshotConflictAttemptsMax)
	}
	other := errors.New("boom")
	calls = 0
	err = retrySnapshotConflict(context.Background(), func() error {
		calls++
		return other
	})
	if !errors.Is(err, other) || calls != 1 {
		t.Fatalf("err=%v calls=%d, want boom after 1", err, calls)
	}
}

// TestRetrySnapshotConflict_BacksOffWithJitterAndStopsOnCancel: waits
// stay inside [0.5, 1.5) of the doubling mean, are not all identical,
// and a cancelled context ends the retries after the current attempt.
func TestRetrySnapshotConflict_BacksOffWithJitterAndStopsOnCancel(t *testing.T) {
	for attempt := 0; attempt < snapshotConflictAttemptsMax; attempt++ {
		mean := snapshotConflictBackoffBase << attempt
		seen := map[time.Duration]bool{}
		for i := 0; i < 20; i++ {
			wait := snapshotConflictBackoff(attempt)
			if wait < mean/2 || wait >= mean*3/2 {
				t.Fatalf("attempt %d wait %v outside [%v, %v)",
					attempt, wait, mean/2, mean*3/2)
			}
			seen[wait] = true
		}
		if len(seen) < 2 {
			t.Fatalf("attempt %d: 20 waits all equal, no jitter", attempt)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := retrySnapshotConflict(ctx, func() error {
		calls++
		return ErrSnapshotConflict
	})
	if !errors.Is(err, ErrSnapshotConflict) || calls != 1 {
		t.Fatalf("cancelled: err=%v calls=%d, want conflict after 1", err, calls)
	}
}
