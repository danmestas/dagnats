// engine/snapshot_test.go
// Tests for KV snapshot operations: store and retrieve WorkflowRun state.
// Methodology: uses real embedded NATS server. Each test gets its own server.
// Tests write snapshots, read them back, verify field fidelity, and check missing key behavior.
package engine

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSnapshotWriteAndRead(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js, 1)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)
	run := dag.WorkflowRun{
		RunID: "run-123", WorkflowID: "test-wf", Status: dag.RunStatusRunning,
		Steps: map[string]dag.StepState{
			"step-a": {Status: dag.StepStatusCompleted, Attempts: 1, Output: []byte(`"ok"`)},
			"step-b": {Status: dag.StepStatusPending},
		},
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	err = store.Save(context.Background(), run)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	got, err := store.Load(context.Background(), run.RunID)
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got.RunID != run.RunID {
		t.Fatalf("RunID = %q, want %q", got.RunID, run.RunID)
	}
	if got.Status != dag.RunStatusRunning {
		t.Fatalf("Status = %v, want Running", got.Status)
	}
	if len(got.Steps) != 2 {
		t.Fatalf("Steps count = %d, want 2", len(got.Steps))
	}
	if got.Steps["step-a"].Status != dag.StepStatusCompleted {
		t.Fatalf("step-a Status = %v, want Completed", got.Steps["step-a"].Status)
	}
}

func TestSnapshotLoadNotFound(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js, 1)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)
	_, err = store.Load(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent run, got nil")
	}
	if err != ErrRunNotFound {
		t.Fatalf("expected ErrRunNotFound, got: %v", err)
	}
}

func TestSnapshotUpdate(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js, 1)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)
	run := dag.WorkflowRun{
		RunID: "run-456", WorkflowID: "test-wf", Status: dag.RunStatusRunning,
		Steps:     map[string]dag.StepState{"a": {Status: dag.StepStatusPending}},
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	err = store.Save(context.Background(), run)
	if err != nil {
		t.Fatalf("first Save failed: %v", err)
	}
	run.Steps["a"] = dag.StepState{Status: dag.StepStatusCompleted, Attempts: 1}
	run.Status = dag.RunStatusCompleted
	err = store.Save(context.Background(), run)
	if err != nil {
		t.Fatalf("second Save failed: %v", err)
	}
	got, err := store.Load(context.Background(), "run-456")
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if got.Status != dag.RunStatusCompleted {
		t.Fatalf("Status = %v, want Completed", got.Status)
	}
	if got.Steps["a"].Status != dag.StepStatusCompleted {
		t.Fatalf("step-a Status = %v, want Completed", got.Steps["a"].Status)
	}
}

func TestSnapshotListAllEmpty(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js, 1)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)

	// Positive: empty bucket returns empty slice, no error.
	runs, err := store.ListAll(context.Background(), 100)
	if err != nil {
		t.Fatalf("ListAll on empty bucket failed: %v", err)
	}
	if len(runs) != 0 {
		t.Fatalf("expected 0 runs, got %d", len(runs))
	}
}

func TestSnapshotListAllBounded(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js, 1)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)

	// Save 3 runs.
	for i := 0; i < 3; i++ {
		run := dag.WorkflowRun{
			RunID:      fmt.Sprintf("bound-%d", i),
			WorkflowID: "wf",
			Status:     dag.RunStatusRunning,
			Steps: map[string]dag.StepState{
				"a": {Status: dag.StepStatusPending},
			},
			CreatedAt: time.Now().UTC(),
		}
		if err := store.Save(context.Background(), run); err != nil {
			t.Fatalf("Save failed: %v", err)
		}
	}

	// Positive: maxRuns=2 limits results.
	runs, err := store.ListAll(context.Background(), 2)
	if err != nil {
		t.Fatalf("ListAll failed: %v", err)
	}
	if len(runs) != 2 {
		t.Fatalf("expected 2 runs, got %d", len(runs))
	}

	// Positive: maxRuns=10 returns all 3.
	allRuns, err := store.ListAll(context.Background(), 10)
	if err != nil {
		t.Fatalf("ListAll failed: %v", err)
	}
	if len(allRuns) != 3 {
		t.Fatalf("expected 3 runs, got %d", len(allRuns))
	}
}

func TestSnapshotListAll(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js, 1)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)
	run1 := dag.WorkflowRun{
		RunID:      "run-001",
		WorkflowID: "wf-a",
		Status:     dag.RunStatusRunning,
		Steps:      map[string]dag.StepState{"a": {Status: dag.StepStatusPending}},
		CreatedAt:  time.Now().UTC().Truncate(time.Millisecond),
	}
	run2 := dag.WorkflowRun{
		RunID:      "run-002",
		WorkflowID: "wf-b",
		Status:     dag.RunStatusCompleted,
		Steps:      map[string]dag.StepState{"b": {Status: dag.StepStatusCompleted}},
		CreatedAt:  time.Now().UTC().Add(1 * time.Second).Truncate(time.Millisecond),
	}
	err = store.Save(context.Background(), run1)
	if err != nil {
		t.Fatalf("Save run1 failed: %v", err)
	}
	err = store.Save(context.Background(), run2)
	if err != nil {
		t.Fatalf("Save run2 failed: %v", err)
	}
	runs, err := store.ListAll(context.Background(), 100)
	if err != nil {
		t.Fatalf("ListAll failed: %v", err)
	}
	if len(runs) < 2 {
		t.Fatalf("expected at least 2 runs, got %d", len(runs))
	}
	foundRun1 := false
	foundRun2 := false
	for _, run := range runs {
		if run.RunID == "run-001" {
			foundRun1 = true
		}
		if run.RunID == "run-002" {
			foundRun2 = true
		}
	}
	if !foundRun1 || !foundRun2 {
		t.Fatal("ListAll did not return both expected runs")
	}
}
