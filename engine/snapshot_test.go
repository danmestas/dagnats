// engine/snapshot_test.go
// Tests for KV snapshot operations: store and retrieve WorkflowRun state.
// Methodology: uses real embedded NATS server. Each test gets its own server.
// Tests write snapshots, read them back, verify field fidelity, and check missing key behavior.
package engine

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
)

func TestSnapshotWriteAndRead(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js)
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
	err = store.Save(run)
	if err != nil {
		t.Fatalf("Save failed: %v", err)
	}
	got, err := store.Load(run.RunID)
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
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)
	_, err = store.Load("nonexistent")
	if err == nil {
		t.Fatal("expected error for nonexistent run, got nil")
	}
	if err != ErrRunNotFound {
		t.Fatalf("expected ErrRunNotFound, got: %v", err)
	}
}

func TestSnapshotUpdate(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js)
	if err != nil {
		t.Fatalf("SetupKVBuckets failed: %v", err)
	}
	store := NewSnapshotStore(js)
	run := dag.WorkflowRun{
		RunID: "run-456", WorkflowID: "test-wf", Status: dag.RunStatusRunning,
		Steps:     map[string]dag.StepState{"a": {Status: dag.StepStatusPending}},
		CreatedAt: time.Now().UTC().Truncate(time.Millisecond),
	}
	err = store.Save(run)
	if err != nil {
		t.Fatalf("first Save failed: %v", err)
	}
	run.Steps["a"] = dag.StepState{Status: dag.StepStatusCompleted, Attempts: 1}
	run.Status = dag.RunStatusCompleted
	err = store.Save(run)
	if err != nil {
		t.Fatalf("second Save failed: %v", err)
	}
	got, err := store.Load("run-456")
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

func TestSnapshotListAll(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	err = natsutil.SetupKVBuckets(js)
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
	err = store.Save(run1)
	if err != nil {
		t.Fatalf("Save run1 failed: %v", err)
	}
	err = store.Save(run2)
	if err != nil {
		t.Fatalf("Save run2 failed: %v", err)
	}
	runs, err := store.ListAll(100)
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
