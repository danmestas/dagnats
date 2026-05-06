// worker/cancel_skip_test.go
// Tests for the worker's fast-skip behavior when a task's parent run
// has already been cancelled (issue #174). Methodology: pre-mark the
// run in workflow_runs KV, publish a task message for it, observe
// whether the handler runs. Also covers the worker_status snapshot
// write that surfaces drain progress in `dagnats status --detail`
// (issue #182).
package worker

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// putRunStatus saves a WorkflowRun with the given status into the
// workflow_runs KV at key "run.<runID>". Test helper.
func putRunStatus(t *testing.T, js jetstream.JetStream, runID string,
	status dag.RunStatus,
) {
	t.Helper()
	kv, err := js.KeyValue(context.Background(), "workflow_runs")
	if err != nil {
		t.Fatalf("KeyValue(workflow_runs): %v", err)
	}
	body, err := json.Marshal(dag.WorkflowRun{
		RunID:  runID,
		Status: status,
	})
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	_, err = kv.Put(context.Background(), "run."+runID, body)
	if err != nil {
		t.Fatalf("Put: %v", err)
	}
}

func TestWorker_SkipsTasksForCancelledRuns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	const runID = "run-cancelled-1"
	putRunStatus(t, js, runID, dag.RunStatusCancelled)

	var calls atomic.Int64
	w := NewWorker(nc)
	w.Handle("echo", func(ctx TaskContext) error {
		calls.Add(1)
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  runID,
		StepID: "step-a",
		Input:  json.RawMessage(`"hello"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(
		context.Background(), "task.echo."+runID, data,
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Allow the worker enough time to receive, look up the run
	// status, and ack-and-skip the message.
	time.Sleep(2 * time.Second)

	// Positive: handler must NOT have been called.
	if got := calls.Load(); got != 0 {
		t.Fatalf("expected 0 handler calls for cancelled run, got %d", got)
	}
}

// TestWorker_WritesCancelledStatusSnapshot verifies that on each
// cancelled-task skip, the worker writes its counter to the
// worker_status KV bucket so `dagnats status --detail` can aggregate
// drain progress (#182). The bucket is opt-in via SetupAll options
// so older deployments don't see surprise NATS writes.
func TestWorker_WritesCancelledStatusSnapshot(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{
			Bucket: "worker_status",
		}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	const runID = "run-cancelled-snap-1"
	putRunStatus(t, js, runID, dag.RunStatusCancelled)

	w := NewWorker(nc)
	w.Handle("echo", func(ctx TaskContext) error {
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  runID,
		StepID: "step-a",
		Input:  json.RawMessage(`"x"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(
		context.Background(), "task.echo."+runID, data,
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Wait for the worker to receive, skip, and write the snapshot.
	// Poll until the snapshot appears or timeout fires.
	kv, err := js.KeyValue(context.Background(), "worker_status")
	if err != nil {
		t.Fatalf("KeyValue: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	var found bool
	for time.Now().Before(deadline) {
		keys, kerr := kv.Keys(context.Background())
		if kerr == nil && len(keys) > 0 {
			entry, _ := kv.Get(context.Background(), keys[0])
			var snap protocol.WorkerStatusSnapshot
			if entry != nil &&
				json.Unmarshal(entry.Value(), &snap) == nil &&
				snap.CancelledTasksSkipped >= 1 {
				found = true
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !found {
		t.Fatal("expected worker_status snapshot with skipped >= 1")
	}
}

func TestWorker_RunningRunNotSkipped(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	const runID = "run-running-1"
	putRunStatus(t, js, runID, dag.RunStatusRunning)

	var called atomic.Bool
	w := NewWorker(nc)
	w.Handle("echo", func(ctx TaskContext) error {
		called.Store(true)
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()

	payload := protocol.TaskPayload{
		RunID:  runID,
		StepID: "step-a",
		Input:  json.RawMessage(`"hello"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(
		context.Background(), "task.echo."+runID, data,
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	deadline := time.After(5 * time.Second)
	for !called.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s for running run")
		case <-time.After(50 * time.Millisecond):
		}
	}
}

func TestWorker_MissingRunSnapshotProceeds(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	// No run snapshot saved — lookup returns ErrKeyNotFound.

	var called atomic.Bool
	w := NewWorker(nc)
	w.Handle("echo", func(ctx TaskContext) error {
		called.Store(true)
		return ctx.Complete(ctx.Input())
	})
	w.Start()
	defer w.Stop()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	payload := protocol.TaskPayload{
		RunID:  "run-missing-1",
		StepID: "step-a",
		Input:  json.RawMessage(`"hello"`),
	}
	data, _ := json.Marshal(payload)
	if _, err := js.Publish(
		context.Background(), "task.echo."+payload.RunID, data,
	); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// Defensive default: missing run KV → execute (don't drop work).
	deadline := time.After(5 * time.Second)
	for !called.Load() {
		select {
		case <-deadline:
			t.Fatal("handler not called within 5s; missing run should default to execute")
		case <-time.After(50 * time.Millisecond):
		}
	}
}
