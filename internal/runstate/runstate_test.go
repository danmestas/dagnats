// runstate_test.go
// Methodology: real embedded NATS with the provisioned workflow_runs
// bucket. Writes run snapshots the way the engine stores them (a JSON
// dag.WorkflowRun under run.<id>) and asks Cancelled about each.
// Positive: a cancelled run reads cancelled. Negative space: every other
// status, a missing run, an unprovisioned bucket and a corrupt snapshot
// all read not-cancelled (fail open).
package runstate

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func runsBucket(t *testing.T) jetstream.KeyValue {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	kv, err := js.KeyValue(context.Background(), "workflow_runs")
	if err != nil {
		t.Fatalf("workflow_runs: %v", err)
	}
	return kv
}

func putRun(t *testing.T, kv jetstream.KeyValue, runID string, status dag.RunStatus) {
	t.Helper()
	data, err := json.Marshal(dag.WorkflowRun{RunID: runID, Status: status})
	if err != nil {
		t.Fatalf("marshal run: %v", err)
	}
	if _, err := kv.Put(context.Background(), "run."+runID, data); err != nil {
		t.Fatalf("put run: %v", err)
	}
}

func TestCancelledReadsTheRunStatus(t *testing.T) {
	kv := runsBucket(t)
	ctx := context.Background()
	putRun(t, kv, "cancelled", dag.RunStatusCancelled)
	if !Cancelled(ctx, kv, "cancelled") {
		t.Fatal("a cancelled run read not-cancelled")
	}
	for _, status := range []dag.RunStatus{
		dag.RunStatusPending, dag.RunStatusRunning, dag.RunStatusCompleted,
		dag.RunStatusFailed, dag.RunStatusCompensated,
	} {
		putRun(t, kv, "other", status)
		if Cancelled(ctx, kv, "other") {
			t.Fatalf("a %s run read cancelled", status)
		}
	}
}

func TestCancelledFailsOpen(t *testing.T) {
	kv := runsBucket(t)
	ctx := context.Background()
	if Cancelled(ctx, kv, "never-stored") {
		t.Fatal("a missing run read cancelled")
	}
	if Cancelled(ctx, nil, "any") {
		t.Fatal("an unprovisioned bucket read cancelled")
	}
	if _, err := kv.Put(ctx, "run.corrupt", []byte("{not json")); err != nil {
		t.Fatalf("put corrupt: %v", err)
	}
	if Cancelled(ctx, kv, "corrupt") {
		t.Fatal("a corrupt snapshot read cancelled")
	}
}
