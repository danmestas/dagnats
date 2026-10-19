// engine/snapshot_cas_cluster_test.go
// Methodology: the snapshot compare-and-set on a real 3-node embedded
// cluster with workflow_runs at R3, the topology cluster mode
// provisions. R3 matters because a replicated stream reports a CAS
// rejection as 10164, not the 10071 a single node returns, and because
// Load's leader-routed read is a stream-leader request there. The
// stale-write interleaving is forced (load, load, save, save). External
// test package: dagnatstest imports packages that import engine.
package engine_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/dagnatstest"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestSnapshotSave_StaleWriterIsRejectedOnR3(t *testing.T) {
	nc := dagnatstest.StartTestCluster(t, 3)
	routes := []string{"a", "b"} // only len matters for replica derivation
	if err := natsutil.SetupAll(
		nc, natsutil.WithCluster(natsutil.ClusterOptions{Routes: routes}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	stream, err := js.Stream(ctx, "KV_workflow_runs")
	if err != nil || stream.CachedInfo().Config.Replicas != 3 {
		t.Fatalf("workflow_runs must be R3 in cluster mode: err=%v", err)
	}
	storeA, storeB := engine.NewSnapshotStore(js), engine.NewSnapshotStore(js)
	run := dag.WorkflowRun{
		RunID: "run-r3", WorkflowID: "wf", Status: dag.RunStatusRunning,
		Steps:     map[string]dag.StepState{"a": {Status: dag.StepStatusRunning}},
		CreatedAt: time.Now().UTC(),
	}
	if err := storeA.SaveInitial(ctx, run); err != nil {
		t.Fatalf("SaveInitial: %v", err)
	}
	runA, errA := storeA.Load(ctx, run.RunID)
	runB, errB := storeB.Load(ctx, run.RunID)
	if errA != nil || errB != nil {
		t.Fatalf("load: %v / %v", errA, errB)
	}
	runA.Status = dag.RunStatusCompleted
	if err := storeA.Save(ctx, runA); err != nil {
		t.Fatalf("save A: %v", err)
	}
	err = storeB.Save(ctx, runB)
	if !errors.Is(err, engine.ErrSnapshotConflict) {
		t.Fatalf("stale save on R3: err=%v, want ErrSnapshotConflict", err)
	}
	got, err := storeB.Load(ctx, run.RunID)
	if err != nil || got.Status != dag.RunStatusCompleted {
		t.Fatalf("after rejected stale save: status=%v err=%v, want completed",
			got.Status, err)
	}
}
