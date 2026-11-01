// dagnatstest/kv_read_your_writes_test.go
// Methodology: read-your-writes for the KV buckets a run start or a
// dispatch reads right after a write, on a real 3-node in-process
// cluster with every bucket at R3. One engine and one API service run
// per node, each on its own connection to that node, so reads land on
// every replica. Cases: concurrent starts of a singleton admit exactly
// one run per key; a hard-sticky run's second step runs on the worker
// that ran its first; a duplicate start with the same idempotency key
// returns the first run. Each case has a positive and a negative
// assertion; every wait is bounded. Lives here because the cluster
// helper is in this package, which imports engine and api.
package dagnatstest

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/worker"
)

const (
	rywSettleWait      = 90 * time.Second
	singletonRounds    = 15
	singletonStarts    = 6
	stickyRuns         = 60
	idempotencyRepeats = 150
)

// rywNode is one node's connection, engine and API service.
type rywNode struct {
	nc  *nats.Conn
	svc *api.Service
}

func startRYWCluster(t *testing.T) []rywNode {
	t.Helper()
	cluster := startClusterWithRetry(t, minTestClusterNodes, allocateFreePorts)
	t.Cleanup(cluster.shutdown)
	waitForRoutesMeshed(t, cluster.servers, minTestClusterNodes)
	waitForJetStreamLeader(t, cluster.servers)
	nc := connectAllNodes(t, cluster)
	confirmClusterQuorum(t, nc, minTestClusterNodes)
	routes := []string{"a", "b"} // only len matters for replica derivation
	err := natsutil.SetupAll(nc, natsutil.WithCluster(natsutil.ClusterOptions{Routes: routes}))
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	nodes := make([]rywNode, 0, len(cluster.servers))
	for _, server := range cluster.servers {
		conn, err := nats.Connect(server.ClientURL())
		if err != nil {
			t.Fatalf("connect %s: %v", server.ClientURL(), err)
		}
		t.Cleanup(conn.Close)
		orch := engine.NewOrchestrator(conn)
		orch.Start()
		t.Cleanup(orch.Stop)
		nodes = append(nodes, rywNode{nc: conn, svc: api.NewService(conn)})
	}
	return nodes
}

// waitRuns polls each run in ids until done reports it settled and
// returns the settled runs.
func waitRuns(
	t *testing.T, svc *api.Service, ids []string, done func(dag.WorkflowRun) bool,
) []dag.WorkflowRun {
	t.Helper()
	deadline := time.Now().Add(rywSettleWait)
	runs := make([]dag.WorkflowRun, len(ids))
	for i, id := range ids {
		for {
			run, err := svc.GetRun(context.Background(), id)
			if err == nil && done(run) {
				runs[i] = run
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("run %s never settled: %+v, %v", id, run.Status, err)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	return runs
}

func TestCluster_SingletonAdmitsOneRunPerKey(t *testing.T) {
	nodes := startRYWCluster(t)
	wb := dag.NewWorkflow("singleton-ryw")
	wb.Task("park", "singleton.park") // no worker: the admitted run stays running
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	def.Singleton = &dag.SingletonConfig{Mode: dag.SingletonModeSkip, Key: "k"}
	if err := nodes[0].svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	for round := 0; round < singletonRounds; round++ {
		ids := startConcurrently(t, nodes, def.Name, fmt.Sprintf(`{"k":%d}`, round))
		runs := waitRuns(t, nodes[0].svc, ids, func(run dag.WorkflowRun) bool {
			return run.Status == dag.RunStatusRunning || run.Status == dag.RunStatusCancelled
		})
		running := 0
		for _, run := range runs {
			if run.Status == dag.RunStatusRunning {
				running++
			}
		}
		if running != 1 {
			t.Fatalf("round %d: %d of %d concurrent starts admitted, want exactly 1",
				round, running, len(runs))
		}
	}
}

// startConcurrently starts singletonStarts runs of name at once,
// spread over the nodes, and returns their ids.
func startConcurrently(t *testing.T, nodes []rywNode, name, input string) []string {
	t.Helper()
	ids := make([]string, singletonStarts)
	errs := make([]error, singletonStarts)
	var wg sync.WaitGroup
	for i := 0; i < singletonStarts; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			svc := nodes[i%len(nodes)].svc
			ids[i], errs[i] = svc.StartRun(context.Background(), name, []byte(input))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("StartRun %d: %v", i, err)
		}
	}
	return ids
}

func TestCluster_HardStickySecondStepRunsOnTheBoundWorker(t *testing.T) {
	nodes := startRYWCluster(t)
	var mu sync.Mutex
	ranOn := map[string]string{} // run.step -> worker name
	for i := 0; i < 2; i++ {
		name := fmt.Sprintf("w%d", i)
		w := worker.NewWorker(nodes[i].nc)
		record := func(tc worker.TaskContext) error {
			mu.Lock()
			ranOn[tc.RunID()+"."+tc.StepID()] = name
			mu.Unlock()
			return tc.Complete([]byte(`"ok"`))
		}
		w.Handle("sticky.first", record)
		w.Handle("sticky.second", record)
		w.Start()
		t.Cleanup(w.Stop)
	}
	wb := dag.NewWorkflow("sticky-ryw")
	first := wb.Task("a", "sticky.first")
	wb.Task("b", "sticky.second").After(first)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// Hard sticky requires a run timeout, which the builder does not set.
	def.Sticky, def.Timeout = dag.StickyHard, time.Minute
	if err := nodes[0].svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	ids := make([]string, stickyRuns)
	for i := range ids {
		ids[i], err = nodes[i%len(nodes)].svc.StartRun(context.Background(), def.Name, nil)
		if err != nil {
			t.Fatalf("StartRun %d: %v", i, err)
		}
	}
	waitRuns(t, nodes[0].svc, ids, func(run dag.WorkflowRun) bool {
		return run.Status == dag.RunStatusCompleted
	})
	mu.Lock()
	defer mu.Unlock()
	used := map[string]bool{}
	for _, id := range ids {
		first, second := ranOn[id+".a"], ranOn[id+".b"]
		if first == "" || first != second {
			t.Fatalf("run %s: step a on %q, step b on %q; want the bound worker", id, first, second)
		}
		used[first] = true
	}
	if len(used) == 0 {
		t.Fatal("no run recorded a worker")
	}
}

func TestCluster_DuplicateStartReturnsTheFirstRun(t *testing.T) {
	nodes := startRYWCluster(t)
	wb := dag.NewWorkflow("idempotent-ryw").WithIdempotencyKey("id")
	wb.Task("park", "idempotent.park")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if err := nodes[0].svc.RegisterWorkflow(context.Background(), def); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}
	seen := map[string]bool{}
	for i := 0; i < idempotencyRepeats; i++ {
		input := []byte(fmt.Sprintf(`{"id":"req-%d"}`, i))
		first, err := nodes[i%len(nodes)].svc.StartRun(context.Background(), def.Name, input)
		if err != nil {
			t.Fatalf("StartRun %d: %v", i, err)
		}
		again, err := nodes[(i+1)%len(nodes)].svc.StartRun(context.Background(), def.Name, input)
		if err != nil {
			t.Fatalf("duplicate StartRun %d: %v", i, err)
		}
		if again != first {
			t.Fatalf("duplicate %d started run %s, want the first run %s", i, again, first)
		}
		if seen[first] {
			t.Fatalf("request %d reused run %s of another key", i, first)
		}
		seen[first] = true
	}
}
