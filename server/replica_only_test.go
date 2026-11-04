// server/replica_only_test.go
// Tests for a replica-only node (engine: false). Methodology: three real
// dagnats servers in process, routed into one cluster, the third with
// the engine off. A run started through an engine node completes while
// the replica-only node runs no orchestrator and serves no control
// plane; a control-plane request sent to the replica-only node is
// answered by another node; a program connected to it authenticates and
// registers through the others; its health says it is replica-only and
// its other HTTP routes answer 503. Every wait is bounded.
package server

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/program"
	"github.com/danmestas/dagnats/worker"
)

const replicaClusterWait = 60 * time.Second

// startReplicaCluster starts n0 and n1 with the engine on and n2
// replica-only, and returns them once all three are ready.
func startReplicaCluster(t *testing.T) []*Server {
	t.Helper()
	ports := freeTCPPorts(t, 3)
	routes := make([]string, len(ports))
	for i, port := range ports {
		routes[i] = fmt.Sprintf("nats://127.0.0.1:%d", port)
	}
	servers := make([]*Server, len(ports))
	errChs := make([]chan error, len(ports))
	for i, port := range ports {
		cfg := testConfig(t)
		cfg.NATSClusterName = "replica-test"
		cfg.NATSClusterRoutes = routes
		cfg.NATSClusterPort = port
		cfg.NATSClusterHost = "127.0.0.1"
		cfg.NATSServerName = fmt.Sprintf("n%d", i)
		cfg.MaxStoreBytes = 512 << 20
		cfg.ReplicaOnly = i == 2
		servers[i] = New(cfg)
		errChs[i] = make(chan error, 1)
		go func(srv *Server, errCh chan error) { errCh <- srv.Run() }(servers[i], errChs[i])
	}
	// One cleanup stops all three together. Stopped one at a time, the
	// last node shut down without JetStream quorum, its shutdown could
	// pass shutdownDeadline, and Run returned while its NATS server still
	// wrote the store t.TempDir was removing.
	t.Cleanup(func() { stopReplicaCluster(t, servers, errChs) })
	deadline := time.Now().Add(replicaClusterWait)
	for _, srv := range servers {
		for srv.NATSURL() == "" {
			if time.Now().After(deadline) {
				t.Fatal("cluster not ready")
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
	return servers
}

// stopReplicaCluster stops every server at once and fails if any did
// not shut down cleanly within the deadline.
func stopReplicaCluster(t *testing.T, servers []*Server, errChs []chan error) {
	t.Helper()
	for _, srv := range servers {
		srv.Stop()
	}
	for i, errCh := range errChs {
		select {
		case err := <-errCh:
			if err != nil {
				t.Errorf("n%d shutdown: %v", i, err)
			}
		case <-time.After(shutdownDeadline + 5*time.Second):
			t.Errorf("n%d did not stop", i)
		}
	}
}

func dialNode(t *testing.T, srv *Server) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(srv.NATSURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

func request(t *testing.T, nc *nats.Conn, subject string, body any) []byte {
	t.Helper()
	data, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	reply, err := nc.Request(subject, data, 10*time.Second)
	if err != nil {
		t.Fatalf("%s: %v", subject, err)
	}
	return reply.Data
}

func TestReplicaOnlyNode(t *testing.T) {
	servers := startReplicaCluster(t)
	engine, replica := servers[0], servers[2]
	// Negative: the replica-only node runs no engine and serves no API.
	if replica.orch != nil || replica.natsAPI != nil || replica.trig != nil {
		t.Fatal("replica-only node started engine components")
	}
	if engine.orch == nil || engine.natsAPI == nil {
		t.Fatal("engine node started without its engine")
	}
	viaReplica := dialNode(t, replica)
	builder := dag.NewWorkflow("replica.flow")
	builder.Task("only", "replica.work")
	def, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	// Positive: a control-plane request sent to the replica-only node
	// is answered by an engine node.
	if reply := request(t, viaReplica, "api.workflows.register", def); !strings.Contains(
		string(reply), "registered") {
		t.Fatalf("register via replica-only node = %s", reply)
	}
	w := worker.NewWorker(dialNode(t, engine))
	w.Handle("replica.work", func(tc worker.TaskContext) error { return tc.Complete(nil) })
	if err := w.Start(); err != nil {
		t.Fatalf("worker: %v", err)
	}
	t.Cleanup(w.Stop)
	var started struct {
		RunID string `json:"run_id"`
	}
	reply := request(t, dialNode(t, engine), "api.runs.start", map[string]string{
		"workflow": def.Name,
	})
	if err := json.Unmarshal(reply, &started); err != nil || started.RunID == "" {
		t.Fatalf("runs.start = %s, %v", reply, err)
	}
	waitRunCompleted(t, viaReplica, started.RunID)
	assertReplicaHealth(t, replica)
	assertProgramThroughReplica(t, replica, "n2")
}

// waitRunCompleted polls api.runs.get through nc until the run completes.
func waitRunCompleted(t *testing.T, nc *nats.Conn, runID string) {
	t.Helper()
	deadline := time.Now().Add(replicaClusterWait)
	for {
		var run dag.WorkflowRun
		msg, err := nc.Request("api.runs.get", []byte(runID), 10*time.Second)
		if err != nil {
			t.Fatalf("api.runs.get: %v", err)
		}
		reply := msg.Data
		if err := json.Unmarshal(reply, &run); err == nil &&
			run.Status == dag.RunStatusCompleted {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("run %s never completed: %s", runID, reply)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func assertReplicaHealth(t *testing.T, replica *Server) {
	t.Helper()
	get := func(path string) (int, string) {
		resp, err := http.Get("http://" + replica.HTTPAddr() + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		defer resp.Body.Close()
		body, err := io.ReadAll(resp.Body)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		return resp.StatusCode, string(body)
	}
	// Positive: health answers and says the node is replica-only.
	if status, body := get("/health/cluster"); status != http.StatusOK ||
		!strings.Contains(body, `"replica_only":true`) {
		t.Fatalf("/health/cluster = %d %s, want 200 naming replica_only", status, body)
	}
	if status, _ := get("/health"); status != http.StatusOK {
		t.Fatalf("/health = %d, want 200", status)
	}
	// Negative: the rest of the HTTP API says replica-only, not half-works.
	for _, path := range []string{"/v1/runs", "/console/", "/workflows"} {
		if status, body := get(path); status != http.StatusServiceUnavailable ||
			!strings.Contains(body, "replica-only") {
			t.Fatalf("GET %s = %d %s, want 503 replica-only", path, status, body)
		}
	}
}

// assertProgramThroughReplica runs a program dialed to the replica-only
// node: it registers through the engine nodes' programs service and logs
// in with its NKey on the replica-only node, which loaded its user.
func assertProgramThroughReplica(t *testing.T, replica *Server, replicaName string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), replicaClusterWait)
	defer cancel()
	prog := &tinyProgram{}
	running, err := program.Run(ctx, prog, program.Options{
		URL: replica.NATSURL(), SeedPath: filepath.Join(t.TempDir(), "seed"),
	})
	if err != nil {
		t.Fatalf("program.Run via the replica-only node: %v", err)
	}
	defer running.Stop(ctx) //nolint:errcheck // test teardown
	conn := prog.conn.Load()
	if conn == nil {
		t.Fatal("program never started")
	}
	// The program's own NKey login was accepted by the replica-only
	// node itself, not by a node it fell over to.
	if got := conn.ConnectedServerName(); got != replicaName {
		t.Fatalf("program connected to %q, want the replica-only node %s", got, replicaName)
	}
}
