// Methodology: true multi-process cluster test. Builds the real dagnats
// binary and starts three `dagnats serve` processes on localhost with
// distinct client/HTTP/route ports and data dirs, routed to each other.
// The test waits for /health/cluster to report all three members, runs a
// workflow through the cluster, runs several concurrent multi-step runs
// (task, sleep, task, task) and requires each run's stored snapshot --
// not just its history -- to reach completed, SIGKILLs one node, then
// proves the
// survivors still report healthy (HTTP 200, 2 of 3 nodes up) and complete
// a second workflow.
//
// Heavy (three servers, ~30-60s), so it is opt-in: set
// DAGNATS_E2E_CLUSTER=1 (the dedicated cluster-e2e CI job and
// `make test-cluster-e2e` do). Every wait is bounded and the sum of the
// bounds fits inside totalBudget, which the test checks against the
// `go test -timeout` deadline up front. Every child runs in its own
// process group that t.Cleanup SIGKILLs; if the test binary itself dies
// (timeout panic, SIGKILL) the children carry --die-with-parent and exit
// when it does, including while still starting up. All nodes share one
// host: this proves process-level clustering, not cross-machine
// networking.
package cluster

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/nats-io/nats.go"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/e2e/harness"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/worker"
)

const (
	nodeCount        = 3
	formTimeout      = 180 * time.Second
	degradeTimeout   = 60 * time.Second
	runTimeout       = 60 * time.Second
	failoverTimeout  = 60 * time.Second
	healthPollPeriod = 500 * time.Millisecond
	// chainRunCount concurrent multi-step runs, each sleeping
	// chainSleep, must all finish within one shared chainTimeout.
	chainRunCount = 8
	chainSleep    = 2 * time.Second
	chainTimeout  = 60 * time.Second
	// startAttemptsMax bounds relaunches when a node dies right after
	// start, which is how a port picked by freePort and taken before the
	// node bound it shows up.
	startAttemptsMax = 3
	// earlyExitWindow is how long after launch a node exit counts as a
	// start failure (retry) rather than a crash (fail).
	earlyExitWindow = 15 * time.Second
	// totalBudget is the worst-case wall time: build, up to
	// startAttemptsMax early-exit windows, formation, one run and the
	// chain runs before the kill, degradation, one run after, plus
	// slack. The test refuses to start if the go test deadline is closer
	// than this.
	totalBudget = 9 * time.Minute
)

// errEarlyExit marks a node that exited inside earlyExitWindow.
var errEarlyExit = errors.New("node exited during start")

// node is one spawned `dagnats serve` process.
type node struct {
	name        string
	natsPort    int
	httpAddr    string
	clusterPort int
	dataDir     string
	logPath     string
	cmd         *exec.Cmd
	exited      chan struct{} // closed once the process has been reaped
	started     time.Time
}

// healthBody is the subset of /health/cluster the test asserts on.
type healthBody struct {
	Mode           string `json:"mode"`
	OK             bool   `json:"ok"`
	ExpectedPeers  int    `json:"expected_peers"`
	ConnectedPeers int    `json:"connected_peers"`
	NodesExpected  int    `json:"nodes_expected"`
	NodesUp        int    `json:"nodes_up"`
	Leader         string `json:"leader"`
	Node           string `json:"node"`
	Members        []struct {
		Name string `json:"name"`
	} `json:"members"`
	JetStream struct {
		Unavailable []string `json:"unavailable"`
		Degraded    []string `json:"degraded"`
		Streams     map[string]struct {
			Replicas int `json:"replicas"`
		} `json:"streams"`
	} `json:"jetstream"`
}

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer ln.Close()
	return ln.Addr().(*net.TCPAddr).Port
}

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "dagnats")
	out, err := exec.Command("go", "build", "-o", bin, "../../cmd/dagnats").
		CombinedOutput()
	if err != nil {
		t.Fatalf("build dagnats: %v\n%s", err, out)
	}
	return bin
}

// startNode launches one server in its own process group and registers
// cleanup that kills the whole group.
func startNode(t *testing.T, bin string, self node, all []node) *node {
	t.Helper()
	routes := ""
	for _, peer := range all {
		if peer.name == self.name {
			continue
		}
		if routes != "" {
			routes += ","
		}
		routes += fmt.Sprintf("nats://127.0.0.1:%d", peer.clusterPort)
	}
	self.logPath = filepath.Join(t.TempDir(), self.name+".log")
	logFile, err := os.Create(self.logPath)
	if err != nil {
		t.Fatalf("create log: %v", err)
	}
	cmd := exec.Command(bin, "serve", "--die-with-parent")
	cmd.Env = append(os.Environ(),
		"DAGNATS_DATA_DIR="+self.dataDir,
		"DAGNATS_HTTP_ADDR="+self.httpAddr,
		fmt.Sprintf("DAGNATS_NATS_PORT=%d", self.natsPort),
		fmt.Sprintf("DAGNATS_NATS_CLUSTER_PORT=%d", self.clusterPort),
		"DAGNATS_NATS_CLUSTER_NAME=e2e-cluster",
		// Production clusters set a token; without one this test could
		// not see routes that never authenticate.
		"DAGNATS_NATS_CLUSTER_AUTH_TOKEN=e2e-route-token",
		"DAGNATS_NATS_CLUSTER_ROUTES="+routes,
		"DAGNATS_MAX_STORE_BYTES=536870912",
	)
	cmd.Stdout, cmd.Stderr = logFile, logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if err := cmd.Start(); err != nil {
		t.Fatalf("start %s: %v", self.name, err)
	}
	self.cmd = cmd
	self.started = time.Now()
	self.exited = make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(self.exited)
	}()
	n := &self
	t.Cleanup(func() { killNode(n) })
	// Registered after the kill so it runs first (cleanups are LIFO).
	t.Cleanup(func() { dumpLogOnFailure(t, n) })
	return n
}

// hasExited reports whether the node process is gone.
func hasExited(n *node) bool {
	select {
	case <-n.exited:
		return true
	default:
		return false
	}
}

// killNode SIGKILLs a node's process group and waits (bounded) for the
// reaper goroutine. Idempotent.
func killNode(n *node) {
	if n == nil || n.cmd == nil || n.cmd.Process == nil {
		return
	}
	_ = syscall.Kill(-n.cmd.Process.Pid, syscall.SIGKILL)
	select {
	case <-n.exited:
	case <-time.After(10 * time.Second):
	}
}

// dumpLogOnFailure prints a node log when the test failed, so a CI failure
// explains itself without the (deleted) temp dir.
func dumpLogOnFailure(t *testing.T, n *node) {
	if !t.Failed() || hasExited(n) {
		if t.Failed() {
			logTail(t, n)
		}
		return
	}
	// SIGQUIT makes the Go runtime print every goroutine's stack, which
	// shows where a node that never became ready is stuck.
	_ = syscall.Kill(n.cmd.Process.Pid, syscall.SIGQUIT)
	time.Sleep(time.Second)
	logTail(t, n)
}

func logTail(t *testing.T, n *node) {
	data, err := os.ReadFile(n.logPath)
	if err != nil {
		return
	}
	t.Logf("--- %s log ---\n%s", n.name, relevantLog(string(data)))
}

// relevantLog keeps the head of a node log plus only the goroutine stacks
// that touch dagnats code, so a SIGQUIT dump stays readable.
func relevantLog(log string) string {
	const maxBytes = 6000
	blocks := strings.Split(log, "\n\ngoroutine ")
	kept := []string{blocks[0]}
	for _, block := range blocks[1:] {
		if strings.Contains(block, "danmestas/dagnats") {
			kept = append(kept, "goroutine "+block)
		}
	}
	out := strings.Join(kept, "\n\n")
	if len(out) > maxBytes {
		out = out[len(out)-maxBytes:]
	}
	return out
}

func fetchHealth(addr string) (healthBody, int, error) {
	client := http.Client{Timeout: 10 * time.Second}
	resp, err := client.Get("http://" + addr + "/health/cluster")
	if err != nil {
		return healthBody{}, 0, err
	}
	defer resp.Body.Close()
	var body healthBody
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return healthBody{}, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

// waitHealth polls addr until pred accepts the report or timeout passes.
// check, when non-nil, runs every poll and aborts the wait with its error.
func waitHealth(
	addr string, timeout time.Duration,
	pred func(healthBody, int) bool, check func() error,
) (healthBody, error) {
	deadline := time.Now().Add(timeout)
	var last healthBody
	for time.Now().Before(deadline) {
		if check != nil {
			if err := check(); err != nil {
				return last, err
			}
		}
		body, status, err := fetchHealth(addr)
		if err == nil {
			last = body
			if pred(body, status) {
				return body, nil
			}
		}
		time.Sleep(healthPollPeriod)
	}
	return last, fmt.Errorf(
		"health on %s did not converge within %s; last=%+v",
		addr, timeout, last)
}

// launchNodes allocates fresh ports and starts all nodes.
func launchNodes(t *testing.T, bin string) []*node {
	t.Helper()
	specs := make([]node, nodeCount)
	for i := range specs {
		specs[i] = node{
			name:        fmt.Sprintf("n%d", i+1),
			natsPort:    freePort(t),
			httpAddr:    fmt.Sprintf("127.0.0.1:%d", freePort(t)),
			clusterPort: freePort(t),
			dataDir:     t.TempDir(),
		}
	}
	nodes := make([]*node, nodeCount)
	for i := range specs {
		nodes[i] = startNode(t, bin, specs[i], specs)
	}
	return nodes
}

// earlyExitCheck fails when any node died; the error wraps errEarlyExit if
// that happened inside earlyExitWindow of its start.
func earlyExitCheck(nodes []*node) func() error {
	return func() error {
		for _, n := range nodes {
			if !hasExited(n) {
				continue
			}
			if time.Since(n.started) < earlyExitWindow {
				return fmt.Errorf("%s: %w", n.name, errEarlyExit)
			}
			return fmt.Errorf("%s exited unexpectedly", n.name)
		}
		return nil
	}
}

// formCluster starts the nodes and waits until the first reports all
// three members with a meta leader, relaunching on fresh ports (bounded)
// if a node dies right after start.
func formCluster(t *testing.T, bin string) []*node {
	t.Helper()
	for attempt := 1; attempt <= startAttemptsMax; attempt++ {
		nodes := launchNodes(t, bin)
		formed, err := waitHealth(nodes[0].httpAddr, formTimeout,
			func(b healthBody, status int) bool {
				return status == http.StatusOK && b.NodesUp == nodeCount &&
					len(b.Members) == nodeCount && b.Leader != ""
			}, earlyExitCheck(nodes))
		if err == nil {
			if formed.Mode != "cluster" || formed.NodesExpected != nodeCount {
				t.Fatalf("unexpected formed report: %+v", formed)
			}
			return nodes
		}
		if errors.Is(err, errEarlyExit) && attempt < startAttemptsMax {
			t.Logf("attempt %d: %v; relaunching on new ports", attempt, err)
			for _, n := range nodes {
				killNode(n)
			}
			continue
		}
		t.Fatalf("cluster did not form: %v", err)
	}
	return nil
}

// runLinearWorkflow registers and runs a one-step workflow, retrying the
// register+start for up to retryWindow: right after a node dies the
// JetStream meta group re-elects and API calls fail transiently, which is
// exactly what a real client retries through. The run itself must then
// complete within runTimeout.
func runLinearWorkflow(
	t *testing.T, svc *api.Service, label string, retryWindow time.Duration,
) {
	t.Helper()
	wb := dag.NewWorkflow(harness.UniqueName(t, label))
	wb.Task("a", "cluster-task")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build workflow: %v", err)
	}
	var runID string
	deadline := time.Now().Add(retryWindow)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = svc.RegisterWorkflow(ctx, def)
		if err == nil {
			runID, err = svc.StartRun(ctx, def.Name, nil)
		}
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: start did not succeed within %s: %v",
				label, retryWindow, err)
		}
		time.Sleep(time.Second)
	}
	harness.WaitForRunStatus(
		t, svc, runID, dag.RunStatusCompleted, runTimeout,
	)
}

// runChainWorkflows starts chainRunCount concurrent runs of
// task -> sleep -> task -> task and requires every run's SNAPSHOT to
// reach completed with every step completed. One engine per node
// shares the history consumer, so consecutive events of one run land
// on different engines; a snapshot write that ignored a rival's newer
// write left runs wedged at running even though history recorded
// workflow.completed. A one-step run cannot show this: it has no
// second event to race the first.
func runChainWorkflows(t *testing.T, svc *api.Service) {
	t.Helper()
	wb := dag.NewWorkflow(harness.UniqueName(t, "chain"))
	first := wb.Task("a", "cluster-task")
	nap := wb.Sleep("nap", chainSleep).After(first)
	second := wb.Task("b", "cluster-task").After(nap)
	wb.Task("c", "cluster-task").After(second)
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("build chain workflow: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := svc.RegisterWorkflow(ctx, def); err != nil {
		t.Fatalf("register chain workflow: %v", err)
	}
	runIDs := make([]string, 0, chainRunCount)
	for i := 0; i < chainRunCount; i++ {
		runID, err := svc.StartRun(ctx, def.Name, nil)
		if err != nil {
			t.Fatalf("start chain run %d: %v", i, err)
		}
		runIDs = append(runIDs, runID)
	}
	waitChainSnapshots(t, svc, runIDs)
}

// waitChainSnapshots polls (bounded by chainTimeout) until every run's
// snapshot is completed with all four steps completed. On timeout it
// reports each unfinished run's step states, which is what tells a
// wedge (c completed, run still running) apart from a slow run.
func waitChainSnapshots(t *testing.T, svc *api.Service, runIDs []string) {
	t.Helper()
	pending := map[string]string{}
	for _, runID := range runIDs {
		pending[runID] = "not read yet"
	}
	deadline := time.Now().Add(chainTimeout)
	for len(pending) > 0 && time.Now().Before(deadline) {
		for runID := range pending {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			run, err := svc.GetRun(ctx, runID)
			cancel()
			if err != nil {
				pending[runID] = err.Error()
				continue
			}
			if chainSnapshotDone(run) {
				delete(pending, runID)
				continue
			}
			pending[runID] = describeChainRun(run)
		}
		time.Sleep(healthPollPeriod)
	}
	for runID, state := range pending {
		t.Errorf("run %s snapshot not completed within %s: %s",
			runID, chainTimeout, state)
	}
}

func chainSnapshotDone(run dag.WorkflowRun) bool {
	if run.Status != dag.RunStatusCompleted {
		return false
	}
	for _, stepID := range []string{"a", "nap", "b", "c"} {
		if run.Steps[stepID].Status != dag.StepStatusCompleted {
			return false
		}
	}
	return true
}

func describeChainRun(run dag.WorkflowRun) string {
	return fmt.Sprintf("run=%v a=%v nap=%v b=%v c=%v", run.Status,
		run.Steps["a"].Status, run.Steps["nap"].Status,
		run.Steps["b"].Status, run.Steps["c"].Status)
}

func TestMultiProcessCluster_FailoverKeepsWorking(t *testing.T) {
	if os.Getenv("DAGNATS_E2E_CLUSTER") != "1" {
		t.Skip("set DAGNATS_E2E_CLUSTER=1 to run the 3-process cluster test")
	}
	if testing.Short() {
		t.Skip("multi-process cluster test skipped in -short mode")
	}
	if dl, ok := t.Deadline(); ok && time.Until(dl) < totalBudget {
		t.Fatalf("go test -timeout leaves %s; this test needs %s "+
			"(use -timeout 900s)", time.Until(dl).Round(time.Second), totalBudget)
	}
	bin := buildBinary(t)
	nodes := formCluster(t, bin)

	urls := ""
	for _, n := range nodes {
		urls += fmt.Sprintf("nats://127.0.0.1:%d,", n.natsPort)
	}
	nc, err := nats.Connect(urls, nats.MaxReconnects(-1),
		nats.ReconnectWait(200*time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	harness.SubscribeWorker(t, nc, "cluster-task",
		func(tc worker.TaskContext) error {
			return tc.Complete([]byte(`"ok"`))
		})

	// Built before the kill: constructing a Service reads KV buckets, which
	// would race the post-kill re-election and is not what is under test.
	svc := harness.NewTestService(t, nc)
	runLinearWorkflow(t, svc, "before-kill", 10*time.Second)
	runChainWorkflows(t, svc)

	killNode(nodes[2])
	// 200, not just a lower count: every provisioned asset is replicated,
	// so two of three nodes keep quorum and the survivors must stay healthy
	// for a per-node load-balancer check.
	degraded, err := waitHealth(nodes[0].httpAddr, degradeTimeout,
		func(b healthBody, status int) bool {
			return status == http.StatusOK && b.NodesUp == nodeCount-1
		}, nil)
	if err != nil {
		t.Fatalf("after kill: %v", err)
	}
	if degraded.ConnectedPeers != 1 || degraded.ExpectedPeers != 2 ||
		!degraded.OK || len(degraded.JetStream.Unavailable) != 0 ||
		len(degraded.JetStream.Degraded) != 0 {
		t.Fatalf("after kill want ok, 1/2 peers, nothing unavailable: %+v", degraded)
	}
	// Every stream is replicated, so nothing may be merely "degraded"; and
	// the streams must actually be present in the report so an empty map
	// cannot make the checks above vacuous.
	for _, name := range []string{"TELEMETRY", "STICKY_TASKS", "TRIGGER_HISTORY"} {
		st, ok := degraded.JetStream.Streams[name]
		if !ok || st.Replicas != 3 {
			t.Errorf("stream %s = %+v (present %v), want replicas 3", name, st, ok)
		}
	}
	if degraded.Node == "" || degraded.Node == "unknown" {
		t.Errorf("report does not name the answering node: %+v", degraded)
	}

	runLinearWorkflow(t, svc, "after-kill", failoverTimeout)
}
