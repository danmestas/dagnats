// e2e/harness/supercluster.go
// Supercluster topology provider. Starts 5 in-process NATS servers:
// Cluster A (2 nodes) + Cluster B (2 nodes) + 1 leaf node connecting
// to Cluster A. Gateways link the two clusters. Tests connect to a
// Cluster A node for JetStream access; the leaf exercises routing.
package harness

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// SuperclusterTopology manages 5 NATS servers across 2 clusters
// plus a leaf node. Implements both Topology and Resilient.
type SuperclusterTopology struct {
	clusterA  [2]*natsserver.Server
	clusterB  [2]*natsserver.Server
	leaf      *natsserver.Server
	confPaths map[string]string
	started   bool
}

// NewSupercluster creates a supercluster topology provider.
func NewSupercluster() *SuperclusterTopology {
	return &SuperclusterTopology{
		confPaths: make(map[string]string),
	}
}

// Name returns the topology identifier.
func (s *SuperclusterTopology) Name() string {
	return "supercluster"
}

// Connect starts all 5 servers and returns a client connected
// to Cluster A. JetStream requires a direct cluster connection;
// the leaf is available via resilience methods.
func (s *SuperclusterTopology) Connect(t *testing.T) *nats.Conn {
	t.Helper()
	s.startAll(t)
	nc, err := nats.Connect(
		s.clusterA[0].ClientURL(),
		nats.MaxReconnects(10),
		nats.ReconnectWait(500*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("supercluster: connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

// Setup provisions streams and KV buckets on the connection.
func (s *SuperclusterTopology) Setup(
	t *testing.T, nc *nats.Conn,
) {
	t.Helper()
	if err := natsutil.SetupAll(
		nc, natsutil.WithStoreBudget(clusterStoreBudgetBytes),
	); err != nil {
		t.Fatalf("supercluster Setup: %v", err)
	}
}

// KillNode shuts down the named server (a0, a1, b0, b1).
func (s *SuperclusterTopology) KillNode(name string) error {
	srv := s.serverByName(name)
	if srv == nil {
		return fmt.Errorf("KillNode: unknown server %q", name)
	}
	srv.Shutdown()
	srv.WaitForShutdown()
	return nil
}

// RestartNode creates a new server from the stored config file.
func (s *SuperclusterTopology) RestartNode(name string) error {
	confPath, ok := s.confPaths[name]
	if !ok {
		return fmt.Errorf("RestartNode: unknown server %q", name)
	}
	opts, err := natsserver.ProcessConfigFile(confPath)
	if err != nil {
		return fmt.Errorf("RestartNode %s: config: %w", name, err)
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		return fmt.Errorf("RestartNode %s: server: %w", name, err)
	}
	srv.Start()
	if !srv.ReadyForConnections(20 * time.Second) {
		srv.Shutdown()
		return fmt.Errorf("RestartNode %s: not ready", name)
	}
	s.setServer(name, srv)
	return nil
}

// DisconnectLeaf shuts down the leaf node.
func (s *SuperclusterTopology) DisconnectLeaf() error {
	if s.leaf == nil {
		return fmt.Errorf("DisconnectLeaf: leaf not started")
	}
	s.leaf.Shutdown()
	s.leaf.WaitForShutdown()
	return nil
}

// ReconnectLeaf creates a new leaf from stored config.
func (s *SuperclusterTopology) ReconnectLeaf() error {
	confPath, ok := s.confPaths["leaf"]
	if !ok {
		return fmt.Errorf("ReconnectLeaf: config not stored")
	}
	opts, err := natsserver.ProcessConfigFile(confPath)
	if err != nil {
		return fmt.Errorf("ReconnectLeaf: config: %w", err)
	}
	srv, err := natsserver.NewServer(opts)
	if err != nil {
		return fmt.Errorf("ReconnectLeaf: server: %w", err)
	}
	srv.Start()
	if !srv.ReadyForConnections(20 * time.Second) {
		srv.Shutdown()
		return fmt.Errorf("ReconnectLeaf: not ready")
	}
	s.leaf = srv
	return nil
}

// startAll allocates ports and starts all 5 servers.
func (s *SuperclusterTopology) startAll(t *testing.T) {
	t.Helper()
	if s.started {
		return
	}
	natsserver.SetGatewaysSolicitDelay(
		10 * time.Millisecond,
	)
	t.Cleanup(natsserver.ResetGatewaysSolicitDelay)

	ports := allocatePorts(t, 15)
	layout := parsePorts(ports)

	s.startClusterA(t, layout)
	s.startClusterB(t, layout)
	s.startLeaf(t, layout)

	waitForCluster(t, s.clusterA[:], 20*time.Second)
	waitForCluster(t, s.clusterB[:], 20*time.Second)
	waitForGateways(t, s.clusterB[0], 1, 20*time.Second)
	waitForLeafNode(t, s.clusterA[0], 20*time.Second)
	allServers := append(s.clusterA[:], s.clusterB[:]...)
	waitForJetStreamLeader(t, allServers, 20*time.Second)

	s.started = true
	t.Cleanup(func() { s.shutdownAll() })
}

// portLayout holds all allocated port assignments.
type portLayout struct {
	aClient  [2]int
	aCluster [2]int
	aGateway [2]int
	aLeaf    [2]int
	bClient  [2]int
	bCluster [2]int
	bGateway [2]int
	leafPort int
}

// parsePorts maps 15 allocated ports into named assignments.
func parsePorts(ports []int) portLayout {
	return portLayout{
		aClient:  [2]int{ports[0], ports[1]},
		aCluster: [2]int{ports[2], ports[3]},
		aGateway: [2]int{ports[4], ports[5]},
		aLeaf:    [2]int{ports[6], ports[7]},
		bClient:  [2]int{ports[8], ports[9]},
		bCluster: [2]int{ports[10], ports[11]},
		bGateway: [2]int{ports[12], ports[13]},
		leafPort: ports[14],
	}
}

// startClusterA creates Cluster A nodes from config files.
// Cluster A has gateways (no remote targets) and leaf listeners.
// Gateways are one-sided: only B solicits A to avoid blocking
// JetStream meta-group formation while waiting for B.
func (s *SuperclusterTopology) startClusterA(
	t *testing.T, p portLayout,
) {
	t.Helper()
	for i := range 2 {
		name := fmt.Sprintf("a%d", i)
		dir := t.TempDir()
		conf := fmt.Sprintf(clusterATemplate,
			p.aClient[i], name,
			filepath.ToSlash(dir),
			p.aLeaf[i],
			p.aCluster[i],
			p.aCluster[0], p.aCluster[1],
			p.aGateway[i],
		)
		confPath := writeConfFile(t, name, conf)
		s.confPaths[name] = confPath
		srv := startServerFromConf(t, confPath)
		s.clusterA[i] = srv
	}
}

// startClusterB creates Cluster B nodes from config files.
// Cluster B solicits gateway connections to Cluster A.
func (s *SuperclusterTopology) startClusterB(
	t *testing.T, p portLayout,
) {
	t.Helper()
	for i := range 2 {
		name := fmt.Sprintf("b%d", i)
		dir := t.TempDir()
		conf := fmt.Sprintf(clusterBTemplate,
			p.bClient[i], name,
			filepath.ToSlash(dir),
			p.bCluster[i],
			p.bCluster[0], p.bCluster[1],
			p.bGateway[i],
			p.aGateway[0], p.aGateway[1],
		)
		confPath := writeConfFile(t, name, conf)
		s.confPaths[name] = confPath
		srv := startServerFromConf(t, confPath)
		s.clusterB[i] = srv
	}
}

// startLeaf creates the leaf node connecting to Cluster A.
func (s *SuperclusterTopology) startLeaf(
	t *testing.T, p portLayout,
) {
	t.Helper()
	conf := fmt.Sprintf(leafTemplate,
		p.leafPort, p.aLeaf[0],
	)
	confPath := writeConfFile(t, "leaf", conf)
	s.confPaths["leaf"] = confPath
	srv := startServerFromConf(t, confPath)
	s.leaf = srv
}

// Config templates. NATS requires a system_account when both
// leaf listeners and gateways are defined on the same server.
// The $SYS account is defined inline; the default $G account
// handles all client and JetStream traffic.
const clusterATemplate = `
listen: "127.0.0.1:%d"
server_name: %s
jetstream {
  max_mem_store: 256MB
  max_file_store: 2GB
  store_dir: '%s'
}
leaf { listen: "127.0.0.1:%d" }
cluster {
  name: cluster-a
  listen: "127.0.0.1:%d"
  routes = ["nats://127.0.0.1:%d", "nats://127.0.0.1:%d"]
}
accounts { $SYS { users = [{user: admin, pass: pass}] } }
gateway {
  name: cluster-a
  listen: "127.0.0.1:%d"
}
system_account: "$SYS"
`

const clusterBTemplate = `
listen: "127.0.0.1:%d"
server_name: %s
jetstream {
  max_mem_store: 256MB
  max_file_store: 2GB
  store_dir: '%s'
}
cluster {
  name: cluster-b
  listen: "127.0.0.1:%d"
  routes = ["nats://127.0.0.1:%d", "nats://127.0.0.1:%d"]
}
accounts { $SYS { users = [{user: admin, pass: pass}] } }
gateway {
  name: cluster-b
  listen: "127.0.0.1:%d"
  gateways = [
    {name: "cluster-a", urls: [
      "nats://127.0.0.1:%d",
      "nats://127.0.0.1:%d"
    ]}
  ]
}
system_account: "$SYS"
`

const leafTemplate = `
listen: "127.0.0.1:%d"
server_name: leaf
leaf {
  remotes [{ urls: ["nats://127.0.0.1:%d"] }]
}
`

// shutdownAll stops all running servers in reverse start order.
func (s *SuperclusterTopology) shutdownAll() {
	servers := []*natsserver.Server{
		s.leaf,
		s.clusterB[1], s.clusterB[0],
		s.clusterA[1], s.clusterA[0],
	}
	for _, srv := range servers {
		if srv != nil {
			srv.Shutdown()
			srv.WaitForShutdown()
		}
	}
}

// serverByName returns the server for the given name.
func (s *SuperclusterTopology) serverByName(
	name string,
) *natsserver.Server {
	switch name {
	case "a0":
		return s.clusterA[0]
	case "a1":
		return s.clusterA[1]
	case "b0":
		return s.clusterB[0]
	case "b1":
		return s.clusterB[1]
	case "leaf":
		return s.leaf
	default:
		return nil
	}
}

// setServer stores the server under the given name.
func (s *SuperclusterTopology) setServer(
	name string, srv *natsserver.Server,
) {
	switch name {
	case "a0":
		s.clusterA[0] = srv
	case "a1":
		s.clusterA[1] = srv
	case "b0":
		s.clusterB[0] = srv
	case "b1":
		s.clusterB[1] = srv
	case "leaf":
		s.leaf = srv
	}
}

// writeConfFile writes a NATS config to a temp file and returns
// its path. The file is cleaned up by t.Cleanup.
func writeConfFile(
	t *testing.T, name, content string,
) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), name+"-*.conf")
	if err != nil {
		t.Fatalf("writeConfFile %s: %v", name, err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatalf("writeConfFile %s: write: %v", name, err)
	}
	f.Close()
	return f.Name()
}

// startServerFromConf parses a config file and starts the server.
// Retries up to 5 times with increasing backoff on startup failure
// (port races and slow CI runners).
func startServerFromConf(
	t *testing.T, confPath string,
) *natsserver.Server {
	t.Helper()
	opts, err := natsserver.ProcessConfigFile(confPath)
	if err != nil {
		t.Fatalf("config %s: %v", confPath, err)
	}
	// maxRetries is generous enough for slow CI runners. The previous
	// budget of 5 (locally fine, ~always succeeds on attempt 1) was
	// too tight on resource-constrained CI VMs and surfaced as
	// "server X: not ready after 5 attempts" flakes on PRs touching
	// unrelated code (PRs #146, #159, others). Cumulative wait at 15
	// retries is bounded: sum_{i=1}^{15} i*500ms = 60s; with
	// readyTimeout per attempt at 30s the total worst case is
	// ~75s + 30*15 = ~7.5min, which is still well inside the package
	// `go test` budget of 600s.
	const maxRetries = 15
	const readyTimeout = 30 * time.Second
	for attempt := 1; attempt <= maxRetries; attempt++ {
		srv, srvErr := natsserver.NewServer(opts)
		if srvErr != nil {
			if attempt == maxRetries {
				t.Fatalf("server %s: %v", opts.ServerName, srvErr)
			}
			backoff := time.Duration(attempt) * 200 * time.Millisecond
			time.Sleep(backoff)
			continue
		}
		srv.Start()
		if srv.ReadyForConnections(readyTimeout) {
			return srv
		}
		srv.Shutdown()
		if attempt == maxRetries {
			t.Fatalf("server %s: not ready after %d attempts",
				opts.ServerName, maxRetries)
		}
		backoff := time.Duration(attempt) * 500 * time.Millisecond
		time.Sleep(backoff)
	}
	t.Fatal("startServerFromConf: unreachable")
	return nil
}

// allocatePorts finds count available TCP ports by binding to :0.
func allocatePorts(t *testing.T, count int) []int {
	t.Helper()
	ports := make([]int, count)
	for i := range ports {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("allocate port %d: %v", i, err)
		}
		ports[i] = listener.Addr().(*net.TCPAddr).Port
		listener.Close()
	}
	return ports
}

// waitForCluster polls until each server has at least 1 route.
func waitForCluster(
	t *testing.T,
	servers []*natsserver.Server,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allReady := true
		for _, srv := range servers {
			if srv.NumRoutes() < 1 {
				allReady = false
				break
			}
		}
		if allReady {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("waitForCluster: timed out")
}

// waitForGateways polls NumOutboundGateways until count is met.
func waitForGateways(
	t *testing.T,
	srv *natsserver.Server,
	count int,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if srv.NumOutboundGateways() >= count {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf(
		"waitForGateways: got %d want %d",
		srv.NumOutboundGateways(), count,
	)
}

// waitForLeafNode polls until at least 1 leaf connection exists.
func waitForLeafNode(
	t *testing.T,
	srv *natsserver.Server,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if srv.NumLeafNodes() > 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("waitForLeafNode: timed out")
}

// waitForJetStreamLeader polls until one server is the JS meta leader.
func waitForJetStreamLeader(
	t *testing.T,
	servers []*natsserver.Server,
	timeout time.Duration,
) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, srv := range servers {
			if srv.JetStreamIsLeader() {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("waitForJetStreamLeader: timed out")
}
