// journal/cluster_test.go
// In-process three-node JetStream cluster for the conformance suite.
// dagnatstest has one, but it imports this package (for the mock task
// context), so the suite carries its own: three routed servers, a wait
// for quorum through the production helper, and a warm-up R3 stream so
// placement never fails on the first real create.
package journal

import (
	"context"
	"fmt"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const clusterNodes = 3

// clusterStoreBudget is each node's JetStream store budget, and the
// budget SetupAll sizes its ceilings against. Pinned on both sides so
// placement is hermetic: AccountInfo on a forming cluster can report
// an unlimited store, which would size R3 ceilings off the 10 GiB
// default and fail placement on a smaller node budget.
const clusterStoreBudget = int64(12 << 30)

// clusterListenWait bounds how long a started node may take to bind its
// client and route listeners. Binding takes milliseconds; the bound is
// for a loaded runner, and a node whose bind failed reports it through
// its logger at once instead of using up the wait.
const clusterListenWait = 10 * time.Second

// clusterStartAttemptsMax bounds the starts on fresh ports when another
// process takes a reserved route port before its node binds it.
const clusterStartAttemptsMax = 3

// startCluster starts the nodes and returns a connection to the first.
func startCluster(t *testing.T) *nats.Conn {
	return startClusterWith(t, reservePort)
}

// startClusterWith is startCluster with the port reservation injected,
// so a test can hand it a port that is already taken.
func startClusterWith(t *testing.T, reserve func(*testing.T) int) *nats.Conn {
	t.Helper()
	if reserve == nil {
		panic("startClusterWith: reserve must not be nil")
	}
	var servers []*natsserver.Server
	var err error
	for attempt := 1; attempt <= clusterStartAttemptsMax; attempt++ {
		servers, err = startClusterNodes(t, reserve)
		if err == nil {
			break
		}
		t.Logf("cluster start attempt %d: %v", attempt, err)
	}
	if err != nil {
		t.Fatalf("cluster never started in %d attempts: %v", clusterStartAttemptsMax, err)
	}
	return connectCluster(t, servers)
}

// startClusterNodes starts every node on freshly reserved route ports
// and waits for each to bind. On failure it shuts the nodes it started
// down, so the caller can retry on new ports.
func startClusterNodes(
	t *testing.T, reserve func(*testing.T) int,
) ([]*natsserver.Server, error) {
	t.Helper()
	ports := make([]int, clusterNodes)
	for i := range ports {
		ports[i] = reserve(t)
	}
	servers := make([]*natsserver.Server, 0, clusterNodes)
	fatals := make([]*fatalLog, 0, clusterNodes)
	for i := range ports {
		server, fatal := startClusterNode(t, i, ports)
		servers = append(servers, server)
		fatals = append(fatals, fatal)
	}
	for i, server := range servers {
		if err := waitListening(server, fatals[i]); err != nil {
			for _, started := range servers {
				started.Shutdown()
			}
			return nil, err
		}
	}
	return servers, nil
}

// waitListening returns once the node has bound its client and route
// listeners, or with the failure it logged, within clusterListenWait.
func waitListening(server *natsserver.Server, fatal *fatalLog) error {
	if server == nil || fatal == nil {
		panic("waitListening: server and fatal must not be nil")
	}
	deadline := time.Now().Add(clusterListenWait)
	for time.Now().Before(deadline) {
		select {
		case message := <-fatal.messages:
			return fmt.Errorf("server %s: %s", server.Name(), message)
		default:
		}
		if server.Addr() != nil && server.ClusterAddr() != nil {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("server %s did not bind its listeners within %s",
		server.Name(), clusterListenWait)
}

// connectCluster connects to the first node and waits for quorum and
// for R3 placement.
func connectCluster(t *testing.T, servers []*natsserver.Server) *nats.Conn {
	t.Helper()
	if len(servers) != clusterNodes {
		panic("connectCluster: want every node")
	}
	nc, err := nats.Connect(servers[0].ClientURL(), nats.DontRandomize())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if _, err := natsutil.WaitForClusterQuorum(ctx, js, clusterNodes); err != nil {
		t.Fatalf("cluster quorum: %v", err)
	}
	warmUpPlacement(t, js)
	return nc
}

// fatalLog is a node's logger. Logging stays off except for Fatalf,
// which nats-server calls when it cannot bind a listener and then
// carries on half started; with no logger the failure is invisible.
type fatalLog struct{ messages chan string }

func (l *fatalLog) Fatalf(format string, v ...any) {
	select {
	case l.messages <- fmt.Sprintf(format, v...):
	default: // the first failure is the one that matters
	}
}

func (*fatalLog) Noticef(string, ...any) {}
func (*fatalLog) Warnf(string, ...any)   {}
func (*fatalLog) Errorf(string, ...any)  {}
func (*fatalLog) Debugf(string, ...any)  {}
func (*fatalLog) Tracef(string, ...any)  {}

func startClusterNode(
	t *testing.T, index int, clusterPorts []int,
) (*natsserver.Server, *fatalLog) {
	t.Helper()
	if index < 0 || index >= len(clusterPorts) {
		panic("startClusterNode: index out of range")
	}
	var routes []*url.URL
	for peer, port := range clusterPorts {
		if peer != index {
			routes = append(routes, &url.URL{
				Scheme: "nats", Host: fmt.Sprintf("127.0.0.1:%d", port),
			})
		}
	}
	server, err := natsserver.NewServer(&natsserver.Options{
		Host: "127.0.0.1", Port: -1,
		ServerName:        fmt.Sprintf("journal-%d", index),
		JetStream:         true,
		JetStreamMaxStore: clusterStoreBudget,
		StoreDir:          t.TempDir(),
		Cluster: natsserver.ClusterOpts{
			Name: "journal", Host: "127.0.0.1", Port: clusterPorts[index],
		},
		Routes: routes,
		NoLog:  true,
		NoSigs: true,
	})
	if err != nil {
		t.Fatalf("new server %d: %v", index, err)
	}
	fatal := &fatalLog{messages: make(chan string, 1)}
	server.SetLogger(fatal, false, false)
	server.Start()
	t.Cleanup(server.Shutdown)
	return server, fatal
}

// warmUpPlacement creates a throwaway R3 stream until the meta leader
// can place one, bounded.
func warmUpPlacement(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	cfg := jetstream.StreamConfig{Name: "WARMUP", Replicas: clusterNodes}
	var err error
	for attempt := 0; attempt < 50; attempt++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, err = js.CreateOrUpdateStream(ctx, cfg)
		cancel()
		if err == nil {
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("R3 placement never succeeded: %v", err)
}

func reservePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve port: %v", err)
	}
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}
