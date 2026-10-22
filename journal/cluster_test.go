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

// startCluster starts the nodes and returns a connection to the first.
func startCluster(t *testing.T) *nats.Conn {
	t.Helper()
	ports := make([]int, clusterNodes)
	for i := range ports {
		ports[i] = reservePort(t)
	}
	servers := make([]*natsserver.Server, clusterNodes)
	for i := range servers {
		servers[i] = startClusterNode(t, i, ports)
	}
	for _, server := range servers {
		if !server.ReadyForConnections(10 * time.Second) {
			t.Fatalf("server %s not ready", server.Name())
		}
	}
	urls := make([]string, clusterNodes)
	for i, server := range servers {
		urls[i] = server.ClientURL()
	}
	nc, err := nats.Connect(urls[0], nats.DontRandomize())
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

func startClusterNode(t *testing.T, index int, clusterPorts []int) *natsserver.Server {
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
	server.Start()
	t.Cleanup(server.Shutdown)
	return server
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
