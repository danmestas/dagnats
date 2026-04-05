// e2e/harness/local_cluster.go
// Local cluster topology provider. Starts a single NATS server with
// production-like configuration (explicit JetStream limits, store dir).
// More realistic than the minimal embedded test server.
package harness

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// LocalClusterTopology starts a production-like single NATS server.
type LocalClusterTopology struct{}

// NewLocalCluster creates a local cluster topology provider.
func NewLocalCluster() *LocalClusterTopology {
	return &LocalClusterTopology{}
}

// Name returns the topology identifier.
func (l *LocalClusterTopology) Name() string {
	return "local_cluster"
}

// Connect starts a NATS server with explicit limits and returns a client.
func (l *LocalClusterTopology) Connect(t *testing.T) *nats.Conn {
	t.Helper()
	opts := &natsserver.Options{
		Host:               "127.0.0.1",
		Port:               -1,
		JetStream:          true,
		StoreDir:           t.TempDir(),
		JetStreamMaxMemory: 256 * 1024 * 1024,
		JetStreamMaxStore:  2 * 1024 * 1024 * 1024,
		MaxPayload:         8 * 1024 * 1024,
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("local_cluster: create server: %v", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("local_cluster: server not ready after 5s")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("local_cluster: connect: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	return nc
}

// Setup provisions streams and KV buckets on the connection.
func (l *LocalClusterTopology) Setup(t *testing.T, nc *nats.Conn) {
	t.Helper()
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("local_cluster Setup: %v", err)
	}
}
