package natsutil

import (
	"testing"

	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

// StartTestServer starts an embedded NATS server with JetStream enabled
// and returns both the server and a connected client. The server and connection
// are automatically shut down via t.Cleanup when the test ends. Accepts
// testing.TB so the same helper works for *testing.T and *testing.B.
func StartTestServer(t testing.TB) (*natsserver.Server, *nats.Conn) {
	t.Helper()
	opts := &natsserver.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	}
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		t.Fatalf("failed to create test NATS server: %v", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5_000_000_000) {
		t.Fatal("NATS server not ready after 5s")
	}
	t.Cleanup(func() {
		ns.Shutdown()
		ns.WaitForShutdown()
	})
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("failed to connect to test NATS server: %v", err)
	}
	t.Cleanup(func() { nc.Close() })
	return ns, nc
}
