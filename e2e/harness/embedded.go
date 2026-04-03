// e2e/harness/embedded.go
// Embedded topology provider. Starts a single in-process NATS server
// with JetStream per test. Fastest topology — suitable for rapid CI.
package harness

import (
	"testing"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/nats-io/nats.go"
)

// EmbeddedTopology starts a single in-process NATS server per test.
type EmbeddedTopology struct{}

// NewEmbedded creates an embedded topology provider.
func NewEmbedded() *EmbeddedTopology {
	return &EmbeddedTopology{}
}

// Name returns the topology identifier.
func (e *EmbeddedTopology) Name() string { return "embedded" }

// Connect starts a NATS server and returns a connected client.
func (e *EmbeddedTopology) Connect(t *testing.T) *nats.Conn {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	return nc
}

// Setup provisions streams and KV buckets on the connection.
func (e *EmbeddedTopology) Setup(t *testing.T, nc *nats.Conn) {
	t.Helper()
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("embedded Setup: %v", err)
	}
}
