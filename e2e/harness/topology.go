// e2e/harness/topology.go
// Defines the Topology interface for E2E test infrastructure providers.
// Each provider manages NATS server lifecycle and provides connections.
package harness

import (
	"testing"

	"github.com/nats-io/nats.go"
)

// Topology provides a NATS connection for E2E tests. Simple topologies
// (embedded, local cluster) only need Connect. The supercluster
// topology also supports infrastructure manipulation for resilience tests.
type Topology interface {
	Name() string
	Connect(t *testing.T) *nats.Conn
	Setup(t *testing.T, nc *nats.Conn)
}

// Resilient extends Topology with infrastructure manipulation methods
// for resilience testing. Only the supercluster topology implements this.
type Resilient interface {
	Topology
	KillNode(name string) error
	RestartNode(name string) error
	DisconnectLeaf() error
	ReconnectLeaf() error
}
