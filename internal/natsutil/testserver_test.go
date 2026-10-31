// natsutil/testserver_test.go
// Test-only aliases for the embedded server in internal/natstest, so the
// package's own tests keep their short names while natsutil itself no
// longer links nats-server.
// Methodology: none; helpers only.
package natsutil

import (
	"testing"

	"github.com/danmestas/dagnats/internal/natstest"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

const (
	testServerMaxStoreBytes         = natstest.MaxStoreBytes
	testServerMaxStoreHeadroomBytes = natstest.MaxStoreHeadroomBytes
)

func StartTestServer(t testing.TB) (*natsserver.Server, *nats.Conn) {
	t.Helper()
	return natstest.StartServer(t)
}
