// Package dagnatstest provides test helpers for DagNats workflows.
// It starts an embedded NATS server with all required streams and KV
// buckets in a single call, ready for workflow testing.
//
// Usage:
//
//	func TestMyWorkflow(t *testing.T) {
//	    nc := dagnatstest.Server(t)
//	    // nc is ready — register workflows, start workers, etc.
//	}
package dagnatstest

import (
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
)

// Server starts an embedded NATS server with JetStream and all
// required streams/KV buckets provisioned. Returns the connected
// client. Server and connection are cleaned up automatically when
// the test ends.
func Server(t *testing.T) *nats.Conn {
	t.Helper()
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("dagnatstest.Server: SetupAll failed: %v", err)
	}
	return nc
}
