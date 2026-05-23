// testhelpers_test.go
// Shared test-only constructors for the bridge package.
// Methodology: keep NewBridge call sites in tests one-liners by
// wrapping the TracingPublisher construction (and its jetstream
// init) behind a single helper. Production code MUST go through
// NewBridge directly with a fully-wired *natsutil.TracingPublisher.
package bridge

import (
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// newTestBridge constructs a Bridge with a real TracingPublisher
// wrapping the test's nc + a freshly-derived jetstream handle.
// Fails the test if jetstream.New errors.
func newTestBridge(t *testing.T, nc *nats.Conn) *Bridge {
	t.Helper()
	if nc == nil {
		t.Fatalf("newTestBridge: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("newTestBridge: jetstream.New: %v", err)
	}
	pub := natsutil.NewTracingPublisher(nc, js)
	return NewBridge(pub)
}
