// setup_bind_test.go
// Pins where startNATS listens. Methodology: start the helper's server,
// then try to bind 127.0.0.1 on its port the way another test process's
// "pick a free port, close it, bind it again" helper would. A server on the
// wildcard address leaves that bind free on macOS, and clients dialing the
// server then reach the other listener and hang for the full connect and
// stream timeouts (the 45s TestCollectLogs_ReceivesPublishedJSON failure).
// A server on 127.0.0.1 owns the port, so the second bind must fail.
package observe

import (
	"fmt"
	"net"
	"strings"
	"testing"
)

func TestStartNATSOwnsItsLoopbackPort(t *testing.T) {
	ns, _ := startNATS(t)
	addr, ok := ns.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("server address %v is not TCP", ns.Addr())
	}
	if !addr.IP.IsLoopback() {
		t.Fatalf("server listens on %v, want a loopback address", addr.IP)
	}
	thief, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", addr.Port))
	if err == nil {
		thief.Close()
		t.Fatalf("another listener bound 127.0.0.1:%d beside the server", addr.Port)
	}
	if !strings.Contains(err.Error(), "address already in use") {
		t.Fatalf("second bind failed for an unexpected reason: %v", err)
	}
}
