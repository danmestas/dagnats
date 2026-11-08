// port_race_test.go: pins the route-port race the cluster test harnesses
// had. freeTCPPorts releases a port before the server binds it, so a
// foreign listener can win it; the server then reports only "not ready
// after 5s" because nats-server runs without a log. The first test shows a
// taken route port produces exactly that error, the second that
// startWithFreshPorts recovers by picking again. Both are deterministic:
// the collision is made by the test, not left to chance.
package server

import (
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
)

func TestStartNATS_TakenRoutePortReportsNotReady(t *testing.T) {
	ports := freeTCPPorts(t, testRegionNodes)
	taken, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[2]))
	if err != nil {
		t.Fatalf("occupy route port: %v", err)
	}
	defer func() {
		if closeErr := taken.Close(); closeErr != nil {
			t.Errorf("close: %v", closeErr)
		}
	}()

	cfg := regionNodeConfig(t, "eu", 2, ports, regionOptions{})
	ns, err := startNATS(cfg)
	if err == nil {
		ns.Shutdown()
		t.Fatal("startNATS succeeded on a route port another listener holds")
	}
	if !strings.Contains(err.Error(), "not ready after") {
		t.Fatalf("err = %v, want the not-ready timeout", err)
	}
}

func TestStartWithFreshPorts_RetriesAfterFailure(t *testing.T) {
	attempts := 0
	seen := map[int]bool{}
	startWithFreshPorts(t, 3, func(ports []int) error {
		attempts++
		if len(ports) != 3 {
			t.Fatalf("ports = %v, want 3", ports)
		}
		for _, port := range ports {
			seen[port] = true
		}
		if attempts < 3 {
			return errors.New("port taken")
		}
		return nil
	})
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
	if len(seen) < 4 {
		t.Fatalf("only %d distinct ports over 3 picks; retries must re-pick", len(seen))
	}
}
