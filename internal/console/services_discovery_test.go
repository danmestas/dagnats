// services_discovery_test.go exercises the live $SRV micro-service
// discovery that backs the Version / Instances / Status columns on the
// /console/services page.
//
// Methodology:
//   - discoverMicroServices is an integration surface: it broadcasts
//     $SRV.PING / $SRV.STATS over a REAL embedded NATS server and folds
//     the replies. Tests stand up a fresh server per test via
//     natsutil.StartTestServer (no shared state) and register a REAL
//     micro.AddService responder — no mocks, per the repo's
//     integration-test convention. The responder's own PING/STATS
//     handlers answer the broadcast, so the assertions exercise the
//     exact wire path production uses.
//   - mergeDiscovery is pure (no NATS): its tests build rows + discovery
//     maps in-memory and assert the join, the synthesized discovery-only
//     rows, the honest Status mapping, and the all-dash nil-map contract.
//   - The timeout test asserts the deadline BOUND (not merely "returns"):
//     with no responders, discovery must drain to its window and come
//     back well under window+slack, proving the success-on-timeout path.
//   - Min 2 assertions per test (positive + negative space). Bounded
//     timeouts on every wait.
package console

import (
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/micro"
)

// TestDiscoverMicroServices_liveColumns asserts a real responder shows up
// in discovery with its Version, a single instance, and STATS present
// (the inputs to an "online" Status). A roster name with no responder is
// NOT in the discovery map (mergeDiscovery later marks it "stale").
func TestDiscoverMicroServices_liveColumns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	svc, err := micro.AddService(nc, micro.Config{
		Name:    "dagnats-test",
		Version: "1.2.3",
		Endpoint: &micro.EndpointConfig{
			Subject: "dagnats.test.ping",
			Handler: micro.HandlerFunc(func(r micro.Request) {
				_ = r.Respond(nil)
			}),
		},
	})
	if err != nil {
		t.Fatalf("AddService: %v", err)
	}
	t.Cleanup(func() { _ = svc.Stop() })

	got, err := discoverMicroServices(nc, discoverWindow)
	if err != nil {
		t.Fatalf("discoverMicroServices: %v", err)
	}
	d, ok := got["dagnats-test"]
	if !ok {
		t.Fatalf("dagnats-test missing from discovery: %+v", got)
	}
	if d.Version != "1.2.3" {
		t.Errorf("Version: got %q, want 1.2.3", d.Version)
	}
	if d.Instances != 1 {
		t.Errorf("Instances: got %d, want 1", d.Instances)
	}
	if !d.HadStats {
		t.Errorf("HadStats: got false, want true (responder has STATS)")
	}
	// Negative space: a name nobody registered never appears.
	if _, leaked := got["never-registered"]; leaked {
		t.Errorf("discovery fabricated an unregistered service")
	}
}

// TestDiscoverMicroServices_twoInstances asserts two responders sharing a
// Name fold into Instances == 2.
func TestDiscoverMicroServices_twoInstances(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	for i := 0; i < 2; i++ {
		svc, err := micro.AddService(nc, micro.Config{
			Name:    "twin",
			Version: "0.1.0",
			Endpoint: &micro.EndpointConfig{
				Subject: "twin.ping",
				Handler: micro.HandlerFunc(func(r micro.Request) {
					_ = r.Respond(nil)
				}),
			},
		})
		if err != nil {
			t.Fatalf("AddService #%d: %v", i, err)
		}
		t.Cleanup(func() { _ = svc.Stop() })
	}

	got, err := discoverMicroServices(nc, discoverWindow)
	if err != nil {
		t.Fatalf("discoverMicroServices: %v", err)
	}
	d, ok := got["twin"]
	if !ok {
		t.Fatalf("twin missing from discovery")
	}
	if d.Instances != 2 {
		t.Errorf("Instances: got %d, want 2", d.Instances)
	}
	// Second behavioral assertion: both responders carry STATS, folded.
	if !d.HadStats {
		t.Errorf("HadStats: got false, want true")
	}
	// Negative space: no third name is fabricated.
	if _, leaked := got["triplet"]; leaked {
		t.Errorf("discovery fabricated a third instance name")
	}
}

// TestDiscoverMicroServices_timeoutDegrades asserts the success-on-timeout
// contract: connected NATS with NO responders returns (empty, nil) and
// completes within the discovery window plus a small slack — the drain
// deadline is the bound, not an open-ended wait.
func TestDiscoverMicroServices_timeoutDegrades(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)

	start := time.Now()
	got, err := discoverMicroServices(nc, discoverWindow)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("no-responder discovery must not error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("discovery: got %d entries, want 0", len(got))
	}
	// Both verbs share ONE deadline, so total wait is bounded by a single
	// window plus slack — not 2x. This proves the single-deadline budget.
	bound := discoverWindow + 200*time.Millisecond
	if elapsed >= bound {
		t.Errorf("discovery took %v, want < %v (single-window bound)", elapsed, bound)
	}
}
