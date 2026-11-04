// server/leaders_test.go
// Tests for wiring the preferred-leader rebalancer into serve.
// Methodology: a real single-node server per case; the rebalancer's
// cluster behaviour is covered in dagnatstest. Checks that it runs only
// when preferred_leaders is set and that shutdown stops it.
package server

import (
	"testing"
	"time"
)

func TestServer_LeaderRebalancerOnlyWithPreferredLeaders(t *testing.T) {
	off, _ := startProgramsServer(t, testConfig(t))
	// Negative: no preference, no rebalancer.
	if off.leaderRebalanceStop != nil {
		t.Fatal("rebalancer started with no preferred_leaders")
	}
	cfg := testConfig(t)
	cfg.PreferredLeaders = []string{"eu-1", "eu-2"}
	srv := New(cfg)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()
	waitHTTPAddr(t, srv)
	// Positive: set, it runs; and shutdown still completes.
	if srv.leaderRebalanceStop == nil {
		t.Fatal("rebalancer not started with preferred_leaders set")
	}
	srv.Stop()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
}
