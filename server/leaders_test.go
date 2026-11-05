// server/leaders_test.go
// Tests for wiring the meta reconcilers (preferred-leader rebalancer,
// peer retirer) into serve.
// Methodology: a real single-node server per case; the rebalancer's
// cluster behaviour is covered in dagnatstest. Checks that it runs only
// when preferred_leaders or retire_peers is set and that shutdown stops
// it.
package server

import (
	"testing"
	"time"
)

func TestServer_LeaderRebalancerOnlyWithPreferredLeaders(t *testing.T) {
	off, _ := startProgramsServer(t, testConfig(t))
	// Negative: no preference, no rebalancer.
	if off.metaReconcileStop != nil {
		t.Fatal("rebalancer started with no preferred_leaders")
	}
	cfg := testConfig(t)
	cfg.PreferredLeaders = []string{"eu-1", "eu-2"}
	srv := New(cfg)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()
	waitHTTPAddr(t, srv)
	// Positive: set, it runs; and shutdown still completes.
	if srv.metaReconcileStop == nil {
		t.Fatal("rebalancer not started with preferred_leaders set")
	}
	// The meta leader is moved through the in-process system user.
	system := srv.dialSystem()
	if system == nil {
		t.Fatal("the system user did not connect in process")
	}
	system.Close()
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

func TestServer_MetaReconcilersStartWithRetirePeersAlone(t *testing.T) {
	cfg := testConfig(t)
	cfg.RetirePeers = []string{"gone"}
	srv := New(cfg)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()
	waitHTTPAddr(t, srv)
	// Positive: retire_peers alone starts the loop and its system conn.
	if srv.metaReconcileStop == nil {
		t.Fatal("meta reconcilers not started with retire_peers set")
	}
	srv.Stop()
	// Negative: shutdown is not held up by it.
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run did not return after Stop")
	}
}
