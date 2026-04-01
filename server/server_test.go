// Methodology: integration tests with real embedded NATS. Each test
// gets isolated server on random port. Verify startup, health, shutdown.
package server

import (
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

// testConfig returns a Config suitable for isolated testing.
// Uses a random free port for HTTP and embedded NATS.
func testConfig(t *testing.T) Config {
	if t == nil {
		panic("testConfig: t must not be nil")
	}

	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.NATSPort = -1 // Random port

	// Find free HTTP port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	addr := listener.Addr().String()
	listener.Close()
	cfg.HTTPAddr = addr

	return cfg
}

// TestServer_StartsAndStops verifies the server starts, becomes ready,
// serves health checks, and shuts down cleanly.
func TestServer_StartsAndStops(t *testing.T) {
	cfg := testConfig(t)
	srv := New(cfg)

	if srv == nil {
		panic("New() returned nil")
	}

	// Run server in background
	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run()
	}()

	// Poll /ready until server is ready (bounded 10s, 50ms sleep)
	readyURL := fmt.Sprintf("http://%s/ready", cfg.HTTPAddr)
	deadline := time.Now().Add(10 * time.Second)
	ready := false

	for time.Now().Before(deadline) && !ready {
		resp, err := http.Get(readyURL)
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			ready = true
			break
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !ready {
		t.Fatal("/ready did not return 200 within 10s")
	}

	// Verify /health returns 200
	healthURL := fmt.Sprintf("http://%s/health", cfg.HTTPAddr)
	resp, err := http.Get(healthURL)
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("/health status: got %d, want 200", resp.StatusCode)
	}
	resp.Body.Close()

	// Stop server
	srv.Stop()

	// Verify Run() returns nil within 20s
	select {
	case err := <-errCh:
		if err != nil {
			t.Errorf("Run() returned error: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run() did not return within 20s")
	}
}
