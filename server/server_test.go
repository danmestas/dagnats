// Methodology: integration tests with real embedded NATS. Each test
// gets isolated server on random port. Verify startup, health, shutdown.
package server

import (
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
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

// TestServer_PrintsStartupAndShutdownBanner verifies that the server
// emits progress lines to stderr during startup and shutdown.
func TestServer_PrintsStartupAndShutdownBanner(t *testing.T) {
	cfg := testConfig(t)
	srv := New(cfg)

	if srv == nil {
		panic("New() returned nil")
	}

	// Capture stderr via pipe so we can inspect output
	origStderr := os.Stderr
	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	os.Stderr = pipeW

	// Restore stderr on exit regardless of outcome
	defer func() { os.Stderr = origStderr }()

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Run()
	}()

	// Poll /ready until server is up (bounded 10s)
	readyURL := fmt.Sprintf("http://%s/ready", cfg.HTTPAddr)
	deadline := time.Now().Add(10 * time.Second)
	ready := false

	for time.Now().Before(deadline) && !ready {
		resp, getErr := http.Get(readyURL)
		if getErr == nil && resp.StatusCode == http.StatusOK {
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

	// Stop server and wait for Run() to return
	srv.Stop()
	select {
	case runErr := <-errCh:
		if runErr != nil {
			t.Fatalf("Run() returned error: %v", runErr)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("Run() did not return within 20s")
	}

	// Close the write end so reads complete, then collect output
	pipeW.Close()
	captured, readErr := io.ReadAll(pipeR)
	if readErr != nil {
		t.Fatalf("read captured stderr: %v", readErr)
	}
	pipeR.Close()
	output := string(captured)

	// Positive assertions: startup and shutdown banners present
	if !strings.Contains(output, "nats server started") {
		t.Errorf("stderr missing 'nats server started'; got:\n%s", output)
	}
	if !strings.Contains(output, "http") {
		t.Errorf("stderr missing ready banner; got:\n%s", output)
	}
	if !strings.Contains(output, "shutdown complete") {
		t.Errorf("stderr missing 'shutdown complete'; got:\n%s", output)
	}

	// Negative: no errors during a clean lifecycle
	if strings.Contains(strings.ToLower(output), "error") {
		t.Errorf("stderr contains 'error' during clean run:\n%s", output)
	}
}
