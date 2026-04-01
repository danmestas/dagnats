// Test methodology: Integration tests with real embedded NATS servers.
// Each test creates its own server with isolated state to avoid cross-test pollution.
// All tests use bounded timeouts and verify both positive and negative space.

package server

import (
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestStartNATS_Standalone(t *testing.T) {
	// Use random port and temp directory for isolation
	cfg := Config{
		DataDir:       t.TempDir(),
		HTTPAddr:      ":8080",
		NATSPort:      -1, // Random port
		LeafRemotes:   nil,
		MaxStoreBytes: 1 << 30, // 1 GiB
	}

	ns, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("startNATS failed: %v", err)
	}
	if ns == nil {
		t.Fatal("startNATS returned nil server")
	}
	defer ns.Shutdown()

	// Verify we can connect a client
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("failed to connect to NATS: %v", err)
	}
	defer nc.Close()

	// Verify JetStream is available
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream not available: %v", err)
	}
	if js == nil {
		t.Fatal("JetStream context is nil")
	}
}

func TestStartNATS_StandaloneBindsLocalhost(t *testing.T) {
	cfg := Config{
		DataDir:       t.TempDir(),
		HTTPAddr:      ":8080",
		NATSPort:      -1,
		LeafRemotes:   nil,
		MaxStoreBytes: 1 << 30,
	}

	ns, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("startNATS failed: %v", err)
	}
	if ns == nil {
		t.Fatal("startNATS returned nil server")
	}
	defer ns.Shutdown()

	// Standalone should bind to localhost
	addr := ns.Addr().String()
	if !strings.Contains(addr, "127.0.0.1") {
		t.Errorf("expected address to contain 127.0.0.1, got %s", addr)
	}
	if addr == "" {
		t.Error("server address is empty")
	}
}
