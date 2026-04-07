// Test methodology: Integration tests with real embedded NATS servers.
// Each test creates its own server with isolated state to avoid cross-test pollution.
// All tests use bounded timeouts and verify both positive and negative space.

package server

import (
	"fmt"
	"net"
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

func TestStartNATS_FallsBackWhenDefaultPortTaken(t *testing.T) {
	ln, err := net.Listen("tcp", fmt.Sprintf(
		"127.0.0.1:%d", defaultNATSPort))
	if err != nil {
		t.Skipf("cannot bind default port %d: %v",
			defaultNATSPort, err)
	}
	defer ln.Close()

	cfg := Config{
		DataDir:       t.TempDir(),
		NATSPort:      defaultNATSPort,
		MaxStoreBytes: 1 << 30,
	}

	ns, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("startNATS should fallback: %v", err)
	}
	defer ns.Shutdown()

	addr := ns.Addr().String()
	if addr == "" {
		t.Fatal("server address is empty")
	}

	defaultAddr := fmt.Sprintf("127.0.0.1:%d",
		defaultNATSPort)
	if addr == defaultAddr {
		t.Errorf("expected different port, got %s", addr)
	}

	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect to fallback port: %v", err)
	}
	nc.Close()
}

func TestStartNATS_ExplicitPortFailsHard(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:14222")
	if err != nil {
		t.Skipf("cannot bind 14222: %v", err)
	}
	defer ln.Close()

	cfg := Config{
		DataDir:       t.TempDir(),
		NATSPort:      14222,
		MaxStoreBytes: 1 << 30,
	}

	ns, err := startNATS(cfg)
	if err == nil {
		ns.Shutdown()
		t.Fatal("expected error for explicit port conflict")
	}
}
