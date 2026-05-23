// Test methodology: Integration tests with real embedded NATS servers.
// Each test creates its own server with isolated state to avoid cross-test pollution.
// All tests use bounded timeouts and verify both positive and negative space.

package server

import (
	"bytes"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"
	"time"

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

func TestStartNATS_ClusterOptsSet(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	cfg.NATSPort = -1
	cfg.NATSClusterName = "dagnats-test"
	cfg.NATSClusterRoutes = []string{
		"nats://127.0.0.1:16222",
		"nats://127.0.0.1:16223",
	}
	cfg.NATSClusterAuthToken = "tok"

	ns, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("startNATS: %v", err)
	}
	t.Cleanup(func() { ns.Shutdown() })

	if ns.ClusterAddr() == nil {
		t.Error("ClusterAddr is nil; cluster opts not applied")
	}
}

// freeTCPPort returns a port the OS just confirmed it can bind.
// The listener is closed before returning; there is a tiny race
// where another process could grab the port, but in practice
// this is the standard pattern for picking ports in tests.
func freeTCPPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	port := ln.Addr().(*net.TCPAddr).Port
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	if port <= 0 {
		t.Fatalf("invalid free port %d", port)
	}
	return port
}

// TestStartNATS_WebsocketDisabledByDefault verifies that with
// NATSWebsocketPort == 0 no WebSocket listener is bound and a
// `ws://` connect attempt to an arbitrary port fails. This is
// the safe production posture per ADR-020.
func TestStartNATS_WebsocketDisabledByDefault(t *testing.T) {
	probePort := freeTCPPort(t)
	cfg := Config{
		DataDir:       t.TempDir(),
		NATSPort:      -1,
		MaxStoreBytes: 1 << 30,
	}
	ns, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("startNATS: %v", err)
	}
	t.Cleanup(func() { ns.Shutdown() })

	if ns.ClientURL() == "" {
		t.Fatal("ClientURL empty; sanity check")
	}
	// No WS listener bound on probePort — verify a fresh TCP
	// listen on that port still succeeds (i.e. the server did
	// not claim it).
	ln, err := net.Listen("tcp",
		fmt.Sprintf("127.0.0.1:%d", probePort))
	if err != nil {
		t.Fatalf("port %d unexpectedly taken; WS may be on by "+
			"default: %v", probePort, err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d", probePort)
	nc, err := nats.Connect(wsURL,
		nats.Timeout(500*time.Millisecond),
		nats.NoReconnect())
	if err == nil {
		nc.Close()
		t.Errorf("WS connect to %s should fail when WS "+
			"disabled by default", wsURL)
	}
}

// TestStartNATS_WebsocketEnabledRoundtrip spins up the embedded
// server with a WebSocket listener and uses the Go nats.go client
// (which speaks ws://) to publish + receive. ADR-020 acceptance:
// a browser-equivalent client can connect and round-trip messages.
func TestStartNATS_WebsocketEnabledRoundtrip(t *testing.T) {
	wsPort := freeTCPPort(t)
	cfg := Config{
		DataDir:            t.TempDir(),
		NATSPort:           -1,
		MaxStoreBytes:      1 << 30,
		NATSWebsocketPort:  wsPort,
		NATSWebsocketNoTLS: true,
	}
	ns, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("startNATS: %v", err)
	}
	t.Cleanup(func() { ns.Shutdown() })

	if ns.WebsocketURL() == "" {
		t.Fatal("WebsocketURL is empty; WS listener not started")
	}

	wsURL := fmt.Sprintf("ws://127.0.0.1:%d", wsPort)
	nc, err := nats.Connect(wsURL,
		nats.Timeout(2*time.Second),
		nats.ReconnectWait(100*time.Millisecond))
	if err != nil {
		t.Fatalf("ws connect %s: %v", wsURL, err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync("ws.echo")
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	if err := nc.Publish("ws.echo", []byte("hello-ws")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("flush: %v", err)
	}

	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	if got := string(msg.Data); got != "hello-ws" {
		t.Errorf("payload = %q, want %q", got, "hello-ws")
	}
}

// TestStartNATS_WebsocketRequiresNoTLSOptIn confirms the
// explicit insecure-mode contract: port>0 without NoTLS=true
// returns a clear error (no top-level TLS config exists yet).
func TestStartNATS_WebsocketRequiresNoTLSOptIn(t *testing.T) {
	cfg := Config{
		DataDir:            t.TempDir(),
		NATSPort:           -1,
		MaxStoreBytes:      1 << 30,
		NATSWebsocketPort:  freeTCPPort(t),
		NATSWebsocketNoTLS: false,
	}
	ns, err := startNATS(cfg)
	if err == nil {
		ns.Shutdown()
		t.Fatal("expected error when WS enabled without NoTLS opt-in")
	}
	if !strings.Contains(err.Error(), "nats-ws-no-tls") {
		t.Errorf("error %q should mention nats-ws-no-tls", err.Error())
	}
}

// TestStartNATS_WebsocketWarnsOnNoTLS captures the stderr warning
// emitted when the WS listener runs without TLS. Per the audit
// comment: cheap belt-and-suspenders so operators don't ship
// cleartext to prod.
func TestStartNATS_WebsocketWarnsOnNoTLS(t *testing.T) {
	origStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("pipe: %v", err)
	}
	os.Stderr = w
	t.Cleanup(func() { os.Stderr = origStderr })

	cfg := Config{
		DataDir:            t.TempDir(),
		NATSPort:           -1,
		MaxStoreBytes:      1 << 30,
		NATSWebsocketPort:  freeTCPPort(t),
		NATSWebsocketNoTLS: true,
	}
	ns, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("startNATS: %v", err)
	}
	t.Cleanup(func() { ns.Shutdown() })

	if err := w.Close(); err != nil {
		t.Fatalf("close writer: %v", err)
	}
	var buf bytes.Buffer
	if _, err := buf.ReadFrom(r); err != nil {
		t.Fatalf("read stderr: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "WebSocket") {
		t.Errorf("stderr missing WebSocket warning, got: %q", out)
	}
	if !strings.Contains(strings.ToLower(out), "tls") {
		t.Errorf("stderr missing TLS hint, got: %q", out)
	}
}
