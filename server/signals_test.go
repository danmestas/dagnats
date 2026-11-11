//go:build unix

// Tests for Config.IgnoreSignals (#998).
// Methodology: a real embedded Server in this test process. The test
// installs its own SIGTERM handler first, as an embedding host would, so
// SIGTERM sent to the process cannot kill the test binary. It then
// observes whether Run returns. Positive: with IgnoreSignals the server
// keeps serving through SIGTERM and stops only on Stop. Negative: without
// it, the same SIGTERM shuts the server down, as before.
package server

import (
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

// signalWait bounds how long a test waits for Run to react to a signal.
const signalWait = 20 * time.Second

// startForSignals runs a server built from cfg and returns its Run error
// channel once it serves /health.
func startForSignals(t *testing.T, cfg Config) (*Server, <-chan error) {
	t.Helper()
	srv := New(cfg)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()
	addr := waitHTTPAddr(t, srv)
	resp, err := http.Get(fmt.Sprintf("http://%s/health", addr))
	if err != nil {
		t.Fatalf("GET /health: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("/health = %d, want 200", resp.StatusCode)
	}
	return srv, errCh
}

// sigtermSelf delivers SIGTERM to this process and waits until the
// test's own handler has seen it, so the server's handler (if any) has
// been signalled too.
func sigtermSelf(t *testing.T) {
	t.Helper()
	host := make(chan os.Signal, 1)
	signal.Notify(host, syscall.SIGTERM)
	defer signal.Stop(host)
	if err := syscall.Kill(os.Getpid(), syscall.SIGTERM); err != nil {
		t.Fatalf("kill -TERM self: %v", err)
	}
	select {
	case <-host:
	case <-time.After(signalWait):
		t.Fatal("SIGTERM was not delivered to the test's handler")
	}
}

func TestServer_IgnoreSignalsKeepsServingThroughSIGTERM(t *testing.T) {
	// The host's handler is installed before the server starts, as an
	// embedder's would be, and outlives the signal.
	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGTERM)
	defer signal.Stop(guard)

	cfg := testConfig(t)
	cfg.IgnoreSignals = true
	srv, errCh := startForSignals(t, cfg)

	sigtermSelf(t)
	select {
	case err := <-errCh:
		t.Fatalf("Run returned on SIGTERM with IgnoreSignals set: %v", err)
	case <-time.After(500 * time.Millisecond):
	}
	if !srv.ready.Load() {
		t.Fatal("server stopped being ready after SIGTERM with IgnoreSignals set")
	}

	srv.Stop()
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run after Stop: %v", err)
		}
	case <-time.After(signalWait):
		t.Fatal("Run did not return after Stop")
	}
}

func TestServer_DefaultStillShutsDownOnSIGTERM(t *testing.T) {
	guard := make(chan os.Signal, 1)
	signal.Notify(guard, syscall.SIGTERM)
	defer signal.Stop(guard)

	cfg := testConfig(t)
	if cfg.IgnoreSignals {
		t.Fatal("IgnoreSignals must default to false")
	}
	_, errCh := startForSignals(t, cfg)

	sigtermSelf(t)
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run after SIGTERM: %v", err)
		}
	case <-time.After(signalWait):
		t.Fatal("default server did not shut down on SIGTERM")
	}
}
