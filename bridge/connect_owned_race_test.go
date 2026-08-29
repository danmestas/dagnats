// connect_owned_race_test.go
// CI-only-flake regression coverage for the RegisterOwned/
// DeregisterOwned revision-conflict-vs-ownership race (worker
// package): a revision conflict caused by a write from the SAME
// owner -- this connection's own heartbeat re-register, or its own
// disconnect-time deregister racing a reconnect -- must never
// surface as ErrWorkerIDOwned/409. Methodology: real embedded NATS
// server via natsutil.StartTestServer, httptest server; each
// scenario is looped many times (dozens of iterations) rather than
// run once, since the underlying race is timing-dependent and a
// single pass can pass by luck even on unfixed code.
package bridge

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// TestConnectDevModeReconnectNeverConflicts loops
// TestConnectWorkerIDOwnershipDevMode's connect/close/reconnect
// sequence 50 times: dev mode has no identity to enforce, so every
// reconnect for the same worker_id must succeed (200), never 409 --
// pins the fix for the race between a reconnect's RegisterOwned and
// the prior connection's own disconnect-time DeregisterOwned landing
// in the same Get-to-write window.
func TestConnectDevModeReconnectNeverConflicts(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	t.Cleanup(ts.Close)

	const iterations = 50
	conflicts := 0
	for i := range iterations {
		resp1, cancel1 := connectWorker(t, ts.URL, "", "dev-race")
		if resp1.StatusCode != http.StatusOK {
			t.Fatalf(
				"iteration %d: first connect status = %d, want 200",
				i, resp1.StatusCode,
			)
		}
		resp1.Body.Close()
		cancel1()

		resp2, cancel2 := connectWorker(t, ts.URL, "", "dev-race")
		if resp2.StatusCode == http.StatusConflict {
			conflicts++
		} else if resp2.StatusCode != http.StatusOK {
			t.Fatalf(
				"iteration %d: second connect status = %d, want 200",
				i, resp2.StatusCode,
			)
		}
		resp2.Body.Close()
		cancel2()
	}
	if conflicts != 0 {
		t.Fatalf(
			"%d/%d iterations returned 409 for a same-owner (dev mode)"+
				" reconnect, want 0",
			conflicts, iterations,
		)
	}
}

// TestConnectReconnectRacesOwnHeartbeatNeverConflicts pins the
// heartbeat-side mechanism directly: with heartbeatInterval lowered
// so several ticks land inside the test window, a token reconnects
// its own worker_id repeatedly while its prior connection's heartbeat
// loop is still ticking. A reconnect losing the revision race to its
// own heartbeat's re-register must retry and succeed, never 409.
func TestConnectReconnectRacesOwnHeartbeatNeverConflicts(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	store := openTokenStore(t, js)
	b := newTestBridge(t, nc)
	b.token = "admin-secret"
	b.SetTokenStore(store)
	ts := httptest.NewServer(b.Handler())
	t.Cleanup(ts.Close)

	origInterval := heartbeatInterval
	heartbeatInterval = 50 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = origInterval })

	// tickerCreated receives once per connection, from
	// sendHeartbeatLoopTestHook, immediately after that connection's
	// ticker is created (i.e. after its one-time read of
	// heartbeatInterval). A plain time.Sleep before closing a
	// connection does NOT prove that read has happened -- closing
	// the client side only unblocks this test's own read locally, so
	// it establishes no happens-before with the server goroutine, and
	// `-race` correctly flags restoring heartbeatInterval afterward
	// as racing it. Receiving on this channel does establish that
	// edge, so every connection is drained here before its
	// connection is ever closed and before the loop returns.
	const iterations = 50
	tickerCreated := make(chan struct{}, iterations)
	sendHeartbeatLoopTestHook = func() { tickerCreated <- struct{}{} }
	t.Cleanup(func() { sendHeartbeatLoopTestHook = nil })

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	_, bearerA, err := store.Mint(mintCtx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint A: %v", err)
	}

	conflicts := 0
	var lastConn *http.Response
	var lastCancel context.CancelFunc
	for i := range iterations {
		resp, cancel := connectWorker(t, ts.URL, bearerA, "w-heartbeat-race")
		switch resp.StatusCode {
		case http.StatusConflict:
			// Rejected before writeSSEHeaders/sendHeartbeatLoop ever
			// ran -- no ticker was created, nothing to wait for.
			conflicts++
			resp.Body.Close()
			cancel()
			continue
		case http.StatusOK:
			// handled below
		default:
			t.Fatalf(
				"iteration %d: connect status = %d, want 200",
				i, resp.StatusCode,
			)
		}
		// Drain in the background so this connection's own heartbeat
		// loop keeps running (and re-registering) while the next
		// reconnect races it, instead of blocking on an unread body.
		go func(body io.ReadCloser) { _, _ = io.Copy(io.Discard, body) }(resp.Body)
		select {
		case <-tickerCreated:
		case <-time.After(2 * time.Second):
			t.Fatalf(
				"iteration %d: timed out waiting for ticker creation",
				i,
			)
		}
		if lastConn != nil {
			// Give the still-open previous connection's heartbeat a
			// chance to tick (and re-register) while this reconnect
			// is in flight, so the race this test targets is
			// actually exercised rather than always winning on an
			// idle bucket.
			time.Sleep(heartbeatInterval)
			lastConn.Body.Close()
			lastCancel()
		}
		lastConn, lastCancel = resp, cancel
	}
	if lastConn != nil {
		lastConn.Body.Close()
		lastCancel()
	}
	if conflicts != 0 {
		t.Fatalf(
			"%d/%d iterations returned 409 for a reconnect racing its"+
				" own heartbeat, want 0",
			conflicts, iterations,
		)
	}
}
