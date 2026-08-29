// worker_id_ownership_test.go
// Tests for #650: POST /v1/workers/connect must not let one token's
// worker_id overwrite another token's directory entry.
// Methodology: real NATS server, real workertoken.Store, httptest
// server; connect is a long-lived SSE stream so each helper keeps
// the underlying request alive with a cancelable context and returns
// the cancel func for the caller to close when done with that
// connection.
package bridge

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/worker"
	"github.com/nats-io/nats.go/jetstream"
)

// connectWorker issues POST /v1/workers/connect for workerID with the
// given bearer (empty means no Authorization header) and returns the
// response plus a cancel func that closes the underlying connection.
// The caller MUST read/close resp.Body and call cancel when finished
// with the connection, even for non-200 responses.
func connectWorker(
	t *testing.T, baseURL, bearer, workerID string,
) (*http.Response, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	body := fmt.Sprintf(
		`{"worker_id":%q,"task_types":["echo"],"max_tasks":1}`,
		workerID,
	)
	req, err := http.NewRequestWithContext(
		ctx, "POST", baseURL+"/v1/workers/connect", strings.NewReader(body),
	)
	if err != nil {
		cancel()
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // caller closes
	if err != nil {
		cancel()
		t.Fatalf("connect request: %v", err)
	}
	return resp, cancel
}

// findWorker returns the registration for workerID, failing the test
// if it is not present in the directory.
func findWorker(
	t *testing.T, dir *worker.Directory, workerID string,
) worker.WorkerRegistration {
	t.Helper()
	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, w := range workers {
		if w.WorkerID == workerID {
			return w
		}
	}
	t.Fatalf("worker %q not found in directory", workerID)
	return worker.WorkerRegistration{}
}

// TestConnectWorkerIDOwnershipEnforced pins the #650 fix: worker_id
// is claimed by the first token that registers it, and can only be
// re-registered by that same token or by an admin caller.
func TestConnectWorkerIDOwnershipEnforced(t *testing.T) {
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
	defer ts.Close()

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	idA, bearerA, err := store.Mint(mintCtx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint A: %v", err)
	}
	_, bearerB, err := store.Mint(mintCtx, "worker-b", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint B: %v", err)
	}

	dir := worker.NewDirectory(js)

	// A connects and claims w1. The connection is left open (Body not
	// closed) until the end of the test -- closing it would trigger
	// handleConnect's deferred Deregister and delete the entry we're
	// about to assert on.
	respA1, cancelA1 := connectWorker(t, ts.URL, bearerA, "w1")
	if respA1.StatusCode != http.StatusOK {
		t.Fatalf("A connect status = %d, want 200", respA1.StatusCode)
	}
	entry := findWorker(t, dir, "w1")
	if entry.TokenID != idA {
		t.Fatalf("entry.TokenID = %q, want %q", entry.TokenID, idA)
	}

	// B attempts to take over w1 -- must be rejected and must not
	// disturb A's entry or leak A's token id in the response body.
	respB, cancelB := connectWorker(t, ts.URL, bearerB, "w1")
	if respB.StatusCode != http.StatusConflict {
		t.Fatalf("B connect status = %d, want 409", respB.StatusCode)
	}
	bodyB, err := io.ReadAll(respB.Body)
	respB.Body.Close()
	cancelB()
	if err != nil {
		t.Fatalf("read B body: %v", err)
	}
	if strings.Contains(string(bodyB), idA) {
		t.Fatalf("409 body leaks owning token id: %s", bodyB)
	}

	entryAfterB := findWorker(t, dir, "w1")
	if entryAfterB.TokenID != idA {
		t.Fatalf(
			"A's entry.TokenID changed to %q after B's rejected connect",
			entryAfterB.TokenID,
		)
	}

	// A reconnects (restart/heartbeat) -- same token, must succeed.
	respA2, cancelA2 := connectWorker(t, ts.URL, bearerA, "w1")
	if respA2.StatusCode != http.StatusOK {
		t.Fatalf("A reconnect status = %d, want 200", respA2.StatusCode)
	}
	respA2.Body.Close()
	cancelA2()

	// Admin takes over w1 -- must succeed regardless of A's ownership.
	respAdmin, cancelAdmin := connectWorker(t, ts.URL, "admin-secret", "w1")
	if respAdmin.StatusCode != http.StatusOK {
		t.Fatalf("admin connect status = %d, want 200", respAdmin.StatusCode)
	}
	entryAfterAdmin := findWorker(t, dir, "w1")
	if entryAfterAdmin.TokenID != "" {
		t.Fatalf(
			"entry.TokenID = %q after admin takeover, want empty",
			entryAfterAdmin.TokenID,
		)
	}
	respAdmin.Body.Close()
	cancelAdmin()
	respA1.Body.Close()
	cancelA1()
}

// TestConnectWorkerIDOwnershipDevMode pins that dev mode (no env
// token configured) never enforces worker_id ownership -- there is
// no identity to enforce, so any caller may claim any worker_id.
func TestConnectWorkerIDOwnershipDevMode(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	resp1, cancel1 := connectWorker(t, ts.URL, "", "dev-w1")
	if resp1.StatusCode != http.StatusOK {
		t.Fatalf("first connect status = %d, want 200", resp1.StatusCode)
	}
	resp1.Body.Close()
	cancel1()

	// A second, unrelated caller (arbitrary bearer, ignored in dev
	// mode) claims the same worker_id without being rejected.
	resp2, cancel2 := connectWorker(t, ts.URL, "whatever-bearer", "dev-w1")
	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("second connect status = %d, want 200", resp2.StatusCode)
	}
	resp2.Body.Close()
	cancel2()
}

// TestConnectWorkerIDOwnershipUnownedEntryClaimable pins that an
// entry with an empty token_id (written before #627, or by a
// dev-mode/native worker) has no owner and is claimable by any token.
func TestConnectWorkerIDOwnershipUnownedEntryClaimable(t *testing.T) {
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
	defer ts.Close()

	// Simulate a pre-#627 (or native/dev-mode) entry: registered
	// directly through the Directory with no token_id, bypassing the
	// HTTP bridge entirely.
	dir := worker.NewDirectory(js)
	if err := dir.Register(worker.WorkerRegistration{
		WorkerID:  "legacy-w1",
		TaskTypes: []string{"echo"},
	}); err != nil {
		t.Fatalf("seed Register: %v", err)
	}
	seeded := findWorker(t, dir, "legacy-w1")
	if seeded.TokenID != "" {
		t.Fatalf("seeded entry.TokenID = %q, want empty", seeded.TokenID)
	}

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	idA, bearerA, err := store.Mint(mintCtx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint A: %v", err)
	}

	resp, cancel := connectWorker(t, ts.URL, bearerA, "legacy-w1")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("claim status = %d, want 200", resp.StatusCode)
	}
	// Check the directory before closing Body -- closing triggers
	// handleConnect's deferred Deregister and would delete the entry.
	claimed := findWorker(t, dir, "legacy-w1")
	if claimed.TokenID != idA {
		t.Fatalf("claimed entry.TokenID = %q, want %q", claimed.TokenID, idA)
	}
	resp.Body.Close()
	cancel()
}

// workerPresent reports whether workerID currently has an entry in
// the directory, without failing the test.
func workerPresent(
	t *testing.T, dir *worker.Directory, workerID string,
) (worker.WorkerRegistration, bool) {
	t.Helper()
	workers, err := dir.List()
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	for _, w := range workers {
		if w.WorkerID == workerID {
			return w, true
		}
	}
	return worker.WorkerRegistration{}, false
}

// TestConnectDeregisterOwnershipScoped pins the #650 delete-side fix:
// disconnect only removes the directory entry when the disconnecting
// connection's identity still owns it. A stale disconnect from a
// token that has since been superseded (e.g. by an admin takeover)
// must leave the current entry alone rather than delete it out from
// under its new owner.
func TestConnectDeregisterOwnershipScoped(t *testing.T) {
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
	// Cleanups run LIFO: registering Close first (before any
	// connection's cancel) guarantees every connection is torn down
	// before Close blocks waiting for them, even on an early t.Fatal.
	t.Cleanup(ts.Close)

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	idA, bearerA, err := store.Mint(mintCtx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint A: %v", err)
	}

	dir := worker.NewDirectory(js)

	// A connects and claims w1; leave the connection open.
	respA, cancelA := connectWorker(t, ts.URL, bearerA, "w1")
	t.Cleanup(cancelA)
	if respA.StatusCode != http.StatusOK {
		t.Fatalf("A connect status = %d, want 200", respA.StatusCode)
	}
	entry, ok := workerPresent(t, dir, "w1")
	if !ok || entry.TokenID != idA {
		t.Fatalf(
			"entry after A connect = %+v, ok=%v, want TokenID=%q",
			entry, ok, idA,
		)
	}

	// Admin takes over w1 while A is still connected.
	respAdmin, cancelAdmin := connectWorker(t, ts.URL, "admin-secret", "w1")
	t.Cleanup(cancelAdmin)
	if respAdmin.StatusCode != http.StatusOK {
		t.Fatalf("admin connect status = %d, want 200", respAdmin.StatusCode)
	}
	entry, ok = workerPresent(t, dir, "w1")
	if !ok || entry.TokenID != "" {
		t.Fatalf(
			"entry after admin takeover = %+v, ok=%v, want TokenID=\"\"",
			entry, ok,
		)
	}

	// A disconnects (stale connection, no longer the owner): w1 must
	// still be present and still admin's.
	respA.Body.Close()
	cancelA()
	time.Sleep(200 * time.Millisecond)
	entry, ok = workerPresent(t, dir, "w1")
	if !ok {
		t.Fatal("w1 was deleted by A's stale disconnect, want it to remain")
	}
	if entry.TokenID != "" {
		t.Fatalf(
			"entry.TokenID = %q after A's stale disconnect, want \"\" (still admin's)",
			entry.TokenID,
		)
	}

	// Admin disconnects its own entry: it is removed.
	respAdmin.Body.Close()
	cancelAdmin()
	time.Sleep(200 * time.Millisecond)
	if _, ok := workerPresent(t, dir, "w1"); ok {
		t.Fatal("w1 still present after admin's own disconnect, want it removed")
	}

	// A connects and disconnects its own worker_id (w2, no takeover):
	// the entry is removed by its own owner.
	respA2, cancelA2 := connectWorker(t, ts.URL, bearerA, "w2")
	t.Cleanup(cancelA2)
	if respA2.StatusCode != http.StatusOK {
		t.Fatalf("A connect w2 status = %d, want 200", respA2.StatusCode)
	}
	if _, ok := workerPresent(t, dir, "w2"); !ok {
		t.Fatal("w2 not present after A's own connect")
	}
	respA2.Body.Close()
	cancelA2()
	time.Sleep(200 * time.Millisecond)
	if _, ok := workerPresent(t, dir, "w2"); ok {
		t.Fatal("w2 still present after A's own disconnect, want it removed")
	}
}

// TestConnectDeregisterDevModeAlwaysDeletes pins that dev mode (no
// env token configured) always removes the entry on disconnect --
// every dev-mode caller is Admin, so there is no ownership to
// enforce.
func TestConnectDeregisterDevModeAlwaysDeletes(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	b := newTestBridge(t, nc)
	ts := httptest.NewServer(b.Handler())
	t.Cleanup(ts.Close)

	dir := worker.NewDirectory(js)

	resp, cancel := connectWorker(t, ts.URL, "", "dev-w1")
	t.Cleanup(cancel)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("connect status = %d, want 200", resp.StatusCode)
	}
	if _, ok := workerPresent(t, dir, "dev-w1"); !ok {
		t.Fatal("dev-w1 not present after connect")
	}
	resp.Body.Close()
	cancel()
	time.Sleep(200 * time.Millisecond)
	if _, ok := workerPresent(t, dir, "dev-w1"); ok {
		t.Fatal("dev-w1 still present after disconnect, want it removed")
	}
}
