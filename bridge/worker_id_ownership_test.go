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
	"sync"
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
	if entryAfterAdmin.TokenID != worker.AdminTokenID {
		t.Fatalf(
			"entry.TokenID = %q after admin takeover, want %q",
			entryAfterAdmin.TokenID, worker.AdminTokenID,
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
	if !ok || entry.TokenID != worker.AdminTokenID {
		t.Fatalf(
			"entry after admin takeover = %+v, ok=%v, want TokenID=%q",
			entry, ok, worker.AdminTokenID,
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
	if entry.TokenID != worker.AdminTokenID {
		t.Fatalf(
			"entry.TokenID = %q after A's stale disconnect, want %q (still admin's)",
			entry.TokenID, worker.AdminTokenID,
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

// raceConnectResult is one goroutine's outcome from raceConnect, sent
// back over a channel so the connecting goroutines never call t.Fatal
// themselves (unsafe/unsupported from a non-test goroutine).
type raceConnectResult struct {
	idx    int
	resp   *http.Response
	cancel context.CancelFunc
	err    error
}

// raceConnect is connectWorker without the *testing.T dependency, so
// it's safe to call concurrently from goroutines that are not the
// test's own goroutine.
func raceConnect(baseURL, bearer, workerID string) (*http.Response, context.CancelFunc, error) {
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
		return nil, nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose // caller closes
	if err != nil {
		cancel()
		return nil, nil, err
	}
	return resp, cancel, nil
}

// TestConnectConcurrentClaimExactlyOneWinner pins RegisterOwned's
// revision guard (#650 round 3, MAJOR #2): the register-time ownership
// check alone (Get, then a plain Put) leaves a window where two
// tokens racing a fresh worker_id both pass the Get and the later Put
// wins, silently overwriting the earlier one instead of rejecting it.
// N distinct tokens connect the same never-before-seen worker_id
// concurrently; exactly one may win (200) and every other racer must
// see 409, with the directory entry left owned by the winner alone.
func TestConnectConcurrentClaimExactlyOneWinner(t *testing.T) {
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

	const racerCount = 8
	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	ids := make([]string, racerCount)
	bearers := make([]string, racerCount)
	for i := range racerCount {
		id, bearer, err := store.Mint(
			mintCtx, fmt.Sprintf("racer-%d", i), []string{"echo"}, "tester",
		)
		if err != nil {
			t.Fatalf("Mint racer %d: %v", i, err)
		}
		ids[i] = id
		bearers[i] = bearer
	}

	results := make(chan raceConnectResult, racerCount)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range racerCount {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			resp, cancel, err := raceConnect(ts.URL, bearers[idx], "race-w1")
			results <- raceConnectResult{idx: idx, resp: resp, cancel: cancel, err: err}
		}(i)
	}
	close(start)
	go func() {
		wg.Wait()
		close(results)
	}()

	statuses := make([]int, racerCount)
	var cleanups []func()
	for res := range results {
		if res.err != nil {
			t.Fatalf("racer %d: connect request: %v", res.idx, res.err)
		}
		statuses[res.idx] = res.resp.StatusCode
		resp, cancel := res.resp, res.cancel
		cleanups = append(cleanups, func() {
			resp.Body.Close()
			cancel()
		})
	}
	t.Cleanup(func() {
		for _, c := range cleanups {
			c()
		}
	})

	winners := 0
	winnerIdx := -1
	for i, code := range statuses {
		switch code {
		case http.StatusOK:
			winners++
			winnerIdx = i
		case http.StatusConflict:
			// expected for every loser
		default:
			t.Fatalf("racer %d unexpected status %d", i, code)
		}
	}
	if winners != 1 {
		t.Fatalf(
			"winners = %d, want exactly 1 (statuses=%v)", winners, statuses,
		)
	}

	dir := worker.NewDirectory(js)
	entry, ok := workerPresent(t, dir, "race-w1")
	if !ok || entry.TokenID != ids[winnerIdx] {
		t.Fatalf(
			"entry after race = %+v, ok=%v, want TokenID=%q",
			entry, ok, ids[winnerIdx],
		)
	}
}

// TestHeartbeatStopsAfterOwnershipTakeover pins the #650 round 3
// BLOCKER: the heartbeat loop used to replay a plain, unguarded Put
// of its connect-time TokenID every tick, with no ownership check, no
// revision guard, and a swallowed error -- resurrecting a worker_id
// an admin had already taken over on the very next tick. Overrides
// the package's heartbeatInterval to observe several ticks inside a
// bounded wait instead of the real 25s cadence.
func TestHeartbeatStopsAfterOwnershipTakeover(t *testing.T) {
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
	heartbeatInterval = 200 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = origInterval })

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	idA, bearerA, err := store.Mint(mintCtx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint A: %v", err)
	}

	dir := worker.NewDirectory(js)

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

	// A's SSE stream closing (EOF) is the observable signal that its
	// heartbeat loop stopped -- heartbeatReregister returning false
	// ends sendHeartbeatLoop, which ends handleConnect's response.
	aClosed := make(chan struct{})
	go func() {
		defer close(aClosed)
		_, _ = io.Copy(io.Discard, respA.Body)
	}()

	// Admin takes over w1 while A's heartbeat loop is still running.
	respAdmin, cancelAdmin := connectWorker(t, ts.URL, "admin-secret", "w1")
	t.Cleanup(cancelAdmin)
	if respAdmin.StatusCode != http.StatusOK {
		t.Fatalf("admin connect status = %d, want 200", respAdmin.StatusCode)
	}

	select {
	case <-aClosed:
	case <-time.After(3*heartbeatInterval + 2*time.Second):
		t.Fatal("A's SSE connection did not close after admin takeover")
	}

	// The entry must stay admin's across several more heartbeat
	// intervals -- if A's loop were still fighting back (the pre-fix
	// unguarded Put) it would have re-claimed it by now.
	for tick := range 3 {
		time.Sleep(heartbeatInterval)
		entry, ok := workerPresent(t, dir, "w1")
		if !ok || entry.TokenID != worker.AdminTokenID {
			t.Fatalf(
				"entry after takeover, tick %d = %+v, ok=%v, want TokenID=%q",
				tick, entry, ok, worker.AdminTokenID,
			)
		}
	}
}

// TestHeartbeatContinuesForOwner pins that a connection which still
// owns its worker_id keeps successfully re-registering (and so
// refreshing LastSeen) across multiple heartbeat ticks -- the #650
// round 3 fix must not turn every heartbeat into a rejection for the
// common, uncontested case.
func TestHeartbeatContinuesForOwner(t *testing.T) {
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
	heartbeatInterval = 200 * time.Millisecond
	t.Cleanup(func() { heartbeatInterval = origInterval })

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	idA, bearerA, err := store.Mint(mintCtx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint A: %v", err)
	}

	dir := worker.NewDirectory(js)

	respA, cancelA := connectWorker(t, ts.URL, bearerA, "w3")
	t.Cleanup(cancelA)
	if respA.StatusCode != http.StatusOK {
		t.Fatalf("A connect status = %d, want 200", respA.StatusCode)
	}

	entry1, ok := workerPresent(t, dir, "w3")
	if !ok || entry1.TokenID != idA {
		t.Fatalf(
			"entry after connect = %+v, ok=%v, want TokenID=%q",
			entry1, ok, idA,
		)
	}

	time.Sleep(3 * heartbeatInterval)

	entry2, ok := workerPresent(t, dir, "w3")
	if !ok {
		t.Fatal("w3 no longer present after heartbeat ticks")
	}
	if entry2.TokenID != idA {
		t.Fatalf(
			"entry.TokenID = %q after heartbeat ticks, want %q (still A's)",
			entry2.TokenID, idA,
		)
	}
	if !entry2.LastSeen.After(entry1.LastSeen) {
		t.Fatalf(
			"LastSeen did not advance across heartbeat ticks (before=%v, after=%v) -- own-entry heartbeat did not refresh",
			entry1.LastSeen, entry2.LastSeen,
		)
	}
}
