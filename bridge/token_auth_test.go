// token_auth_test.go
// Tests for scoped worker-token authentication on the bridge (#627):
// bridge auth mode is decided by the env token ALONE. With it unset,
// the bridge stays in dev mode (allow all) regardless of whether a
// Store is wired in -- minted tokens are only meaningful once an
// admin token exists to mint them. With it set, the bearer must be
// either the env token (admin, bypasses scoping) or a valid minted
// token (checked against its task-type prefixes).
// Methodology: real NATS, real workertoken.Store, httptest.
package bridge

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/nats-io/nats.go/jetstream"
)

// openTokenStore opens a workertoken.Store for the test's NATS
// connection and registers cleanup.
func openTokenStore(t *testing.T, js jetstream.JetStream) *workertoken.Store {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := workertoken.Open(ctx, js)
	if err != nil {
		t.Fatalf("workertoken.Open: %v", err)
	}
	t.Cleanup(store.Close)
	return store
}

func pollWithBearer(
	t *testing.T, baseURL, bearer, taskTypesJSON string,
) *http.Response {
	t.Helper()
	body := `{"task_types":` + taskTypesJSON + `,"timeout_ms":100}`
	req, err := http.NewRequest(
		"POST", baseURL+"/v1/tasks/poll", strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("poll request: %v", err)
	}
	return resp
}

func TestTokenPollInScopeSucceeds(t *testing.T) {
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
	// Minted tokens are only meaningful once an admin token exists to
	// mint them -- the env token must be set for Store.Authorize to be
	// consulted at all.
	b.token = "admin-secret"
	b.SetTokenStore(store)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	_, bearer, err := store.Mint(mintCtx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	resp := pollWithBearer(t, ts.URL, bearer, `["echo"]`)
	defer resp.Body.Close()
	// Positive: an in-scope task type is accepted (200, empty result set
	// since nothing was enqueued -- this test only proves auth, not
	// dispatch).
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

func TestTokenPollOutOfScopeForbidden(t *testing.T) {
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
	_, bearer, err := store.Mint(mintCtx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	resp := pollWithBearer(t, ts.URL, bearer, `["other-type"]`)
	defer resp.Body.Close()
	// Positive: an out-of-scope task type is rejected with 403.
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusForbidden)
	}
}

func TestTokenAdminBypassesScoping(t *testing.T) {
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

	resp := pollWithBearer(t, ts.URL, "admin-secret", `["anything-goes"]`)
	defer resp.Body.Close()
	// Positive: the admin bearer authorizes any task type.
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

// TestTokenEnvUnsetStoreWiredAllowsUnauthenticated pins the fix for the
// lockout bug: wiring a Store into every server (#627) must NOT change
// dev-mode behavior. Bridge auth mode is decided by the env token
// ALONE -- with it unset, an unauthenticated request still succeeds
// even though a Store is configured, because a fresh `dagnats serve`
// with no admin token could otherwise never admit any worker (minting
// itself requires the admin token, so no worker token could ever
// exist to satisfy a Store-only gate).
func TestTokenEnvUnsetStoreWiredAllowsUnauthenticated(t *testing.T) {
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
	// b.token intentionally left empty (env token unset).
	b.SetTokenStore(store)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// Positive: no Authorization header at all still succeeds -- dev
	// mode, unaffected by the Store being wired in.
	resp := pollWithBearer(t, ts.URL, "", `["echo"]`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
}

// TestTokenEnvSetRandomBearerRejected keeps the "env token set + random
// bearer -> 401" case pinned once a Store is wired in, distinct from
// the dev-mode case above.
func TestTokenEnvSetRandomBearerRejected(t *testing.T) {
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

	resp := pollWithBearer(t, ts.URL, "dgn_notarealtoken_secretvalue", `["echo"]`)
	defer resp.Body.Close()
	// Negative: a random bearer is rejected once the env token is set.
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

// TestFirstUnauthorizedTaskTypeEmptyTokenIDNeverBypasses pins the fix
// for a defensive-coding hole: only claims.Admin bypasses scoping.
// Before this fix, a non-admin Claims with an empty TokenID (a
// hand-built or future zero-value Claims{}) was ALSO treated as
// unscoped -- silently as permissive as admin, for a shape that must
// never be reachable via HTTP today but must not be trusted if it is.
func TestFirstUnauthorizedTaskTypeEmptyTokenIDNeverBypasses(t *testing.T) {
	claims := workertoken.Claims{} // Admin: false, TokenID: "", no prefixes.
	// Positive: every task type is rejected, not bypassed.
	unmatched, ok := firstUnauthorizedTaskType(claims, []string{"echo"})
	if !ok || unmatched != "echo" {
		t.Fatalf("firstUnauthorizedTaskType(zero Claims) = (%q, %v), want (echo, true)",
			unmatched, ok)
	}
	// Negative: Admin claims still bypass, unaffected by the fix.
	admin := workertoken.Claims{Admin: true}
	if _, ok := firstUnauthorizedTaskType(admin, []string{"echo"}); ok {
		t.Fatal("firstUnauthorizedTaskType(admin) rejected a type, want bypass")
	}
}
