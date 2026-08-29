// token_auth_test.go
// Tests for scoped worker-token authentication on the bridge (#627):
// admin bearer bypasses scoping, a minted worker token is checked
// against its task-type prefixes, out-of-scope polls are rejected, and
// an unset env token still allows minted tokens through a configured
// Store while rejecting an unrelated random bearer.
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

func TestTokenEnvUnsetStoreConfiguredMintedWorksRandomRejected(t *testing.T) {
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

	mintCtx, mintCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer mintCancel()
	_, bearer, err := store.Mint(mintCtx, "worker-a", []string{"echo"}, "tester")
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}

	okResp := pollWithBearer(t, ts.URL, bearer, `["echo"]`)
	defer okResp.Body.Close()
	// Positive: a minted token works even with no admin token set.
	if okResp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", okResp.StatusCode, http.StatusOK)
	}

	badResp := pollWithBearer(t, ts.URL, "dgn_notarealtoken_secretvalue", `["echo"]`)
	defer badResp.Body.Close()
	// Negative: a random bearer is rejected once a Store is configured
	// -- env-unset no longer means "allow all" when a Store exists.
	if badResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", badResp.StatusCode, http.StatusUnauthorized)
	}
}
