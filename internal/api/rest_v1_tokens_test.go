// rest_v1_tokens_test.go
// Tests for the admin-only /v1/tokens routes (#627): mint, list, and
// revoke a worker token over REST, the fail-closed 503 when
// DAGNATS_BRIDGE_TOKEN is unset, the 401 on a wrong admin bearer, and
// that List never leaks a hash or secret over the wire.
// Methodology: real embedded NATS, real workertoken.Store, httptest,
// t.Setenv for the admin token so each test controls its own value.
package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/workertoken"
	"github.com/nats-io/nats.go/jetstream"
)

// mintTokensTestFixture wires a Service + workertoken.Store behind
// MountV1 with DAGNATS_BRIDGE_TOKEN set to adminToken.
func mintTokensTestFixture(
	t *testing.T, adminToken string,
) (baseURL string) {
	t.Helper()
	t.Setenv("DAGNATS_BRIDGE_TOKEN", adminToken)
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err := workertoken.Open(ctx, js)
	if err != nil {
		t.Fatalf("workertoken.Open: %v", err)
	}
	t.Cleanup(store.Close)

	mux := http.NewServeMux()
	MountV1(mux, svc, store)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.URL
}

func doJSON(
	t *testing.T, method, url, bearer, body string,
) *http.Response {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatalf("NewRequest: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	return resp
}

func TestTokensAPI503WhenAdminTokenUnset(t *testing.T) {
	baseURL := mintTokensTestFixture(t, "")
	resp := doJSON(t, "POST", baseURL+"/v1/tokens", "", `{"label":"x"}`)
	defer resp.Body.Close()
	// Positive: an unset admin credential fails closed, not open.
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusServiceUnavailable)
	}
}

func TestTokensAPI401OnWrongAdminBearer(t *testing.T) {
	baseURL := mintTokensTestFixture(t, "correct-admin-token")
	resp := doJSON(t, "GET", baseURL+"/v1/tokens", "wrong-token", "")
	defer resp.Body.Close()
	// Positive: a set admin token still rejects the wrong bearer.
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}

func TestTokensAPIMintListRevokeRoundTrip(t *testing.T) {
	baseURL := mintTokensTestFixture(t, "admin-token")

	mintResp := doJSON(t, "POST", baseURL+"/v1/tokens", "admin-token",
		`{"label":"worker-a","task_type_prefixes":["echo"]}`)
	defer mintResp.Body.Close()
	// Positive: a correct admin bearer mints successfully.
	if mintResp.StatusCode != http.StatusCreated {
		t.Fatalf("mint status = %d, want %d", mintResp.StatusCode, http.StatusCreated)
	}
	var minted struct {
		ID               string    `json:"id"`
		Token            string    `json:"token"`
		Label            string    `json:"label"`
		TaskTypePrefixes []string  `json:"task_type_prefixes"`
		CreatedAt        time.Time `json:"created_at"`
	}
	if err := json.NewDecoder(mintResp.Body).Decode(&minted); err != nil {
		t.Fatalf("decode mint response: %v", err)
	}
	// Negative: the response actually carries a usable bearer and the
	// echoed scope, not just a 201 with an empty body.
	if minted.ID == "" || !strings.HasPrefix(minted.Token, "dgn_") {
		t.Fatalf("mint response missing id/token: %+v", minted)
	}
	if len(minted.TaskTypePrefixes) != 1 || minted.TaskTypePrefixes[0] != "echo" {
		t.Fatalf("TaskTypePrefixes = %v, want [echo]", minted.TaskTypePrefixes)
	}

	listResp := doJSON(t, "GET", baseURL+"/v1/tokens", "admin-token", "")
	defer listResp.Body.Close()
	if listResp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want %d", listResp.StatusCode, http.StatusOK)
	}
	var listed struct {
		Tokens []workertoken.Token `json:"tokens"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	// Positive: the minted token shows up in the listing.
	if len(listed.Tokens) != 1 || listed.Tokens[0].ID != minted.ID {
		t.Fatalf("Tokens = %+v, want one entry with id %q", listed.Tokens, minted.ID)
	}
	// Negative: List never carries the hash or secret over the wire.
	if listed.Tokens[0].SecretHash != nil {
		t.Fatalf("Tokens[0].SecretHash = %v, want nil", listed.Tokens[0].SecretHash)
	}
	// Positive: the mint response's CreatedAt is the token's actual
	// stored creation time (via Store.Lookup), not a fresh time.Now()
	// stamped after Mint returns -- pinned by exact equality against
	// the same field read back from List.
	if minted.CreatedAt.IsZero() || !minted.CreatedAt.Equal(listed.Tokens[0].CreatedAt) {
		t.Fatalf("mint CreatedAt = %v, want exactly listed CreatedAt %v",
			minted.CreatedAt, listed.Tokens[0].CreatedAt)
	}

	revokeResp := doJSON(
		t, "DELETE", baseURL+"/v1/tokens/"+minted.ID, "admin-token", "",
	)
	defer revokeResp.Body.Close()
	// Positive: revoking a known id returns 204.
	if revokeResp.StatusCode != http.StatusNoContent {
		t.Fatalf("revoke status = %d, want %d", revokeResp.StatusCode, http.StatusNoContent)
	}

	revokeUnknownResp := doJSON(
		t, "DELETE", baseURL+"/v1/tokens/does-not-exist", "admin-token", "",
	)
	defer revokeUnknownResp.Body.Close()
	// Negative: revoking an unknown id returns 404, not a silent 204.
	if revokeUnknownResp.StatusCode != http.StatusNotFound {
		t.Fatalf("revoke unknown status = %d, want %d",
			revokeUnknownResp.StatusCode, http.StatusNotFound)
	}
}

// mintedTokenResponse mirrors mintTokenResponse for decoding in tests
// that need to inspect the WorkerGroups scope.
type mintedTokenResponse struct {
	ID               string    `json:"id"`
	Token            string    `json:"token"`
	Label            string    `json:"label"`
	TaskTypePrefixes []string  `json:"task_type_prefixes"`
	WorkerGroups     []string  `json:"worker_groups"`
	CreatedAt        time.Time `json:"created_at"`
}

// TestTokensAPIMintWorkerGroupsAbsentOK proves an absent worker_groups
// field mints successfully and unscoped (#695) -- the pre-#695 request
// shape must keep working byte-for-byte.
func TestTokensAPIMintWorkerGroupsAbsentOK(t *testing.T) {
	baseURL := mintTokensTestFixture(t, "admin-token")
	resp := doJSON(t, "POST", baseURL+"/v1/tokens", "admin-token",
		`{"label":"worker-a","task_type_prefixes":["echo"]}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("mint status = %d, want %d", resp.StatusCode, http.StatusCreated)
	}
	var minted mintedTokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&minted); err != nil {
		t.Fatalf("decode mint response: %v", err)
	}
	if len(minted.WorkerGroups) != 0 {
		t.Fatalf("WorkerGroups = %v, want empty for an absent field",
			minted.WorkerGroups)
	}
}

// TestTokensAPIMintWorkerGroupsPresentButEmptyRefused pins the mint-
// time rejection of a present-but-empty worker_groups array (#695):
// absent means unscoped, but an explicit `[]` must never silently mean
// "every group".
func TestTokensAPIMintWorkerGroupsPresentButEmptyRefused(t *testing.T) {
	baseURL := mintTokensTestFixture(t, "admin-token")
	resp := doJSON(t, "POST", baseURL+"/v1/tokens", "admin-token",
		`{"label":"worker-a","task_type_prefixes":["echo"],"worker_groups":[]}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

// TestTokensAPIMintWorkerGroupsInvalidRefused proves a group value
// that could never appear on a real dispatch (a dot, forbidden by
// dag.ValidWorkerGroup) is refused at mint.
func TestTokensAPIMintWorkerGroupsInvalidRefused(t *testing.T) {
	baseURL := mintTokensTestFixture(t, "admin-token")
	resp := doJSON(t, "POST", baseURL+"/v1/tokens", "admin-token",
		`{"label":"worker-a","task_type_prefixes":["echo"],"worker_groups":["a.b"]}`)
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}
}

// TestTokensAPIMintListEchoesWorkerGroups proves a valid worker_groups
// scope round-trips through both the mint response and the listing.
func TestTokensAPIMintListEchoesWorkerGroups(t *testing.T) {
	baseURL := mintTokensTestFixture(t, "admin-token")
	mintResp := doJSON(t, "POST", baseURL+"/v1/tokens", "admin-token",
		`{"label":"worker-a","task_type_prefixes":["echo"],"worker_groups":["alpha"]}`)
	defer mintResp.Body.Close()
	if mintResp.StatusCode != http.StatusCreated {
		t.Fatalf("mint status = %d, want %d", mintResp.StatusCode, http.StatusCreated)
	}
	var minted mintedTokenResponse
	if err := json.NewDecoder(mintResp.Body).Decode(&minted); err != nil {
		t.Fatalf("decode mint response: %v", err)
	}
	if len(minted.WorkerGroups) != 1 || minted.WorkerGroups[0] != "alpha" {
		t.Fatalf("mint WorkerGroups = %v, want [alpha]", minted.WorkerGroups)
	}

	listResp := doJSON(t, "GET", baseURL+"/v1/tokens", "admin-token", "")
	defer listResp.Body.Close()
	var listed struct {
		Tokens []workertoken.Token `json:"tokens"`
	}
	if err := json.NewDecoder(listResp.Body).Decode(&listed); err != nil {
		t.Fatalf("decode list response: %v", err)
	}
	if len(listed.Tokens) != 1 ||
		len(listed.Tokens[0].WorkerGroups) != 1 ||
		listed.Tokens[0].WorkerGroups[0] != "alpha" {
		t.Fatalf("listed WorkerGroups = %+v, want [alpha]", listed.Tokens)
	}
}
