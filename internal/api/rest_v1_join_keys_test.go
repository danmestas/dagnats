// rest_v1_join_keys_test.go
// Tests for the admin-only /v1/programs/join-keys routes (#885): create
// returns the plaintext key once, list never carries a secret or hash,
// revoke takes effect at once, and the admin gate fails closed (503
// with no admin token, 401 on a wrong one).
// Methodology: real embedded NATS, real joinkey.Store, httptest,
// t.Setenv for the admin token so each test controls its own value.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/joinkey"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func joinKeysTestFixture(
	t *testing.T, adminToken string,
) (baseURL string, store *joinkey.Store) {
	t.Helper()
	t.Setenv("DAGNATS_BRIDGE_TOKEN", adminToken)
	_, nc := natstest.StartServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store, err = joinkey.Open(ctx, js)
	if err != nil {
		t.Fatalf("joinkey.Open: %v", err)
	}
	t.Cleanup(store.Close)
	mux := http.NewServeMux()
	MountJoinKeys(mux, store)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return server.URL + "/v1/programs/join-keys", store
}

func readBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer func() {
		if err := resp.Body.Close(); err != nil {
			t.Errorf("close body: %v", err)
		}
	}()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	return string(data)
}

func TestJoinKeysCreateListRevoke(t *testing.T) {
	url, store := joinKeysTestFixture(t, "admin-secret")
	resp := doJSON(t, http.MethodPost, url, "admin-secret",
		`{"label":"deploys","name_glob":"artifact*","uses_max":3}`)
	body := readBody(t, resp)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, body %s", resp.StatusCode, body)
	}
	var created map[string]any
	if err := json.Unmarshal([]byte(body), &created); err != nil {
		t.Fatalf("decode create: %v", err)
	}
	plaintext, _ := created["key"].(string)
	id, _ := created["id"].(string)
	if !strings.HasPrefix(plaintext, "djk_"+id+"_") || id == "" {
		t.Fatalf("create body %s lacks id/key", body)
	}
	if created["name_glob"] != "artifact*" || created["uses_max"] != float64(3) ||
		created["uses_left"] != float64(3) || created["created_by"] != "admin" {
		t.Fatalf("create body %s does not echo the stored record", body)
	}
	if _, leaked := created["secret_hash"]; leaked {
		t.Fatalf("create body carries the hash: %s", body)
	}
	if _, err := store.Verify(plaintext, "artifactstore"); err != nil {
		t.Fatalf("created key does not verify: %v", err)
	}
	secretPart := plaintext[strings.LastIndex(plaintext, "_")+1:]

	listBody := readBody(t, doJSON(t, http.MethodGet, url, "admin-secret", ""))
	if !strings.Contains(listBody, `"id":"`+id+`"`) {
		t.Fatalf("list %s lacks the key", listBody)
	}
	if strings.Contains(listBody, secretPart) || strings.Contains(listBody, "secret_hash") ||
		strings.Contains(listBody, `"key"`) {
		t.Fatalf("list leaks the secret or hash: %s", listBody)
	}

	resp = doJSON(t, http.MethodDelete, url+"/"+id, "admin-secret", "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("revoke status = %d, body %s", resp.StatusCode, readBody(t, resp))
	}
	if _, err := store.Verify(plaintext, "artifactstore"); !errors.Is(err, joinkey.ErrRevoked) {
		t.Fatalf("Verify after REST revoke err = %v, want ErrRevoked", err)
	}
	resp = doJSON(t, http.MethodDelete, url+"/nosuchid", "admin-secret", "")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("revoke unknown status = %d, want 404", resp.StatusCode)
	}
}

func TestJoinKeysCreateRefusesBadSpec(t *testing.T) {
	url, store := joinKeysTestFixture(t, "admin-secret")
	bodies := []string{
		`{"label":""}`,
		`{"label":"x","name_glob":"[a-z]"}`,
		`{"label":"x","uses_max":-1}`,
		`{"label":"x","expires_at":"2001-01-01T00:00:00Z"}`,
		`{"label":"x","unknown_field":1}`,
		`not json`,
	}
	for _, body := range bodies {
		resp := doJSON(t, http.MethodPost, url, "admin-secret", body)
		if resp.StatusCode != http.StatusBadRequest {
			t.Errorf("create %s status = %d, want 400", body, resp.StatusCode)
		}
		readBody(t, resp)
	}
	if got := store.List(); len(got) != 0 {
		t.Fatalf("refused creates stored %+v", got)
	}
}

func TestJoinKeysAdminGate(t *testing.T) {
	url, store := joinKeysTestFixture(t, "admin-secret")
	requests := []struct{ method, path, body string }{
		{http.MethodPost, "", `{"label":"x"}`},
		{http.MethodGet, "", ""},
		{http.MethodDelete, "/someid", ""},
	}
	for _, req := range requests {
		for _, bearer := range []string{"", "wrong"} {
			resp := doJSON(t, req.method, url+req.path, bearer, req.body)
			if resp.StatusCode != http.StatusUnauthorized {
				t.Errorf("%s bearer=%q status = %d, want 401",
					req.method, bearer, resp.StatusCode)
			}
			readBody(t, resp)
		}
	}
	if got := store.List(); len(got) != 0 {
		t.Fatalf("unauthorized create stored %+v", got)
	}

	closedURL, _ := joinKeysTestFixture(t, "")
	resp := doJSON(t, http.MethodGet, closedURL, "anything", "")
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("no admin token: status = %d, want 503", resp.StatusCode)
	}
	readBody(t, resp)
}
