// program_join_key_test.go
// Round-trip tests for `dagnats program join-key create|list|revoke`
// against the real admin REST routes: the plaintext key is printed by
// create only, list (text and --json) never prints it, revoke shows up
// in the next list, and a wrong admin token surfaces as an error.
// Methodology: real embedded NATS + joinkey.Store behind httptest, the
// command functions driven with captured output.
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/joinkey"
	"github.com/danmestas/dagnats/internal/natstest"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func joinKeyCLIFixture(t *testing.T) (adminClient, *joinkey.Store) {
	t.Helper()
	t.Setenv("DAGNATS_BRIDGE_TOKEN", "admin-secret")
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
	store, err := joinkey.Open(ctx, js)
	if err != nil {
		t.Fatalf("joinkey.Open: %v", err)
	}
	t.Cleanup(store.Close)
	mux := http.NewServeMux()
	api.MountJoinKeys(mux, store)
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	return adminClient{baseURL: server.URL, token: "admin-secret"}, store
}

func TestJoinKeyCLIRoundTrip(t *testing.T) {
	client, store := joinKeyCLIFixture(t)
	var out bytes.Buffer
	args := []string{"--label", "deploys", "--names", "artifact*", "--uses", "2",
		"--expires", "7d"}
	if err := joinKeyCreate(&out, client, args); err != nil {
		t.Fatalf("create: %v", err)
	}
	plaintext := ""
	for _, field := range strings.Fields(out.String()) {
		if strings.HasPrefix(field, "djk_") {
			plaintext = field
		}
	}
	key, err := store.Verify(plaintext, "artifactstore")
	if err != nil {
		t.Fatalf("printed key %q does not verify: %v\n%s", plaintext, err, out.String())
	}
	if key.UsesMax != 2 || key.NameGlob != "artifact*" || key.ExpiresAt == nil ||
		time.Until(*key.ExpiresAt) < 6*24*time.Hour {
		t.Fatalf("stored key %+v does not match the flags", key)
	}
	secretPart := plaintext[strings.LastIndex(plaintext, "_")+1:]

	for _, listArgs := range [][]string{{}, {"--json"}} {
		out.Reset()
		if err := joinKeyList(&out, client, listArgs); err != nil {
			t.Fatalf("list %v: %v", listArgs, err)
		}
		if !strings.Contains(out.String(), key.ID) || strings.Contains(out.String(), secretPart) {
			t.Fatalf("list %v: want id, never secret:\n%s", listArgs, out.String())
		}
	}

	out.Reset()
	if err := joinKeyRevoke(&out, client, []string{key.ID}); err != nil {
		t.Fatalf("revoke: %v", err)
	}
	if _, err := store.Verify(plaintext, "artifactstore"); !errors.Is(err, joinkey.ErrRevoked) {
		t.Fatalf("Verify after CLI revoke err = %v, want ErrRevoked", err)
	}
	out.Reset()
	if err := joinKeyList(&out, client, []string{"--json"}); err != nil {
		t.Fatalf("list after revoke: %v", err)
	}
	var listed []joinkey.JoinKey
	if err := json.Unmarshal(out.Bytes(), &listed); err != nil {
		t.Fatalf("decode list --json: %v\n%s", err, out.String())
	}
	if len(listed) != 1 || listed[0].RevokedAt == nil {
		t.Fatalf("list after revoke = %+v, want one revoked key", listed)
	}
}

func TestJoinKeyCLIErrors(t *testing.T) {
	client, store := joinKeyCLIFixture(t)
	var out bytes.Buffer
	if err := joinKeyRevoke(&out, client, []string{"nosuchid"}); err == nil ||
		!strings.Contains(err.Error(), "404") {
		t.Fatalf("revoke unknown err = %v, want a 404", err)
	}
	if err := joinKeyRevoke(&out, client, nil); err == nil {
		t.Fatal("revoke with no id succeeded")
	}
	for _, bad := range [][]string{{}, {"--label", "x", "--expires", "soon"},
		{"--label", "x", "--uses", "-3"}} {
		if err := joinKeyCreate(&out, client, bad); err == nil {
			t.Errorf("create %v succeeded", bad)
		}
	}
	wrong := adminClient{baseURL: client.baseURL, token: "wrong"}
	if err := joinKeyList(&out, wrong, nil); err == nil ||
		!strings.Contains(err.Error(), "401") {
		t.Fatalf("list with wrong token err = %v, want a 401", err)
	}
	if got := store.List(); len(got) != 0 {
		t.Fatalf("failed commands stored keys: %+v", got)
	}
}

func TestParseExpiry(t *testing.T) {
	cases := map[string]time.Duration{"90m": 90 * time.Minute, "7d": 7 * 24 * time.Hour}
	for text, want := range cases {
		got, err := parseExpiry(text)
		if err != nil || got != want {
			t.Errorf("parseExpiry(%q) = %v, %v; want %v", text, got, err, want)
		}
	}
	for _, bad := range []string{"", "0", "-1h", "d", "1.5d", "soon"} {
		if _, err := parseExpiry(bad); err == nil {
			t.Errorf("parseExpiry(%q) accepted", bad)
		}
	}
}
