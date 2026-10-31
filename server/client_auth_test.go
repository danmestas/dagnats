// Test methodology: unit tests for secret files and the HTTP token gate,
// and integration tests with a real embedded NATS server per test for the
// client login. Each case checks the refused path as well as the allowed one.

package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func writeSecret(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write secret: %v", err)
	}
	return path
}

func TestReadSecretFile(t *testing.T) {
	got, err := readSecretFile(writeSecret(t, "  s3cret-value\n"))
	if err != nil || got != "s3cret-value" {
		t.Fatalf("readSecretFile = %q, %v; want trimmed value", got, err)
	}
	if _, err := readSecretFile(writeSecret(t, " \n")); err == nil {
		t.Error("an empty secret file was accepted")
	}
	big := strings.Repeat("x", secretFileBytesMax+1)
	if _, err := readSecretFile(writeSecret(t, big)); err == nil {
		t.Error("an oversized secret file was accepted")
	}
	missing := filepath.Join(t.TempDir(), "absent")
	if _, err := readSecretFile(missing); err == nil {
		t.Error("a missing secret file was accepted")
	}
}

func TestResolveClientAuth(t *testing.T) {
	cfg := DefaultConfig()
	if err := resolveClientAuth(&cfg); err != nil {
		t.Fatalf("no auth configured: %v", err)
	}
	if cfg.natsPassword != "" || cfg.apiToken != "" {
		t.Fatal("secrets resolved with nothing configured")
	}

	cfg = DefaultConfig()
	cfg.NATSUser = "dagnats"
	if err := resolveClientAuth(&cfg); err == nil {
		t.Error("nats_user without nats_password_file was accepted")
	}
	cfg = DefaultConfig()
	cfg.NATSPasswordFile = writeSecret(t, "pw")
	if err := resolveClientAuth(&cfg); err == nil {
		t.Error("nats_password_file without nats_user was accepted")
	}

	cfg = DefaultConfig()
	cfg.NATSUser = "dagnats"
	cfg.NATSPasswordFile = writeSecret(t, "pw\n")
	cfg.APITokenFile = writeSecret(t, "tok\n")
	if err := resolveClientAuth(&cfg); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	if cfg.natsPassword != "pw" || cfg.apiToken != "tok" {
		t.Fatalf("resolved %q / %q", cfg.natsPassword, cfg.apiToken)
	}
}

func TestApplyEnvOverrides_ClientAuth(t *testing.T) {
	t.Setenv("DAGNATS_NATS_USER", "dagnats")
	t.Setenv("DAGNATS_NATS_PASSWORD_FILE", "/run/pw")
	t.Setenv("DAGNATS_API_TOKEN_FILE", "/run/tok")
	cfg := DefaultConfig()
	applyEnvOverrides(&cfg)
	if cfg.NATSUser != "dagnats" || cfg.NATSPasswordFile != "/run/pw" ||
		cfg.APITokenFile != "/run/tok" {
		t.Fatalf("env not applied: %q %q %q",
			cfg.NATSUser, cfg.NATSPasswordFile, cfg.APITokenFile)
	}
	file := DefaultConfig()
	for key, val := range map[string]string{
		"nats_user": "u", "nats_password_file": "/p", "api_token_file": "/t",
	} {
		if err := applyConfigValue(key, val, 1, &file); err != nil {
			t.Fatalf("%s: %v", key, err)
		}
	}
	if file.NATSUser != "u" || file.NATSPasswordFile != "/p" || file.APITokenFile != "/t" {
		t.Fatal("config-file keys not applied")
	}
}

// The embedded server refuses anonymous clients once a login is set, and
// dagnats' own connection (startNATSAndConnect) logs in with it.
func TestStartNATSAndConnect_RequiresTheClientLogin(t *testing.T) {
	cfg := Config{
		DataDir:          t.TempDir(),
		HTTPAddr:         ":8080",
		NATSPort:         -1,
		MaxStoreBytes:    1 << 30,
		NATSUser:         "dagnats",
		NATSPasswordFile: writeSecret(t, "pw-for-test"),
	}
	if err := resolveClientAuth(&cfg); err != nil {
		t.Fatalf("resolve: %v", err)
	}
	ns, nc, err := startNATSAndConnect(cfg, nil)
	if err != nil {
		t.Fatalf("dagnats' own connection failed: %v", err)
	}
	defer ns.Shutdown()
	defer nc.Close()

	if anon, err := nats.Connect(ns.ClientURL()); err == nil {
		anon.Close()
		t.Fatal("an anonymous client connected")
	}
	wrong, err := nats.Connect(ns.ClientURL(), nats.UserInfo("dagnats", "nope"))
	if err == nil {
		wrong.Close()
		t.Fatal("a wrong password connected")
	}
	right, err := nats.Connect(ns.ClientURL(), nats.UserInfo("dagnats", "pw-for-test"))
	if err != nil {
		t.Fatalf("the right login was refused: %v", err)
	}
	right.Close()
}

func TestStartNATS_OpenWithoutALogin(t *testing.T) {
	cfg := Config{DataDir: t.TempDir(), HTTPAddr: ":8080", NATSPort: -1, MaxStoreBytes: 1 << 30}
	ns, err := startNATS(cfg)
	if err != nil {
		t.Fatalf("startNATS: %v", err)
	}
	defer ns.Shutdown()
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("anonymous client refused with no login configured: %v", err)
	}
	nc.Close()
}

func TestAPITokenGate(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	gated := apiTokenGate("tok-123", nil, ok)
	cases := []struct {
		path, auth string
		want       int
	}{
		{"/runs", "", http.StatusUnauthorized},
		{"/runs", "Bearer wrong", http.StatusUnauthorized},
		{"/runs", "tok-123", http.StatusUnauthorized},
		{"/runs", "Bearer tok-123", http.StatusTeapot},
		{"/runs", "bearer tok-123", http.StatusTeapot},
		{"/v1/tasks/poll", "", http.StatusUnauthorized},
		{"/console/", "", http.StatusUnauthorized},
		{"/health", "", http.StatusTeapot},
		{"/health/cluster", "", http.StatusTeapot},
		{"/ready", "", http.StatusTeapot},
		{"/healthz", "", http.StatusUnauthorized},
	}
	for _, c := range cases {
		req := httptest.NewRequest(http.MethodGet, c.path, nil)
		if c.auth != "" {
			req.Header.Set("Authorization", c.auth)
		}
		rec := httptest.NewRecorder()
		gated.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s with %q: status %d, want %d", c.path, c.auth, rec.Code, c.want)
		}
	}
	if apiTokenGate("", nil, ok) == nil {
		t.Fatal("no gate returned for an empty token")
	}
	rec := httptest.NewRecorder()
	apiTokenGate("", nil, ok).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/runs", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("with no token configured the gate blocked: %d", rec.Code)
	}
}

// The whole server, not the pieces: with both logins set, the HTTP API,
// the client port and the WebSocket listener all refuse an anonymous
// caller, and the health probes still answer. Removing the wiring in
// startComponents or startHTTP fails this test.
func TestServer_EnforcesBothLogins(t *testing.T) {
	cfg := testConfig(t)
	cfg.NATSUser = "dagnats"
	cfg.NATSPasswordFile = writeSecret(t, "server-pw")
	cfg.APITokenFile = writeSecret(t, "server-token")
	cfg.NATSWebsocketPort = freeTCPPort(t)
	cfg.NATSWebsocketNoTLS = true
	srv := New(cfg)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()
	defer func() {
		srv.Stop()
		select {
		case <-errCh:
		case <-time.After(20 * time.Second):
			t.Fatal("Run() did not return within 20s")
		}
	}()
	httpAddr := waitHTTPAddr(t, srv)

	status := func(path, auth string) int {
		req, err := http.NewRequest(http.MethodGet, "http://"+httpAddr+path, nil)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if got := status("/workflows", ""); got != http.StatusUnauthorized {
		t.Errorf("/workflows without a token: %d, want 401", got)
	}
	if got := status("/workflows", "Bearer server-token"); got != http.StatusOK {
		t.Errorf("/workflows with the token: %d, want 200", got)
	}
	if got := status("/ready", ""); got != http.StatusOK {
		t.Errorf("/ready without a token: %d, want 200", got)
	}

	clientURL := srv.ns.ClientURL()
	if nc, err := nats.Connect(clientURL); err == nil {
		nc.Close()
		t.Error("an anonymous client connected to the client port")
	}
	wsURL := fmt.Sprintf("ws://127.0.0.1:%d", cfg.NATSWebsocketPort)
	if nc, err := nats.Connect(wsURL); err == nil {
		nc.Close()
		t.Error("an anonymous client connected over WebSocket")
	}
	nc, err := nats.Connect(wsURL, nats.UserInfo("dagnats", "server-pw"))
	if err != nil {
		t.Fatalf("the login was refused over WebSocket: %v", err)
	}
	nc.Close()
}

func TestReadSecretFile_DoesNotReadPastTheLimit(t *testing.T) {
	if _, err := os.Stat("/dev/zero"); err != nil {
		t.Skip("no /dev/zero")
	}
	done := make(chan error, 1)
	go func() { _, err := readSecretFile("/dev/zero"); done <- err }()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("/dev/zero was accepted as a secret")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("readSecretFile kept reading an endless file")
	}
}

// A bridge worker presents its own worker token, which the API token does
// not match. The gate lets it through on the bridge's worker routes when
// the bridge accepts it, and nowhere else.
func TestAPITokenGate_BridgeWorkerToken(t *testing.T) {
	ok := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})
	workerAuth := func(header string) bool { return header == "Bearer worker-tok" }
	gated := apiTokenGate("tok-123", workerAuth, ok)
	cases := []struct {
		method, path, auth string
		want               int
	}{
		{http.MethodPost, "/v1/tasks/poll", "Bearer worker-tok", http.StatusTeapot},
		{http.MethodPost, "/v1/workers/connect", "Bearer worker-tok", http.StatusTeapot},
		{http.MethodPost, "/v1/tasks/t-1/resolve", "Bearer worker-tok", http.StatusTeapot},
		{http.MethodPost, "/v1/tasks/t-1/logs", "Bearer worker-tok", http.StatusTeapot},
		{http.MethodPost, "/v1/tasks/poll", "Bearer tok-123", http.StatusTeapot},
		{http.MethodPost, "/v1/tasks/poll", "Bearer other", http.StatusUnauthorized},
		{http.MethodPost, "/v1/tasks/poll", "", http.StatusUnauthorized},
		{http.MethodGet, "/v1/workers", "Bearer worker-tok", http.StatusUnauthorized},
		{http.MethodPost, "/v1/tokens", "Bearer worker-tok", http.StatusUnauthorized},
		{http.MethodGet, "/runs", "Bearer worker-tok", http.StatusUnauthorized},
		{http.MethodGet, "/v1/tasks/poll", "Bearer worker-tok", http.StatusUnauthorized},
	}
	for _, c := range cases {
		req := httptest.NewRequest(c.method, c.path, nil)
		if c.auth != "" {
			req.Header.Set("Authorization", c.auth)
		}
		rec := httptest.NewRecorder()
		gated.ServeHTTP(rec, req)
		if rec.Code != c.want {
			t.Errorf("%s %s with %q: status %d, want %d",
				c.method, c.path, c.auth, rec.Code, c.want)
		}
	}
}

// The whole server again: with an API token set, a bridge worker that
// presents the bridge's own token still polls, and the API token still
// guards everything else. Removing bridgeWorkerAuth from startHTTP fails
// this test with a 401 on the poll.
func TestServer_APITokenAdmitsBridgeWorkerToken(t *testing.T) {
	t.Setenv("DAGNATS_BRIDGE_TOKEN", "bridge-admin")
	cfg := testConfig(t)
	cfg.APITokenFile = writeSecret(t, "server-token")
	srv := New(cfg)
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Run() }()
	defer func() {
		srv.Stop()
		select {
		case <-errCh:
		case <-time.After(20 * time.Second):
			t.Fatal("Run() did not return within 20s")
		}
	}()
	httpAddr := waitHTTPAddr(t, srv)

	status := func(method, path, auth, body string) int {
		req, err := http.NewRequest(method, "http://"+httpAddr+path, strings.NewReader(body))
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		req.Header.Set("Content-Type", "application/json")
		if auth != "" {
			req.Header.Set("Authorization", auth)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("%s %s: %v", method, path, err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	poll := `{"task_types":["echo"],"timeout_ms":100}`
	if got := status(http.MethodPost, "/v1/tasks/poll", "Bearer bridge-admin", poll); got != http.StatusOK {
		t.Errorf("poll with the bridge token: %d, want 200", got)
	}
	if got := status(http.MethodPost, "/v1/tasks/poll", "", poll); got != http.StatusUnauthorized {
		t.Errorf("poll without a token: %d, want 401", got)
	}
	if got := status(http.MethodGet, "/workflows", "Bearer bridge-admin", ""); got != http.StatusUnauthorized {
		t.Errorf("/workflows with the bridge token: %d, want 401", got)
	}
}
