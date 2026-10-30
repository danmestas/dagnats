// Test methodology: unit tests for secret files and the HTTP token gate,
// and integration tests with a real embedded NATS server per test for the
// client login. Each case checks the refused path as well as the allowed one.

package server

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

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
	gated := apiTokenGate("tok-123", ok)
	cases := []struct {
		path, auth string
		want       int
	}{
		{"/runs", "", http.StatusUnauthorized},
		{"/runs", "Bearer wrong", http.StatusUnauthorized},
		{"/runs", "tok-123", http.StatusUnauthorized},
		{"/runs", "Bearer tok-123", http.StatusTeapot},
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
	if apiTokenGate("", ok) == nil {
		t.Fatal("no gate returned for an empty token")
	}
	rec := httptest.NewRecorder()
	apiTokenGate("", ok).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/runs", nil))
	if rec.Code != http.StatusTeapot {
		t.Fatalf("with no token configured the gate blocked: %d", rec.Code)
	}
}
