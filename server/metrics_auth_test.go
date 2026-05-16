// server/metrics_auth_test.go
// Methodology: table-driven tests over the four MetricsAuthMode
// values. Each mode asserts both an auth-pass (200) and an auth-fail
// (401) path so the gate's positive and negative space are covered.
// httptest gives us a real http.Handler chain without spinning up
// the full Server.
package server

import (
	"encoding/base64"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// okHandler is the "underlying" handler the gate protects.
func okHandler() http.Handler {
	return http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		_, _ = io.WriteString(w, "metrics-body")
	})
}

func basicHeader(user, pass string) string {
	creds := user + ":" + pass
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(creds))
}

func TestMetricsAuthLoopbackAllowsLocalhost(t *testing.T) {
	h := metricsAuthMiddleware(
		MetricsAuthConfig{Mode: MetricsAuthLoopback}, okHandler(),
	)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.RemoteAddr = "127.0.0.1:5555"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("loopback 127.0.0.1: expected 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "metrics-body") {
		t.Fatalf("body missing payload: %q", rec.Body.String())
	}
}

func TestMetricsAuthLoopbackRejectsRemote(t *testing.T) {
	h := metricsAuthMiddleware(
		MetricsAuthConfig{Mode: MetricsAuthLoopback}, okHandler(),
	)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.RemoteAddr = "10.1.2.3:5555"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("loopback remote: expected 401, got %d", rec.Code)
	}
}

func TestMetricsAuthBasicAcceptsValidCreds(t *testing.T) {
	cfg := MetricsAuthConfig{
		Mode: MetricsAuthBasic, BasicUser: "u", BasicPass: "p",
	}
	h := metricsAuthMiddleware(cfg, okHandler())
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", basicHeader("u", "p"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("basic good creds: expected 200, got %d", rec.Code)
	}
}

func TestMetricsAuthBasicRejectsBadCreds(t *testing.T) {
	cfg := MetricsAuthConfig{
		Mode: MetricsAuthBasic, BasicUser: "u", BasicPass: "p",
	}
	h := metricsAuthMiddleware(cfg, okHandler())
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", basicHeader("u", "wrong"))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("basic bad creds: expected 401, got %d", rec.Code)
	}
	if !strings.Contains(
		rec.Header().Get("WWW-Authenticate"), "Basic",
	) {
		t.Fatal("missing WWW-Authenticate challenge")
	}
}

func TestMetricsAuthBasicRejectsMissingCreds(t *testing.T) {
	cfg := MetricsAuthConfig{
		Mode: MetricsAuthBasic, BasicUser: "u", BasicPass: "p",
	}
	h := metricsAuthMiddleware(cfg, okHandler())
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("basic missing: expected 401, got %d", rec.Code)
	}
}

func TestMetricsAuthBasicFailsClosedOnEmptyCreds(t *testing.T) {
	// Operator set basic but forgot the user/pass — must 401, never
	// pass any caller through with empty stored creds.
	cfg := MetricsAuthConfig{Mode: MetricsAuthBasic}
	h := metricsAuthMiddleware(cfg, okHandler())
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", basicHeader("", ""))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("basic empty cfg: expected 401, got %d", rec.Code)
	}
}

func TestMetricsAuthForwardAcceptsHeader(t *testing.T) {
	cfg := MetricsAuthConfig{Mode: MetricsAuthForward}
	h := metricsAuthMiddleware(cfg, okHandler())
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("X-Forwarded-User", "scraper@example.com")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("forward with header: expected 200, got %d", rec.Code)
	}
}

func TestMetricsAuthForwardRejectsMissingHeader(t *testing.T) {
	cfg := MetricsAuthConfig{Mode: MetricsAuthForward}
	h := metricsAuthMiddleware(cfg, okHandler())
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("forward missing: expected 401, got %d", rec.Code)
	}
}

func TestMetricsAuthNoneAllowsAll(t *testing.T) {
	cfg := MetricsAuthConfig{Mode: MetricsAuthNone}
	h := metricsAuthMiddleware(cfg, okHandler())
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.RemoteAddr = "10.1.2.3:5555"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("none: expected 200, got %d", rec.Code)
	}
}

func TestMetricsAuthUnknownModeFailsClosed(t *testing.T) {
	cfg := MetricsAuthConfig{Mode: MetricsAuthMode("oops")}
	h := metricsAuthMiddleware(cfg, okHandler())
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.RemoteAddr = "10.1.2.3:5555"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("unknown mode: expected 401, got %d", rec.Code)
	}
}

func TestMetricsAuthIPv6Loopback(t *testing.T) {
	h := metricsAuthMiddleware(
		MetricsAuthConfig{Mode: MetricsAuthLoopback}, okHandler(),
	)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.RemoteAddr = "[::1]:5555"
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("[::1]: expected 200, got %d", rec.Code)
	}
}

func TestLoadMetricsAuthConfigDefaultsToLoopback(t *testing.T) {
	t.Setenv("METRICS_AUTH", "")
	cfg := LoadMetricsAuthConfigFromEnv(nil)
	if cfg.Mode != MetricsAuthLoopback {
		t.Fatalf("default: expected loopback, got %q", cfg.Mode)
	}
}

func TestLoadMetricsAuthConfigParsesValues(t *testing.T) {
	t.Setenv("METRICS_AUTH", "basic")
	t.Setenv("METRICS_BASIC_USER", "scraper")
	t.Setenv("METRICS_BASIC_PASS", "shh")
	cfg := LoadMetricsAuthConfigFromEnv(nil)
	if cfg.Mode != MetricsAuthBasic {
		t.Fatalf("expected basic, got %q", cfg.Mode)
	}
	if cfg.BasicUser != "scraper" {
		t.Fatalf("user: %q", cfg.BasicUser)
	}
	if cfg.BasicPass != "shh" {
		t.Fatalf("pass: %q", cfg.BasicPass)
	}
}

func TestLoadMetricsAuthConfigUnknownModeWarnsAndDefaults(t *testing.T) {
	t.Setenv("METRICS_AUTH", "wrongmode")
	cfg := LoadMetricsAuthConfigFromEnv(nil)
	if cfg.Mode != MetricsAuthLoopback {
		t.Fatalf("expected loopback fallback, got %q", cfg.Mode)
	}
}
