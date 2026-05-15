// auth_test.go exercises the four gating modes the console supports.
//
// Methodology:
//   - Resolver-level: ResolveAuthMode is a pure function; every branch
//     is covered by a table-driven test.
//   - Handler-level: each mode is wired into authMiddleware around a
//     no-op next-handler and exercised with net/http/httptest. The
//     loopback case asserts the Actor lands on the request context.
//     The forward case asserts X-Forwarded-User flows through. The
//     basic case asserts 401 without credentials and 200 with them.
//     The disabled case asserts the verbatim JSON body and 503.
//
// Why these and not more: the spec for PR 1 is exactly these branches.
// Read-only middleware, audit emission, etc. land in later PRs and get
// their own tests there.
package console

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestResolveAuthMode_table(t *testing.T) {
	cases := []struct {
		name string
		cfg  AuthConfig
		want AuthMode
		err  bool
	}{
		{
			name: "loopback ipv4",
			cfg:  AuthConfig{HTTPAddr: "127.0.0.1:8080"},
			want: AuthLoopback,
		},
		{
			name: "loopback ipv6",
			cfg:  AuthConfig{HTTPAddr: "[::1]:8080"},
			want: AuthLoopback,
		},
		{
			name: "loopback hostname",
			cfg:  AuthConfig{HTTPAddr: "localhost:8080"},
			want: AuthLoopback,
		},
		{
			name: "non-loopback bare port",
			cfg:  AuthConfig{HTTPAddr: ":8080"},
			want: AuthDisabled,
		},
		{
			name: "non-loopback all interfaces",
			cfg:  AuthConfig{HTTPAddr: "0.0.0.0:8080"},
			want: AuthDisabled,
		},
		{
			name: "non-loopback with forward auth",
			cfg:  AuthConfig{HTTPAddr: "0.0.0.0:8080", ForwardAuth: true},
			want: AuthForwarded,
		},
		{
			name: "non-loopback with password",
			cfg:  AuthConfig{HTTPAddr: "0.0.0.0:8080", Password: "x"},
			want: AuthBasic,
		},
		{
			name: "non-loopback with both flags set",
			cfg:  AuthConfig{HTTPAddr: "0.0.0.0:8080", ForwardAuth: true, Password: "x"},
			want: AuthDisabled,
			err:  true,
		},
		{
			name: "public ip not loopback",
			cfg:  AuthConfig{HTTPAddr: "8.8.8.8:8080"},
			want: AuthDisabled,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ResolveAuthMode(tc.cfg)
			if (err != nil) != tc.err {
				t.Fatalf("err mismatch: got %v, wantErr=%v", err, tc.err)
			}
			if got != tc.want {
				t.Fatalf("mode = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestAuthMiddleware_loopback_attachesActor(t *testing.T) {
	var seen Actor
	var ok bool
	h := authMiddleware(AuthLoopback, "", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			seen, ok = ActorFrom(r.Context())
			w.WriteHeader(http.StatusOK)
		}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !ok {
		t.Fatalf("expected actor to be attached")
	}
	if seen.Source != AuthLoopback {
		t.Fatalf("source = %s, want loopback", seen.Source)
	}
	if seen.Display() != "loopback" {
		t.Fatalf("display = %q, want loopback", seen.Display())
	}
}

func TestAuthMiddleware_forwarded_readsHeaders(t *testing.T) {
	var seen Actor
	h := authMiddleware(AuthForwarded, "", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			seen, _ = ActorFrom(r.Context())
			w.WriteHeader(http.StatusOK)
		}))

	req := httptest.NewRequest(http.MethodGet, "/console/", nil)
	req.Header.Set("X-Forwarded-User", "alice@example.com")
	req.Header.Set("X-Forwarded-Email", "alice@example.com")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if seen.User != "alice@example.com" {
		t.Fatalf("user = %q, want alice@example.com", seen.User)
	}
	if seen.Source != AuthForwarded {
		t.Fatalf("source = %s, want forward-auth", seen.Source)
	}
	if seen.Display() != "alice@example.com" {
		t.Fatalf("display = %q, want alice@example.com", seen.Display())
	}
}

func TestAuthMiddleware_basic_rejectsAndAccepts(t *testing.T) {
	const password = "supersecret-min-16-chars"
	h := authMiddleware(AuthBasic, password, http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

	noCreds := httptest.NewRecorder()
	h.ServeHTTP(noCreds, httptest.NewRequest(http.MethodGet, "/console/", nil))
	if noCreds.Code != http.StatusUnauthorized {
		t.Fatalf("no-creds status = %d, want 401", noCreds.Code)
	}
	if hdr := noCreds.Header().Get("WWW-Authenticate"); hdr == "" {
		t.Fatalf("missing WWW-Authenticate header on 401")
	}

	bad := httptest.NewRequest(http.MethodGet, "/console/", nil)
	bad.SetBasicAuth("admin", "wrong-password")
	badRR := httptest.NewRecorder()
	h.ServeHTTP(badRR, bad)
	if badRR.Code != http.StatusUnauthorized {
		t.Fatalf("bad-creds status = %d, want 401", badRR.Code)
	}

	good := httptest.NewRequest(http.MethodGet, "/console/", nil)
	good.SetBasicAuth("admin", password)
	goodRR := httptest.NewRecorder()
	h.ServeHTTP(goodRR, good)
	if goodRR.Code != http.StatusOK {
		t.Fatalf("good-creds status = %d, want 200", goodRR.Code)
	}
}

func TestAuthMiddleware_disabled_returnsJSON(t *testing.T) {
	// next-handler must not run when disabled — the gate short-circuits.
	called := false
	h := authMiddleware(AuthDisabled, "", http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) { called = true }))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/", nil))

	if called {
		t.Fatalf("next-handler must not run when console is disabled")
	}
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", rr.Code)
	}
	if ct := rr.Header().Get("Content-Type"); !strings.HasPrefix(ct, "application/json") {
		t.Fatalf("content-type = %q, want JSON", ct)
	}
	body, err := io.ReadAll(rr.Body)
	if err != nil {
		t.Fatalf("read body: %v", err)
	}
	var parsed struct {
		Error   string `json:"error"`
		Message string `json:"message"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		t.Fatalf("unmarshal body: %v: %s", err, body)
	}
	if parsed.Error != "console_disabled" {
		t.Fatalf("error = %q, want console_disabled", parsed.Error)
	}
	if !strings.Contains(parsed.Message, "DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH") {
		t.Fatalf("message missing forward-auth flag: %q", parsed.Message)
	}
	if !strings.Contains(parsed.Message, "DAGNATS_CONSOLE_PASSWORD") {
		t.Fatalf("message missing password flag: %q", parsed.Message)
	}
}
