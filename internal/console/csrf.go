package console

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
)

// csrf.go owns the lightweight CSRF protection the console applies
// when the resolved auth mode is forward-auth or basic-auth. The
// stop-condition in the PR brief explicitly scopes loopback out — a
// loopback bind has no session boundary the token could bind to, so
// the token would only add friction without protecting anything.
//
// Token shape: HMAC-SHA256 over the actor identity + a server-side
// secret. The secret comes from CONSOLE_CSRF_SECRET; absent / empty
// values trigger a random secret generated at startup with a slog.Warn
// telling the operator to set the env var for stability across
// restarts.
//
// The middleware is defence-in-depth: same-origin policy + the
// authenticated context already prevent the obvious CSRF attack. The
// HMAC token raises the bar so a CSRF requires reading the operator's
// page (not just guessing form actions). For loopback, that bar is
// the OS access-control already in place.

// CSRFSecret holds the active server secret. Set once at startup via
// LoadCSRFSecret; subsequent reads are concurrent-safe (string assign
// + read is atomic on Go's GC, and we never mutate after init).
var csrfSecret []byte

// csrfSecretLen is the byte length used when CONSOLE_CSRF_SECRET is
// unset and we generate a fresh secret. 32 bytes = 256 bits of entropy,
// the same length sha256 outputs.
const csrfSecretLen = 32

// LoadCSRFSecret reads CONSOLE_CSRF_SECRET (or the given override) and
// installs the active secret. Returns (secret, generated, error) where
// generated=true means we fell back to a random secret because no
// env var was set — the caller logs a warning so operators see that
// restarts will rotate the secret unless they set the variable.
func LoadCSRFSecret(envValue string) ([]byte, bool, error) {
	if envValue != "" {
		csrfSecret = []byte(envValue)
		return csrfSecret, false, nil
	}
	buf := make([]byte, csrfSecretLen)
	if _, err := rand.Read(buf); err != nil {
		return nil, false, err
	}
	csrfSecret = buf
	return csrfSecret, true, nil
}

// LoadCSRFSecretFromEnv is the entry the server startup path calls.
// Centralised so test helpers can hand it a fake env value without
// importing os in the production wiring.
func LoadCSRFSecretFromEnv() ([]byte, bool, error) {
	return LoadCSRFSecret(os.Getenv("CONSOLE_CSRF_SECRET"))
}

// CSRFTokenForActor returns the HMAC token for one actor identity.
// Empty actor means "loopback" — the function returns "" so handlers
// that don't apply CSRF on loopback can short-circuit without a
// branch on the actor type.
func CSRFTokenForActor(a Actor) string {
	if len(csrfSecret) == 0 {
		// Misconfiguration: serve a stable but non-validating token.
		// The middleware's verifyCSRF returns false on empty secrets,
		// so this just keeps the form rendering testable.
		return ""
	}
	if a.Display() == "loopback" {
		return ""
	}
	mac := hmac.New(sha256.New, csrfSecret)
	_, _ = mac.Write([]byte(a.Display()))
	return hex.EncodeToString(mac.Sum(nil))
}

// VerifyCSRFToken reports whether token is the HMAC for the given
// actor. Constant-time compare via hmac.Equal. Empty secret or empty
// token always fails.
func VerifyCSRFToken(a Actor, token string) bool {
	if len(csrfSecret) == 0 || token == "" {
		return false
	}
	want := CSRFTokenForActor(a)
	if want == "" {
		return false
	}
	return hmac.Equal([]byte(want), []byte(token))
}

// csrfMiddleware wraps next so every state-changing request under
// /console/* carries a valid CSRF token. The middleware is a no-op
// when the resolved auth mode is loopback (no session identity to
// bind to) or AuthDisabled (the authMiddleware already returns 503
// before reaching here).
//
// Token sources, checked in order:
//   - X-CSRF-Token request header (preferred for JS-driven POSTs).
//   - csrf_token form field (POSTed via traditional HTML form).
//
// Missing or invalid tokens get a 403 + a JSON error body explaining
// the missing header. The mount path passes-through GET / HEAD /
// OPTIONS; the policy is mutation-only.
func csrfMiddleware(mode AuthMode, next http.Handler) http.Handler {
	if next == nil {
		panic("csrfMiddleware: next is nil")
	}
	// Loopback and disabled both skip CSRF: loopback has no session
	// to bind to, disabled returns 503 before reaching here. Test mode
	// uses loopback; production with auth modes hits the real check.
	if mode == AuthLoopback || mode == AuthDisabled {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if w == nil {
			panic("csrfMiddleware: w is nil")
		}
		if r == nil {
			panic("csrfMiddleware: r is nil")
		}
		if isCSRFExempt(r) {
			next.ServeHTTP(w, r)
			return
		}
		if !csrfTokenValid(r) {
			w.Header().Set("Content-Type",
				"application/json; charset=utf-8")
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(csrfDeniedBody))
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isCSRFExempt returns true for read-only methods and asset paths.
// The middleware only guards state-changing requests.
func isCSRFExempt(r *http.Request) bool {
	if r == nil {
		return true
	}
	switch r.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return true
	}
	// Asset paths can't accept POST anyway, but exempt them eagerly so
	// 405-from-asset-handler outruns the 403 here.
	if strings.HasPrefix(r.URL.Path, "/console/assets/") {
		return true
	}
	return false
}

// csrfTokenValid extracts the token from the request and verifies it
// against the actor's expected token. The actor must be set on the
// request context — authMiddleware guarantees that for forward-auth
// and basic-auth modes.
func csrfTokenValid(r *http.Request) bool {
	if r == nil {
		return false
	}
	actor, ok := ActorFrom(r.Context())
	if !ok {
		return false
	}
	token := r.Header.Get("X-CSRF-Token")
	if token == "" {
		token = r.FormValue("csrf_token")
	}
	if token == "" {
		return false
	}
	return VerifyCSRFToken(actor, token)
}

// csrfDeniedBody is the verbatim JSON the middleware writes on a CSRF
// failure. Includes both the error code and a hint about which header
// the client must send.
const csrfDeniedBody = `{"error":"csrf_invalid","message":` +
	`"CSRF token missing or invalid. Submit the token via the ` +
	`X-CSRF-Token header or the csrf_token form field."}`
