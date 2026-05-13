// trigger/http_config_test.go
//
// Methodology: pure unit tests. HTTPConfig.Validate is field-only — no
// NATS, no I/O. Each test isolates one validation rule. Two assertions
// per test, positive + negative space. Validation is fatal at workflow
// registration; the symmetrical reachability checks live in dag/.
package trigger

import (
	"strings"
	"testing"
)

func validHTTPConfig() HTTPConfig {
	return HTTPConfig{
		Path:         "/api/orders",
		Method:       "POST",
		TimeoutMs:    30000,
		MaxBodyBytes: 1 * 1024 * 1024,
	}
}

func TestHTTPConfigValidateAccepts(t *testing.T) {
	cfg := validHTTPConfig()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	// Negative: still valid after adding optional fields.
	cfg.Secret = "supersecretkey1234"
	cfg.IdempotencyHeader = "Idempotency-Key"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid config with optionals rejected: %v", err)
	}
}

func TestHTTPConfigPathMissingLeadingSlash(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.Path = "api/orders"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for path without leading slash")
	}
	if !strings.Contains(err.Error(), "path") {
		t.Fatalf("err = %v, want mention of path", err)
	}
}

func TestHTTPConfigPathEmpty(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.Path = ""
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty path")
	}
	if !strings.Contains(err.Error(), "path") {
		t.Fatalf("err = %v, want mention of path", err)
	}
}

func TestHTTPConfigPathWildcardRejected(t *testing.T) {
	// ADR Layer 2 field validation: no wildcards in v1.
	cfg := validHTTPConfig()
	cfg.Path = "/api/orders/*"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for wildcard path")
	}
	if !strings.Contains(err.Error(), "wildcard") {
		t.Fatalf("err = %v, want mention of wildcard", err)
	}
}

func TestHTTPConfigMethodEnum(t *testing.T) {
	valid := []string{"GET", "POST", "PUT", "PATCH", "DELETE"}
	for _, m := range valid {
		cfg := validHTTPConfig()
		cfg.Method = m
		if err := cfg.Validate(); err != nil {
			t.Fatalf("method %q rejected: %v", m, err)
		}
	}
	cfg := validHTTPConfig()
	cfg.Method = "TRACE"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for TRACE")
	}
	if !strings.Contains(err.Error(), "method") {
		t.Fatalf("err = %v, want mention of method", err)
	}
}

func TestHTTPConfigMethodEmpty(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.Method = ""
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty method")
	}
	if !strings.Contains(err.Error(), "method") {
		t.Fatalf("err = %v, want mention of method", err)
	}
}

func TestHTTPConfigMaxBodyBytesNonPositive(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.MaxBodyBytes = 0
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for MaxBodyBytes == 0")
	}
	if !strings.Contains(err.Error(), "max_body_bytes") {
		t.Fatalf("err = %v, want mention of max_body_bytes", err)
	}

	cfg.MaxBodyBytes = -1
	err = cfg.Validate()
	if err == nil {
		t.Fatal("expected error for MaxBodyBytes < 0")
	}
}

func TestHTTPConfigTimeoutMsNonPositive(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.TimeoutMs = 0
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for TimeoutMs == 0")
	}
	if !strings.Contains(err.Error(), "timeout_ms") {
		t.Fatalf("err = %v, want mention of timeout_ms", err)
	}

	cfg.TimeoutMs = -1
	err = cfg.Validate()
	if err == nil {
		t.Fatal("expected error for TimeoutMs < 0")
	}
}

func TestHTTPConfigSecretShortRejected(t *testing.T) {
	// ADR: if Secret set, minimum length check.
	cfg := validHTTPConfig()
	cfg.Secret = "tooshort"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for short secret")
	}
	if !strings.Contains(err.Error(), "secret") {
		t.Fatalf("err = %v, want mention of secret", err)
	}
}

func TestHTTPConfigSecretEmptyAllowed(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.Secret = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("empty secret (opt-in) should be accepted: %v", err)
	}
	// Negative: a longer-than-min secret also accepted.
	cfg.Secret = strings.Repeat("a", 64)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("long secret rejected: %v", err)
	}
}

func TestHTTPConfigIdempotencyHeaderInvalid(t *testing.T) {
	cfg := validHTTPConfig()
	// HTTP header tokens disallow whitespace.
	cfg.IdempotencyHeader = "Bad Header"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for invalid header name")
	}
	if !strings.Contains(err.Error(), "idempotency_header") {
		t.Fatalf("err = %v, want mention of idempotency_header", err)
	}
}

func TestHTTPConfigIdempotencyHeaderEmptyAllowed(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.IdempotencyHeader = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("empty idempotency header (opt-in) rejected: %v", err)
	}
	cfg.IdempotencyHeader = "Idempotency-Key"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("valid header rejected: %v", err)
	}
}

func TestHTTPAuthHTTPBearerAccepted(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.Authentication = &HTTPAuthentication{
		Name:         "bearerAuth",
		Type:         "http",
		Scheme:       "bearer",
		BearerFormat: "JWT",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("http+bearer rejected: %v", err)
	}
	// Negative: an http auth without scheme fails.
	cfg.Authentication.Scheme = ""
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for type=http without scheme")
	}
	if !strings.Contains(err.Error(), "scheme") {
		t.Fatalf("err = %v, want mention of scheme", err)
	}
}

func TestHTTPAuthAPIKeyHeaderAccepted(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.Authentication = &HTTPAuthentication{
		Name:       "apiKeyAuth",
		Type:       "apiKey",
		In:         "header",
		HeaderName: "X-API-Key",
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("apiKey header rejected: %v", err)
	}
	// Negative: header missing name.
	cfg.Authentication.HeaderName = ""
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for apiKey/header without header_name")
	}
	if !strings.Contains(err.Error(), "header_name") {
		t.Fatalf("err = %v, want mention of header_name", err)
	}
}

func TestHTTPAuthTypeEnumRejected(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.Authentication = &HTTPAuthentication{
		Name: "weirdAuth",
		Type: "magicToken",
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for bogus auth type")
	}
	if !strings.Contains(err.Error(), "type") {
		t.Fatalf("err = %v, want mention of type", err)
	}
}

func TestHTTPAuthNameRequired(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.Authentication = &HTTPAuthentication{
		Type:   "http",
		Scheme: "bearer",
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for empty auth name")
	}
	if !strings.Contains(err.Error(), "name") {
		t.Fatalf("err = %v, want mention of name", err)
	}
}

func TestHTTPAuthAPIKeyInEnum(t *testing.T) {
	cfg := validHTTPConfig()
	cfg.Authentication = &HTTPAuthentication{
		Name: "apiKeyAuth",
		Type: "apiKey",
		In:   "body",
	}
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error for apiKey with in=body")
	}
	if !strings.Contains(err.Error(), "in") {
		t.Fatalf("err = %v, want mention of in", err)
	}
}
