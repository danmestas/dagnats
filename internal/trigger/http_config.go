package trigger

import (
	"fmt"
	"net/http"
	"strings"
)

// HTTPConfig defines a synchronous HTTP trigger per ADR-013. Unlike
// WebhookConfig (fire-and-forget), HTTP triggers correlate a response
// back to the originating request via dagnats.http.response.<run_id>
// (see ResponseSubject). Field validation lives in Validate(); graph
// reachability checks live in dag.ValidateRespondReachability.
type HTTPConfig struct {
	Path              string `json:"path"`
	Method            string `json:"method"`
	TimeoutMs         int    `json:"timeout_ms"`
	MaxBodyBytes      int64  `json:"max_body_bytes"`
	Secret            string `json:"secret,omitempty"`
	IdempotencyHeader string `json:"idempotency_header,omitempty"`
}

// httpConfigSecretMinLen sets the minimum HMAC secret length when a
// caller opts into Secret. Below this, the secret has negligible
// security value; making it a fatal validation error stops the
// foot-gun at registration rather than after a leak.
const httpConfigSecretMinLen = 16

// httpConfigAllowedMethods is the closed v1 method set. ADR-013 caps
// the surface at REST-shaped verbs; adding HEAD/OPTIONS later is a
// strict superset and stays an additive change.
var httpConfigAllowedMethods = map[string]bool{
	http.MethodGet:    true,
	http.MethodPost:   true,
	http.MethodPut:    true,
	http.MethodPatch:  true,
	http.MethodDelete: true,
}

// Validate enforces field-level rules from ADR-013 Layer 2. It is
// fatal at workflow registration — graph-level (Layer 1) reachability
// is the warning-only counterpart so legitimate branch-per-outcome
// patterns stay legal.
func (c *HTTPConfig) Validate() error {
	if c == nil {
		panic("HTTPConfig.Validate: receiver must not be nil")
	}
	if err := validateHTTPPath(c.Path); err != nil {
		return err
	}
	if !httpConfigAllowedMethods[c.Method] {
		return fmt.Errorf(
			"method %q not allowed; valid: GET POST PUT PATCH DELETE",
			c.Method,
		)
	}
	if c.MaxBodyBytes <= 0 {
		return fmt.Errorf(
			"max_body_bytes must be > 0, got %d", c.MaxBodyBytes,
		)
	}
	if c.TimeoutMs <= 0 {
		return fmt.Errorf(
			"timeout_ms must be > 0, got %d", c.TimeoutMs,
		)
	}
	if c.Secret != "" && len(c.Secret) < httpConfigSecretMinLen {
		return fmt.Errorf(
			"secret length %d < min %d",
			len(c.Secret), httpConfigSecretMinLen,
		)
	}
	if c.IdempotencyHeader != "" &&
		!isValidHTTPHeaderName(c.IdempotencyHeader) {
		return fmt.Errorf(
			"idempotency_header %q is not a valid header name",
			c.IdempotencyHeader,
		)
	}
	return nil
}

// validateHTTPPath checks the path rules from ADR-013: must start with
// "/", no wildcards in v1.
func validateHTTPPath(p string) error {
	if p == "" {
		return fmt.Errorf("path must not be empty")
	}
	if !strings.HasPrefix(p, "/") {
		return fmt.Errorf(
			"path %q must start with /", p,
		)
	}
	if strings.ContainsAny(p, "*") {
		return fmt.Errorf(
			"path %q contains wildcard (not supported in v1)", p,
		)
	}
	return nil
}

// isValidHTTPHeaderName accepts only RFC 7230 token characters. Mirrors
// net/http's textproto.CanonicalMIMEHeaderKey rules without importing
// the package (token = 1*tchar, no whitespace, no separators).
func isValidHTTPHeaderName(name string) bool {
	if name == "" {
		return false
	}
	for i := 0; i < len(name); i++ {
		if !isTokenByte(name[i]) {
			return false
		}
	}
	return true
}

// isTokenByte returns true for RFC 7230 token characters.
func isTokenByte(b byte) bool {
	switch {
	case b >= 'A' && b <= 'Z':
		return true
	case b >= 'a' && b <= 'z':
		return true
	case b >= '0' && b <= '9':
		return true
	}
	// RFC 7230: !#$%&'*+-.^_`|~
	switch b {
	case '!', '#', '$', '%', '&', '\'', '*', '+', '-',
		'.', '^', '_', '`', '|', '~':
		return true
	}
	return false
}
