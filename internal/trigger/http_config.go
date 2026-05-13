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
//
// Authentication is metadata only — the engine does not enforce
// JWT/OAuth/etc. validation. Per ADR-013 Q5 enforcement is a
// workflow-step concern. The field exists so the OpenAPI spec
// generator can advertise the expected security scheme to clients.
type HTTPConfig struct {
	Path              string              `json:"path"`
	Method            string              `json:"method"`
	TimeoutMs         int                 `json:"timeout_ms"`
	MaxBodyBytes      int64               `json:"max_body_bytes"`
	Secret            string              `json:"secret,omitempty"`
	IdempotencyHeader string              `json:"idempotency_header,omitempty"`
	Authentication    *HTTPAuthentication `json:"authentication,omitempty"`
}

// HTTPAuthentication declaratively advertises an OpenAPI 3.1 security
// scheme so the generated spec at GET /openapi.json describes the
// expected auth shape. The engine does NOT enforce the declared
// scheme — validation remains a workflow-step concern per ADR-013 Q5.
// This field exists purely so clients generated from the spec carry
// the correct auth headers / scopes.
type HTTPAuthentication struct {
	Name         string `json:"name"`
	Type         string `json:"type"`
	Scheme       string `json:"scheme,omitempty"`
	BearerFormat string `json:"bearer_format,omitempty"`
	In           string `json:"in,omitempty"`
	HeaderName   string `json:"header_name,omitempty"`
	Description  string `json:"description,omitempty"`
}

// httpAuthAllowedTypes mirrors OpenAPI 3.1 securityScheme.type. The
// closed set rejects typo-class errors (e.g. "Bearer" → "type": "http"
// with "scheme": "bearer") at registration time. Extending requires a
// matching mapping in the OpenAPI synthesiser.
var httpAuthAllowedTypes = map[string]bool{
	"http":          true,
	"apiKey":        true,
	"oauth2":        true,
	"openIdConnect": true,
}

// httpAuthAPIKeyInValues enumerates OpenAPI 3.1's apiKey.in values.
var httpAuthAPIKeyInValues = map[string]bool{
	"header": true,
	"query":  true,
	"cookie": true,
}

// Validate enforces field-level rules per OpenAPI 3.1 securityScheme
// shape. Mirrored from HTTPConfig.Validate — fatal at registration.
func (a *HTTPAuthentication) Validate() error {
	if a == nil {
		panic("HTTPAuthentication.Validate: receiver must not be nil")
	}
	if a.Name == "" {
		return fmt.Errorf("authentication name must not be empty")
	}
	if !httpAuthAllowedTypes[a.Type] {
		return fmt.Errorf(
			"authentication type %q not allowed; valid: "+
				"http apiKey oauth2 openIdConnect",
			a.Type,
		)
	}
	if a.Type == "http" && a.Scheme == "" {
		return fmt.Errorf(
			"authentication scheme required for type=http",
		)
	}
	if a.Type == "apiKey" {
		if !httpAuthAPIKeyInValues[a.In] {
			return fmt.Errorf(
				"authentication.in %q not allowed for apiKey; "+
					"valid: header query cookie",
				a.In,
			)
		}
		if a.In == "header" && a.HeaderName == "" {
			return fmt.Errorf(
				"authentication.header_name required for apiKey/header",
			)
		}
		if a.HeaderName != "" && !isValidHTTPHeaderName(a.HeaderName) {
			return fmt.Errorf(
				"authentication.header_name %q is not a valid header name",
				a.HeaderName,
			)
		}
	}
	return nil
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
	if c.Authentication != nil {
		if err := c.Authentication.Validate(); err != nil {
			return fmt.Errorf("authentication: %w", err)
		}
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
