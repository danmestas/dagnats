package console

import (
	"context"
	"crypto/subtle"
	"fmt"
	"net"
	"net/http"
	"strings"
)

// AuthMode reports which gate (if any) protects the console.
// It is set at startup, never mutated after Mount returns.
type AuthMode int

const (
	// AuthLoopback means the listener is bound to a loopback address.
	// OS-level access control is the only boundary; no headers are read.
	AuthLoopback AuthMode = iota
	// AuthForwarded means an upstream proxy provides the operator
	// identity via X-Forwarded-User / X-Forwarded-Email headers.
	AuthForwarded
	// AuthBasic means HTTP Basic Auth gates every request; the actor
	// is the literal string "console".
	AuthBasic
	// AuthDisabled means the listener is non-loopback and no auth was
	// configured; the console refuses to serve.
	AuthDisabled
)

// String returns a human-readable label for log/UI surfaces.
func (m AuthMode) String() string {
	switch m {
	case AuthLoopback:
		return "loopback"
	case AuthForwarded:
		return "forward-auth"
	case AuthBasic:
		return "basic-auth"
	case AuthDisabled:
		return "disabled"
	default:
		return "unknown"
	}
}

// AuthConfig captures the inputs needed to pick an AuthMode.
// HTTPAddr is the listener's already-resolved address (host:port form).
// ForwardAuth and Password come from DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH
// and DAGNATS_CONSOLE_PASSWORD respectively.
type AuthConfig struct {
	HTTPAddr    string
	ForwardAuth bool
	Password    string
}

// ResolveAuthMode picks the AuthMode from cfg.
// Loopback wins outright; otherwise the operator must opt into one of
// the two remote modes. Conflicting flags are an error so the operator
// has to pick one explicitly.
func ResolveAuthMode(cfg AuthConfig) (AuthMode, error) {
	if cfg.HTTPAddr == "" {
		panic("ResolveAuthMode: cfg.HTTPAddr is empty")
	}
	if isLoopbackAddr(cfg.HTTPAddr) {
		return AuthLoopback, nil
	}
	if cfg.ForwardAuth && cfg.Password != "" {
		return AuthDisabled, fmt.Errorf(
			"console: pick exactly one of " +
				"DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH and DAGNATS_CONSOLE_PASSWORD",
		)
	}
	if cfg.ForwardAuth {
		return AuthForwarded, nil
	}
	if cfg.Password != "" {
		return AuthBasic, nil
	}
	return AuthDisabled, nil
}

// isLoopbackAddr reports whether the listen address binds only to a
// loopback interface. "" / ":PORT" / "0.0.0.0:PORT" / a public IP are
// non-loopback. "127.0.0.1:PORT", "[::1]:PORT", or "localhost:PORT"
// (after DNS) are loopback.
//
// Bounded loop: we resolve at most one host token; we never recurse.
func isLoopbackAddr(addr string) bool {
	if addr == "" {
		panic("isLoopbackAddr: addr is empty")
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		// Fallbacks: ":8080" parses fine; a bare hostname does not.
		host = addr
	}
	if host == "" || host == "0.0.0.0" || host == "::" {
		return false
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	// Hostname form ("localhost"). Only treat the lowercase literal as
	// loopback — DNS-resolving arbitrary names here would couple
	// startup to resolver state we do not control.
	return strings.EqualFold(host, "localhost")
}

// Actor identifies the caller for audit and UI purposes.
// Empty fields are valid in the loopback case where attribution is
// implicit ("loopback"); see the Display() helper.
type Actor struct {
	User   string
	Email  string
	Source AuthMode
}

// Display returns the string the UI/audit log shows. It never panics
// and never returns an empty string: a loopback request without
// explicit attribution renders as "loopback".
func (a Actor) Display() string {
	if a.User != "" {
		return a.User
	}
	if a.Email != "" {
		return a.Email
	}
	if a.Source == AuthBasic {
		return "console"
	}
	return "loopback"
}

// ctxKey is unexported so callers must use the helpers below.
type ctxKey struct{}

// withActor attaches an Actor to ctx for downstream handlers.
func withActor(ctx context.Context, a Actor) context.Context {
	if ctx == nil {
		panic("withActor: ctx is nil")
	}
	return context.WithValue(ctx, ctxKey{}, a)
}

// ActorFrom returns the Actor associated with ctx. If none is present
// the zero value (Source: AuthLoopback, empty user/email) is returned.
// The boolean reports whether an Actor was actually set.
func ActorFrom(ctx context.Context) (Actor, bool) {
	if ctx == nil {
		panic("ActorFrom: ctx is nil")
	}
	a, ok := ctx.Value(ctxKey{}).(Actor)
	return a, ok
}

// authMiddleware wraps next with the gate matching mode.
//
// Loopback: pass through with Actor{Source: AuthLoopback}.
// Forwarded: copy X-Forwarded-User / X-Forwarded-Email; never reject.
// Basic: 401 on missing/wrong credentials; constant-time compare.
// Disabled: 503 with the documented JSON body.
//
// Returns a handler whose total auth-related code stays well under the
// 50-LOC budget the design doc names.
func authMiddleware(mode AuthMode, password string, next http.Handler) http.Handler {
	if next == nil {
		panic("authMiddleware: next is nil")
	}
	if mode == AuthBasic && password == "" {
		panic("authMiddleware: basic-auth without password")
	}
	switch mode {
	case AuthLoopback:
		return loopbackHandler(next)
	case AuthForwarded:
		return forwardedHandler(next)
	case AuthBasic:
		return basicHandler(password, next)
	case AuthDisabled:
		return disabledHandler()
	default:
		panic(fmt.Sprintf("authMiddleware: unknown mode %d", mode))
	}
}

func loopbackHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if w == nil {
			panic("loopbackHandler: w is nil")
		}
		if r == nil {
			panic("loopbackHandler: r is nil")
		}
		ctx := withActor(r.Context(), Actor{Source: AuthLoopback})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func forwardedHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if w == nil {
			panic("forwardedHandler: w is nil")
		}
		if r == nil {
			panic("forwardedHandler: r is nil")
		}
		a := Actor{
			User:   r.Header.Get("X-Forwarded-User"),
			Email:  r.Header.Get("X-Forwarded-Email"),
			Source: AuthForwarded,
		}
		next.ServeHTTP(w, r.WithContext(withActor(r.Context(), a)))
	})
}

func basicHandler(password string, next http.Handler) http.Handler {
	want := []byte(password)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if w == nil {
			panic("basicHandler: w is nil")
		}
		if r == nil {
			panic("basicHandler: r is nil")
		}
		_, got, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(got), want) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="dagnats console"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		a := Actor{Source: AuthBasic}
		next.ServeHTTP(w, r.WithContext(withActor(r.Context(), a)))
	})
}

func disabledHandler() http.Handler {
	body := []byte(consoleDisabledBody)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if w == nil {
			panic("disabledHandler: w is nil")
		}
		if r == nil {
			panic("disabledHandler: r is nil")
		}
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write(body)
	})
}

// consoleDisabledBody is the verbatim JSON body returned when the
// console refuses to serve due to a non-loopback bind without auth.
const consoleDisabledBody = `{"error":"console_disabled","message":"console disabled: ` +
	`listener bound to non-loopback but no auth mode configured. ` +
	`Set DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH=true or ` +
	`DAGNATS_CONSOLE_PASSWORD=... to enable."}`

// DisabledLogMessage is the loud startup line the operator sees in
// the dagnats serve logs when /console/ refuses to mount. The message
// names both env vars so the operator knows exactly which flag to set.
const DisabledLogMessage = "console disabled: listener bound to " +
	"non-loopback but no auth mode configured. " +
	"Set DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH=true or " +
	"DAGNATS_CONSOLE_PASSWORD=... to enable."
