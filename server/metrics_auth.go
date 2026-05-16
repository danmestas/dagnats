// server/metrics_auth.go
// METRICS_AUTH gate around the /metrics exporter. Independent from the
// console gate: an operator may want the console locked behind basic
// auth while letting their Prometheus scraper hit /metrics with a
// service-account token (forward auth). Documented modes:
//
//	loopback (default): only 127.0.0.1 / ::1 reaches /metrics.
//	basic: HTTP Basic Auth from METRICS_BASIC_USER + METRICS_BASIC_PASS.
//	forward: trust X-Forwarded-User from an upstream proxy.
//	none: open. Logs a Warn at startup in a non-dev context.
//
// The gate wraps prom.Handler; the rendered output is unchanged.
package server

import (
	"crypto/subtle"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
)

// MetricsAuthMode is the bounded enum the gate accepts. The env var
// METRICS_AUTH carries the string form; resolveMetricsAuthMode
// normalises it.
type MetricsAuthMode string

const (
	MetricsAuthLoopback MetricsAuthMode = "loopback"
	MetricsAuthBasic    MetricsAuthMode = "basic"
	MetricsAuthForward  MetricsAuthMode = "forward"
	MetricsAuthNone     MetricsAuthMode = "none"
)

// MetricsAuthConfig captures the gate's inputs. HTTPAddr is required
// because the loopback mode rejects non-loopback requests by remote
// address rather than listener bind.
type MetricsAuthConfig struct {
	Mode      MetricsAuthMode
	BasicUser string
	BasicPass string
}

// LoadMetricsAuthConfigFromEnv reads METRICS_AUTH + METRICS_BASIC_USER
// + METRICS_BASIC_PASS from the process environment. Defaults to
// loopback when METRICS_AUTH is unset. Unknown modes are normalised
// to loopback with a slog.Warn so a typo fails closed.
func LoadMetricsAuthConfigFromEnv(logger *slog.Logger) MetricsAuthConfig {
	if logger == nil {
		logger = slog.Default()
	}
	raw := strings.TrimSpace(os.Getenv("METRICS_AUTH"))
	mode := MetricsAuthMode(strings.ToLower(raw))
	switch mode {
	case "":
		mode = MetricsAuthLoopback
	case MetricsAuthLoopback, MetricsAuthBasic,
		MetricsAuthForward, MetricsAuthNone:
		// valid
	default:
		logger.Warn("METRICS_AUTH: unknown mode; defaulting to loopback",
			"raw", raw)
		mode = MetricsAuthLoopback
	}
	cfg := MetricsAuthConfig{
		Mode:      mode,
		BasicUser: os.Getenv("METRICS_BASIC_USER"),
		BasicPass: os.Getenv("METRICS_BASIC_PASS"),
	}
	if mode == MetricsAuthBasic &&
		(cfg.BasicUser == "" || cfg.BasicPass == "") {
		logger.Warn(
			"METRICS_AUTH=basic but credentials missing; gate refuses all",
		)
	}
	if mode == MetricsAuthNone {
		logger.Warn(
			"METRICS_AUTH=none — /metrics is open to every caller",
		)
	}
	return cfg
}

// metricsAuthMiddleware wraps next with the gate. Pure function over
// (cfg, next); test seams swap cfg without env mutation.
func metricsAuthMiddleware(
	cfg MetricsAuthConfig, next http.Handler,
) http.Handler {
	if next == nil {
		panic("metricsAuthMiddleware: next is nil")
	}
	switch cfg.Mode {
	case MetricsAuthLoopback:
		return loopbackMetricsHandler(next)
	case MetricsAuthBasic:
		return basicMetricsHandler(cfg.BasicUser, cfg.BasicPass, next)
	case MetricsAuthForward:
		return forwardMetricsHandler(next)
	case MetricsAuthNone:
		return next
	}
	// Unknown mode reached the middleware — fail closed.
	return loopbackMetricsHandler(next)
}

// loopbackMetricsHandler accepts requests whose RemoteAddr resolves
// to a loopback IP. The TCP listener may be on 0.0.0.0; this gate
// looks at the per-connection remote address.
func loopbackMetricsHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		if w == nil {
			panic("loopbackMetricsHandler: w is nil")
		}
		if r == nil {
			panic("loopbackMetricsHandler: r is nil")
		}
		if !isRemoteLoopback(r.RemoteAddr) {
			http.Error(w, "unauthorized",
				http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// basicMetricsHandler requires HTTP Basic Auth that matches the
// configured user / pass via constant-time compare. Empty creds in
// cfg => 401 every time (fail-closed when the operator set basic but
// forgot to set the secret).
func basicMetricsHandler(
	user, pass string, next http.Handler,
) http.Handler {
	if user == "" || pass == "" {
		return http.HandlerFunc(func(
			w http.ResponseWriter, r *http.Request,
		) {
			w.Header().Set("WWW-Authenticate",
				`Basic realm="dagnats metrics"`)
			http.Error(w, "unauthorized",
				http.StatusUnauthorized)
		})
	}
	wantU := []byte(user)
	wantP := []byte(pass)
	return http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		if r == nil {
			panic("basicMetricsHandler: r is nil")
		}
		gotU, gotP, ok := r.BasicAuth()
		userMatch := subtle.ConstantTimeCompare(
			[]byte(gotU), wantU,
		) == 1
		passMatch := subtle.ConstantTimeCompare(
			[]byte(gotP), wantP,
		) == 1
		if !ok || !userMatch || !passMatch {
			w.Header().Set("WWW-Authenticate",
				`Basic realm="dagnats metrics"`)
			http.Error(w, "unauthorized",
				http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// forwardMetricsHandler requires the X-Forwarded-User header. The
// operator's reverse proxy authenticates upstream and injects this
// header; the middleware does not validate the proxy itself.
func forwardMetricsHandler(next http.Handler) http.Handler {
	return http.HandlerFunc(func(
		w http.ResponseWriter, r *http.Request,
	) {
		if r == nil {
			panic("forwardMetricsHandler: r is nil")
		}
		user := strings.TrimSpace(r.Header.Get("X-Forwarded-User"))
		if user == "" {
			http.Error(w, "unauthorized",
				http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// isRemoteLoopback reports whether a connection's RemoteAddr came
// from a loopback interface. Defensive against the "host:port" vs
// bare-IP variants that net/http passes in different environments.
func isRemoteLoopback(remote string) bool {
	if remote == "" {
		return false
	}
	host, _, err := net.SplitHostPort(remote)
	if err != nil {
		host = remote
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return false
	}
	return ip.IsLoopback()
}
