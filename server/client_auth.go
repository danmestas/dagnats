// server/client_auth.go
// Optional logins for a dagnats that other local users can reach: a NATS
// user for the client port (dagnats' own connection logs in with it too)
// and a bearer token for the HTTP listener. Secrets come from files so
// they stay out of the environment and the process list. With neither
// set, both listeners stay open, as before.
package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
)

// secretFileBytesMax bounds a secret file; a password or token is short,
// so anything larger is a wrong path, not a secret.
const secretFileBytesMax = 4096

// readSecretFile returns the trimmed contents of path, refusing an empty
// or oversized file.
func readSecretFile(path string) (string, error) {
	if path == "" {
		panic("readSecretFile: path must not be empty")
	}
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	if info, err := f.Stat(); err == nil && info.Mode().Perm()&0o077 != 0 {
		slog.Warn("secret file is readable by other users; chmod 600 it",
			"path", path, "mode", info.Mode().Perm().String())
	}
	// Read one byte past the limit through a bounded reader, so a device
	// or FIFO given by mistake cannot stall startup.
	raw, err := io.ReadAll(io.LimitReader(f, secretFileBytesMax+1))
	if err != nil {
		return "", err
	}
	if len(raw) > secretFileBytesMax {
		return "", fmt.Errorf("%s: over the %d-byte limit for a secret",
			path, secretFileBytesMax)
	}
	secret := strings.TrimSpace(string(raw))
	if secret == "" {
		return "", fmt.Errorf("%s is empty", path)
	}
	return secret, nil
}

// resolveClientAuth reads the configured secret files into cfg. The NATS
// user and its password file come as a pair.
func resolveClientAuth(cfg *Config) error {
	if cfg == nil {
		panic("resolveClientAuth: cfg must not be nil")
	}
	if (cfg.NATSUser == "") != (cfg.NATSPasswordFile == "") {
		return errors.New("nats_user and nats_password_file must be set together")
	}
	if cfg.NATSPasswordFile != "" {
		password, err := readSecretFile(cfg.NATSPasswordFile)
		if err != nil {
			return fmt.Errorf("nats_password_file: %w", err)
		}
		cfg.natsPassword = password
	}
	if cfg.APITokenFile != "" {
		token, err := readSecretFile(cfg.APITokenFile)
		if err != nil {
			return fmt.Errorf("api_token_file: %w", err)
		}
		cfg.apiToken = token
	}
	warnUncoveredListeners(*cfg)
	return nil
}

// warnUncoveredListeners names the listeners the client login does not
// reach, so an operator who set one does not assume they are closed too.
func warnUncoveredListeners(cfg Config) {
	if cfg.NATSUser == "" {
		return
	}
	if cfg.MonitorPort > 0 {
		slog.Warn("nats_user does not cover the NATS monitoring port: "+
			"anyone who can reach it sees connection names, subjects and streams",
			"monitor_port", cfg.MonitorPort)
	}
	if len(cfg.NATSClusterRoutes) > 0 && cfg.NATSClusterAuthToken == "" {
		slog.Warn("nats_user does not cover cluster routes: set " +
			"nats_cluster_auth_token, or a process that speaks the route " +
			"protocol reads every message")
	}
}

// apiTokenGate requires "Authorization: Bearer <token>" on every request
// except the health and readiness probes, which carry no data. An empty
// token returns next unchanged.
func apiTokenGate(token string, next http.Handler) http.Handler {
	if next == nil {
		panic("apiTokenGate: next must not be nil")
	}
	if token == "" {
		return next
	}
	// Compare digests so the comparison takes the same time whatever the
	// length of the presented value.
	want := sha256.Sum256([]byte(token))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isProbePath(r.URL.Path) {
			next.ServeHTTP(w, r)
			return
		}
		presented, ok := bearerToken(r.Header.Get("Authorization"))
		got := sha256.Sum256([]byte(presented))
		if !ok || subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			w.Header().Set("WWW-Authenticate", `Bearer realm="dagnats"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// bearerToken returns the credentials of a Bearer authorization header.
// The scheme name is case-insensitive (RFC 7235).
func bearerToken(header string) (string, bool) {
	scheme, token, found := strings.Cut(header, " ")
	if !found || !strings.EqualFold(scheme, "Bearer") {
		return "", false
	}
	return token, true
}

func isProbePath(path string) bool {
	return path == "/health" || path == "/health/cluster" || path == "/ready"
}
