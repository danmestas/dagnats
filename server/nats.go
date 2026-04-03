package server

import (
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

const natsReadyTimeout = 5 * time.Second

// startNATS starts an embedded NATS server with JetStream enabled.
// In standalone mode (no leaf remotes), binds to localhost only.
// In leaf mode (len(cfg.LeafRemotes) > 0), binds to 0.0.0.0 for remote access.
// Panics if cfg.DataDir is empty or cfg.MaxStoreBytes <= 0.
func startNATS(cfg Config) (*natsserver.Server, error) {
	if cfg.DataDir == "" {
		panic("startNATS: cfg.DataDir is empty")
	}
	if cfg.MaxStoreBytes <= 0 {
		panic(fmt.Sprintf("startNATS: cfg.MaxStoreBytes <= 0: %d", cfg.MaxStoreBytes))
	}

	// Determine bind address based on leaf mode
	host := "127.0.0.1"
	if len(cfg.LeafRemotes) > 0 {
		host = "0.0.0.0"
	}

	// Build server options
	opts := &natsserver.Options{
		Host:              host,
		Port:              cfg.NATSPort,
		HTTPPort:          cfg.MonitorPort,
		JetStream:         true,
		StoreDir:          filepath.Join(cfg.DataDir, "jetstream"),
		JetStreamMaxStore: cfg.MaxStoreBytes,
		NoLog:             true,
		NoSigs:            true,
	}

	// Configure leaf node if remotes specified
	if len(cfg.LeafRemotes) > 0 {
		remotes := make([]*natsserver.RemoteLeafOpts, 0, len(cfg.LeafRemotes))
		for _, remote := range cfg.LeafRemotes {
			remoteURL, err := url.Parse(remote)
			if err != nil {
				return nil, fmt.Errorf("parse leaf remote %q: %w", remote, err)
			}
			remote := &natsserver.RemoteLeafOpts{
				URLs: []*url.URL{remoteURL},
			}
			if cfg.LeafCredentials != "" {
				remote.Credentials = cfg.LeafCredentials
			}
			remotes = append(remotes, remote)
		}
		opts.LeafNode = natsserver.LeafNodeOpts{
			Remotes: remotes,
		}
	}

	// Create and start server
	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("create NATS server: %w", err)
	}

	ns.Start()

	// Wait for server to be ready
	if !ns.ReadyForConnections(natsReadyTimeout) {
		ns.Shutdown()
		return nil, fmt.Errorf("NATS server not ready after %v", natsReadyTimeout)
	}

	return ns, nil
}

// resolveCredentials handles leaf_credentials as either a file
// path or inline credential content. If the value starts with
// "-----BEGIN", it is treated as inline content and written to a
// temp file (returned path). Otherwise it is treated as a file
// path and returned as-is. Callers must clean up temp files via
// cleanupCredentials.
func resolveCredentials(value string) (string, error) {
	if value == "" {
		panic("resolveCredentials: value must not be empty")
	}
	if strings.HasPrefix(value, "-----BEGIN") {
		f, err := os.CreateTemp("", "dagnats-creds-*.creds")
		if err != nil {
			return "", fmt.Errorf("create temp creds: %w", err)
		}
		if _, err := f.WriteString(value); err != nil {
			f.Close()
			os.Remove(f.Name())
			return "", fmt.Errorf("write temp creds: %w", err)
		}
		f.Close()
		os.Chmod(f.Name(), 0600)
		return f.Name(), nil
	}
	return value, nil
}
