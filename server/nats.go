package server

import (
	"fmt"
	"net/url"
	"os"
	"strings"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
)

const natsReadyTimeout = 5 * time.Second

const defaultClusterPort = 6222

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
		StoreDir:          cfg.DataDir,
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

	// Configure embedded cluster if cluster routes specified.
	// Cluster mode is mutually exclusive with leaf mode; validation in
	// server/config.go panics if both are set.
	//
	// Note: Routes lives on Options (not ClusterOpts) in nats-server v2.
	// Cluster authorization explicitly rejects token-based auth, so the
	// "auth token" is wired into Cluster.Username (single shared secret;
	// Password left empty), which matches how operators typically encode
	// a bearer-like cluster credential.
	if len(cfg.NATSClusterRoutes) > 0 {
		parsedRoutes, err := parseClusterRoutes(cfg.NATSClusterRoutes)
		if err != nil {
			return nil, fmt.Errorf("parse cluster routes: %w", err)
		}
		opts.Cluster = natsserver.ClusterOpts{
			Name: cfg.NATSClusterName,
			Host: "0.0.0.0",
			Port: defaultClusterPort,
		}
		opts.Routes = parsedRoutes
		// JetStream clustering requires a unique server_name per node.
		// Derive a stable-but-unique name from the cluster name and pid
		// so multi-node tests don't collide; operators can override later
		// once a deployment-aware name source is wired in.
		if opts.ServerName == "" {
			opts.ServerName = fmt.Sprintf(
				"%s-%d", cfg.NATSClusterName, os.Getpid(),
			)
		}
		if cfg.NATSClusterAuthToken != "" {
			opts.Cluster.Username = cfg.NATSClusterAuthToken
		}
	}

	ns, err := tryStartNATS(opts)
	if err != nil && cfg.NATSPort == defaultNATSPort {
		printStep(os.Stderr,
			fmt.Sprintf("port %d in use, picking a free port", cfg.NATSPort))
		opts.Port = -1
		ns, err = tryStartNATS(opts)
	}
	if err != nil {
		return nil, err
	}
	return ns, nil
}

func tryStartNATS(opts *natsserver.Options) (*natsserver.Server, error) {
	if opts == nil {
		panic("tryStartNATS: opts must not be nil")
	}

	ns, err := natsserver.NewServer(opts)
	if err != nil {
		return nil, fmt.Errorf("create NATS server: %w", err)
	}

	ns.Start()

	if !ns.ReadyForConnections(natsReadyTimeout) {
		ns.Shutdown()
		return nil, fmt.Errorf("NATS server not ready after %v", natsReadyTimeout)
	}

	return ns, nil
}

// parseClusterRoutes converts raw URL strings into *url.URL values
// suitable for natsserver.ClusterOpts.Routes. Caller must ensure raw
// is non-empty; this is a programmer-error contract.
func parseClusterRoutes(raw []string) ([]*url.URL, error) {
	if len(raw) == 0 {
		panic("parseClusterRoutes: raw is empty")
	}
	out := make([]*url.URL, 0, len(raw))
	for _, r := range raw {
		u, err := url.Parse(r)
		if err != nil {
			return nil, fmt.Errorf("parse cluster route %q: %w", r, err)
		}
		out = append(out, u)
	}
	return out, nil
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
