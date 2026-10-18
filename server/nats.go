package server

import (
	"fmt"
	"net/url"
	"os"
	"runtime/debug"
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

	// 0 means "unset": keep the nats-server default (1 MiB). Config load
	// already rejects bad explicit values; this guards a Config built
	// directly in code (#735).
	if cfg.MaxPayload != 0 {
		if err := validateMaxPayload(int64(cfg.MaxPayload)); err != nil {
			return nil, fmt.Errorf("invalid max_payload: %w", err)
		}
	}

	// Apply the soft Go memory limit (GOMEMLIMIT equivalent) so the runtime
	// GCs harder and returns heap to the OS near the ceiling (#441). Skipped
	// when GOMEMLIMIT is already set by the operator (their value wins).
	_, goMemLimitSet := os.LookupEnv("GOMEMLIMIT")
	applyGoMemoryLimit(cfg.MaxMemoryBytes, goMemLimitSet)

	// Build server options
	opts := &natsserver.Options{
		Host:               host,
		Port:               cfg.NATSPort,
		HTTPPort:           cfg.MonitorPort,
		JetStream:          true,
		StoreDir:           cfg.DataDir,
		JetStreamMaxStore:  cfg.MaxStoreBytes,
		JetStreamMaxMemory: cfg.MaxMemoryBytes,
		MaxPayload:         cfg.MaxPayload,
		NoLog:              true,
		NoSigs:             true,
	}

	// Configure leaf node if remotes specified
	if len(cfg.LeafRemotes) > 0 {
		err := configureLeafNode(opts, cfg)
		if err != nil {
			return nil, err
		}
	}

	// Configure embedded cluster if cluster routes specified.
	if len(cfg.NATSClusterRoutes) > 0 {
		err := configureCluster(opts, cfg)
		if err != nil {
			return nil, err
		}
	}

	// Configure the embedded NATS WebSocket listener for browser
	// clients when an operator opts in via NATSWebsocketPort > 0.
	if cfg.NATSWebsocketPort > 0 {
		err := configureWebsocket(opts, cfg, host)
		if err != nil {
			return nil, err
		}
	}

	ns, err := tryStartNATS(opts)
	if err != nil && cfg.NATSPort == defaultNATSPort {
		return startNATSWithPortFallback(opts, cfg)
	}
	if err != nil {
		return nil, err
	}
	return ns, nil
}

// startNATSWithPortFallback handles the default-port conflict branch of
// startNATS: it either fails fast (when cfg.FailOnPortConflict) or retries
// on an ephemeral port (opts.Port = -1). Extracted to keep startNATS under
// the 70-line limit (#370).
func startNATSWithPortFallback(
	opts *natsserver.Options, cfg Config,
) (*natsserver.Server, error) {
	if opts == nil {
		panic("startNATSWithPortFallback: opts must not be nil")
	}
	if cfg.NATSPort != defaultNATSPort {
		panic("startNATSWithPortFallback: only valid for the default port")
	}

	if cfg.FailOnPortConflict {
		return nil, fmt.Errorf(
			"NATS port %d in use (another process likely "+
				"holds it); --fail-on-port-conflict is set, "+
				"refusing to fall back", cfg.NATSPort)
	}
	printWarning(os.Stderr, fmt.Sprintf(
		"NATS port %d in use, picking a free port", cfg.NATSPort))
	opts.Port = -1
	return tryStartNATS(opts)
}

// configureLeafNode wires cfg.LeafRemotes into opts.LeafNode. Extracted
// from startNATS to keep it under the 70-line limit (#370); behavior is
// unchanged. Caller guards on len(cfg.LeafRemotes) > 0.
func configureLeafNode(opts *natsserver.Options, cfg Config) error {
	if opts == nil {
		panic("configureLeafNode: opts must not be nil")
	}
	if len(cfg.LeafRemotes) == 0 {
		panic("configureLeafNode: no leaf remotes")
	}

	remotes := make([]*natsserver.RemoteLeafOpts, 0, len(cfg.LeafRemotes))
	for _, remote := range cfg.LeafRemotes {
		remoteURL, err := url.Parse(remote)
		if err != nil {
			return fmt.Errorf("parse leaf remote %q: %w", remote, err)
		}
		leaf := &natsserver.RemoteLeafOpts{
			URLs: []*url.URL{remoteURL},
		}
		if cfg.LeafCredentials != "" {
			leaf.Credentials = cfg.LeafCredentials
		}
		remotes = append(remotes, leaf)
	}
	opts.LeafNode = natsserver.LeafNodeOpts{Remotes: remotes}
	return nil
}

// configureCluster wires cfg.NATSClusterRoutes into opts.Cluster/Routes.
// Cluster mode is mutually exclusive with leaf mode; config validation
// panics if both are set. Routes lives on Options (not ClusterOpts) in
// nats-server v2. Cluster authorization rejects token-based auth, so the
// auth token is wired into Cluster.Username (single shared secret;
// Password left empty). Extracted from startNATS to keep it under the
// 70-line limit (#370); behavior is unchanged.
func configureCluster(opts *natsserver.Options, cfg Config) error {
	if opts == nil {
		panic("configureCluster: opts must not be nil")
	}
	if len(cfg.NATSClusterRoutes) == 0 {
		panic("configureCluster: no cluster routes")
	}

	parsedRoutes, err := parseClusterRoutes(cfg.NATSClusterRoutes)
	if err != nil {
		return fmt.Errorf("parse cluster routes: %w", err)
	}
	clusterPort := cfg.NATSClusterPort
	if clusterPort == 0 {
		clusterPort = defaultClusterPort
	}
	clusterHost := cfg.NATSClusterHost
	if clusterHost == "" {
		clusterHost = "0.0.0.0"
	}
	opts.Cluster = natsserver.ClusterOpts{
		Name: cfg.NATSClusterName,
		Host: clusterHost,
		Port: clusterPort,
	}
	opts.Routes = parsedRoutes
	opts.ServerName = clusterServerName(cfg, clusterPort)
	if cfg.NATSClusterAuthToken != "" {
		opts.Cluster.Username = cfg.NATSClusterAuthToken
		withRouteCredentials(parsedRoutes, cfg.NATSClusterAuthToken)
	}
	return nil
}

// withRouteCredentials puts the cluster token on every route URL that
// carries no credentials of its own. nats-server checks Cluster.Username
// on routes it accepts but sends credentials on the routes it dials only
// from the URL's userinfo, so without this every node rejected every
// other node's routes and a token-protected cluster never formed.
func withRouteCredentials(routes []*url.URL, token string) {
	if token == "" {
		panic("withRouteCredentials: token must not be empty")
	}
	if len(routes) == 0 {
		panic("withRouteCredentials: routes must not be empty")
	}
	for _, route := range routes {
		if route.User == nil {
			route.User = url.User(token)
		}
	}
}

// configureWebsocket wires the embedded NATS WebSocket listener (ADR-020)
// into opts when an operator opts in via NATSWebsocketPort > 0. Auth
// fields are left zero so they inherit Options.Users / top-level auth —
// the contract is "no new auth model". Host reuses the TCP-port binding
// posture (127.0.0.1 standalone, 0.0.0.0 in leaf mode). Extracted from
// startNATS to keep it under the 70-line limit (#370).
func configureWebsocket(
	opts *natsserver.Options, cfg Config, host string,
) error {
	if opts == nil {
		panic("configureWebsocket: opts must not be nil")
	}
	if cfg.NATSWebsocketPort <= 0 {
		panic("configureWebsocket: NATSWebsocketPort not set")
	}

	if !cfg.NATSWebsocketNoTLS {
		return fmt.Errorf(
			"nats_ws_port set but TLS not configured: " +
				"pass --nats-ws-no-tls to opt into the " +
				"explicit insecure dev mode, or wait for " +
				"server TLS wiring",
		)
	}
	opts.Websocket = natsserver.WebsocketOpts{
		Host:  host,
		Port:  cfg.NATSWebsocketPort,
		NoTLS: cfg.NATSWebsocketNoTLS,
	}
	printStep(os.Stderr, fmt.Sprintf(
		"WARNING: WebSocket listener on :%d running "+
			"WITHOUT TLS — dev mode only, do not "+
			"expose to untrusted networks",
		cfg.NATSWebsocketPort,
	))
	return nil
}

// applyGoMemoryLimit sets the soft Go memory limit (the GOMEMLIMIT
// equivalent) from the configured ceiling so the runtime GCs harder and
// returns heap to the OS as it approaches the limit (#441). It is a no-op
// when limitBytes <= 0 (never force a 0 limit — that pins the GC running
// continuously) or when envSet is true (an explicit GOMEMLIMIT wins).
func applyGoMemoryLimit(limitBytes int64, envSet bool) {
	if limitBytes < 0 {
		panic(fmt.Sprintf(
			"applyGoMemoryLimit: negative limit %d", limitBytes))
	}
	if limitBytes == 0 || envSet {
		return
	}
	debug.SetMemoryLimit(limitBytes)
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

// clusterServerName picks the node's JetStream server_name. JetStream
// records stream and meta peers by this name, so it must be the same on
// every restart: a pid-derived name made each restart look like a new
// peer and left the old one behind as a permanently offline replica.
// Hostname plus cluster port is stable and still unique when several
// nodes share a host.
func clusterServerName(cfg Config, clusterPort int) string {
	if cfg.NATSClusterName == "" {
		panic("clusterServerName: cluster name must not be empty")
	}
	if clusterPort <= 0 {
		panic("clusterServerName: cluster port must be positive")
	}
	if cfg.NATSServerName != "" {
		return cfg.NATSServerName
	}
	hostname, err := os.Hostname()
	if err != nil || hostname == "" {
		hostname = "node"
	}
	hostname = strings.Map(func(r rune) rune {
		if isServerNameRune(r) {
			return r
		}
		return '-'
	}, hostname)
	return fmt.Sprintf("%s-%s-%d", cfg.NATSClusterName, hostname, clusterPort)
}
