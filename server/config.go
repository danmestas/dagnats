package server

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/danmestas/dagnats/internal/engine"
)

// errUnknownConfigKey marks a config-file key the parser does not
// recognize. loadConfigFile tolerates these (logs a warning) for
// forward-compat, but propagates every other applyConfigValue error
// as a hard load failure.
var errUnknownConfigKey = errors.New("unknown config key")

const (
	// defaultHTTPAddr binds only to loopback by default so the embedded
	// control plane UI (and every other HTTP surface) is reachable
	// only from processes on the host. Operators with remote-access
	// deployments must set DAGNATS_HTTP_ADDR explicitly to a
	// non-loopback bind (e.g. "0.0.0.0:8080"). See ADR-014.
	defaultHTTPAddr      = "127.0.0.1:8080"
	defaultNATSPort      = 4222
	defaultMaxStoreBytes = 10 << 30 // 10 GiB
	// defaultMaxMemoryBytes caps the JetStream in-memory store and is also
	// applied as the soft Go memory limit (GOMEMLIMIT) at startup so the
	// runtime GCs harder and returns heap to the OS near the ceiling (#441).
	// 1 GiB is safe for a typical host; tunable via DAGNATS_MAX_MEMORY_BYTES
	// or the max_memory_bytes config key.
	defaultMaxMemoryBytes = 1 << 30 // 1 GiB
	maxLeafRemotes        = 10
	maxClusterRoutes      = 10
	maxConfigFileLines    = 300
	maxWorkerConfigs      = 50

	// Per-runtime bound defaults (ADR-021 Phase A, #378). MaxGenerationDepth
	// has no const here — its default is engine.MaxNestingDepth so the depth
	// cap reuses the single existing mechanism rather than forking a value.
	defaultMaxActiveRunsPerRoot         = 100
	defaultMaxDefsPerRoot               = 500
	defaultMaxRegistersPerMinutePerRoot = 60
)

// WorkerConfig defines a config-driven embedded worker handler.
type WorkerConfig struct {
	Task       string
	Exec       string
	HTTP       string
	HTTPMethod string // default: POST
}

// Config holds all server configuration.
type Config struct {
	DataDir         string   `json:"data_dir"`
	HTTPAddr        string   `json:"http_addr"`
	NATSPort        int      `json:"nats_port"`
	LeafRemotes     []string `json:"leaf_remotes"`
	LeafCredentials string   `json:"leaf_credentials"`

	NATSClusterName       string   `json:"nats_cluster_name"`
	NATSClusterRoutes     []string `json:"nats_cluster_routes"`
	NATSClusterAuthToken  string   `json:"nats_cluster_auth_token"`
	NATSJetStreamReplicas int      `json:"nats_jetstream_replicas"`

	MonitorPort   int   `json:"monitor_port"`
	MaxStoreBytes int64 `json:"max_store_bytes"`
	// MaxMemoryBytes caps the JetStream in-memory store
	// (JetStreamMaxMemory) and is applied as the soft Go memory limit at
	// startup (#441). Defaults to defaultMaxMemoryBytes; <= 0 disables the
	// JetStream cap and the Go limit.
	MaxMemoryBytes int64          `json:"max_memory_bytes"`
	Workers        []WorkerConfig `json:"workers"`
	OTLPEndpoint   string         `json:"otlp_endpoint"`

	// RunsMaxAge is the opt-in run-retention window for the
	// workflow_runs KV (#453). Zero (the default) DISABLES pruning
	// entirely — upgrading never silently deletes anyone's runs.
	// When > 0, terminal runs older than this are dropped
	// (delete-only) by the orchestrator's background sweeper. Set
	// via DAGNATS_RUNS_MAX_AGE, which accepts a Go duration ("720h")
	// or a d/w suffix ("30d", "2w") for operator ergonomics.
	RunsMaxAge time.Duration `json:"runs_max_age"`

	// Per-runtime safety bounds (ADR-021 Phase A, #378). These cap a
	// single spawn-tree's resource use so a runaway agent loop cannot
	// fork-bomb the orchestrator. Zero means DEFAULT, not "unlimited" — it
	// resolves to the default consts in internal/api at service
	// construction (additive: an old config file silently inherits the
	// defaults; a safety layer has no "off" switch). #380 owns grant
	// policy; #378 only reads these numeric values.
	//
	// MaxActiveRunsPerRoot caps non-terminal runs sharing a tree-root.
	// MaxDefsPerRoot caps ephemeral defs registered under a tree-root.
	// MaxGenerationDepth caps spawn-chain nesting depth (reuses the
	// existing depth check; default equals engine.MaxNestingDepth).
	// MaxRegistersPerMinutePerRoot rate-limits runtime def registration.
	MaxActiveRunsPerRoot         int `json:"max_active_runs_per_root"`
	MaxDefsPerRoot               int `json:"max_defs_per_root"`
	MaxGenerationDepth           int `json:"max_generation_depth"`
	MaxRegistersPerMinutePerRoot int `json:"max_registers_per_minute_per_root"`

	// NATSWebsocketPort enables an embedded NATS WebSocket
	// listener for browser clients when > 0. 0 (default)
	// disables it — the safe production posture. See ADR-020.
	NATSWebsocketPort int `json:"nats_ws_port"`

	// NATSWebsocketNoTLS turns off TLS for the WebSocket
	// listener. Until top-level NATS TLS is wired this is
	// required when NATSWebsocketPort > 0; the explicit
	// opt-in keeps operators from shipping cleartext to
	// production by accident.
	NATSWebsocketNoTLS bool `json:"nats_ws_no_tls"`

	// FailOnPortConflict makes startup return an error (non-zero exit)
	// instead of auto-falling-back to an ephemeral port when the default
	// NATS port or default HTTP address is already in use. Default false
	// keeps auto-fallback as the documented behavior (#370). Opt-in for
	// operators who want a hard failure when a stale server holds the
	// port.
	FailOnPortConflict bool `json:"fail_on_port_conflict"`

	// Build is the binary's version/revision string, threaded from
	// cli.Version by the serve command (ldflags-stamped). Empty for
	// un-stamped local builds — the console footer degrades empty to
	// the honest "dev" marker (consoleBuildLabel). Not persisted to
	// dagnats.yaml; it is link-time identity, not user config.
	Build string `json:"-"`

	// ConfigFilePath is the absolute path of the dagnats.yaml that
	// was loaded (empty when no file was found). Phase 4 / ADR-018:
	// the server uses it to drive the configfile.Watcher for live
	// reload of workflows and triggers declared in the same file.
	// Not stored in the on-disk file itself — populated by the CLI
	// from the resolved path after the file is loaded.
	ConfigFilePath string `json:"-"`

	// DieWithParent makes a spawned `dagnats serve` self-terminate via
	// the normal graceful shutdown when its parent process dies (#476).
	// Default OFF. Opt-in for sidecar spawners (notify's e2e tests, the
	// eventbus sidecar) whose own cleanup can't run on SIGKILL /
	// `go test -timeout`, so they'd otherwise orphan the server. The
	// CLI's --die-with-parent flag sets it; not persisted to
	// dagnats.yaml — it's an invocation mode, not stored config.
	DieWithParent bool `json:"-"`
}

// DefaultConfig returns platform-appropriate defaults.
// Panics if dataDir resolves empty.
func DefaultConfig() Config {
	if runtime.GOOS == "" {
		panic("runtime.GOOS is empty")
	}

	var dataDir string
	home, err := os.UserHomeDir()
	if err != nil {
		panic(fmt.Sprintf("os.UserHomeDir() failed: %v", err))
	}

	if runtime.GOOS == "darwin" {
		dataDir = filepath.Join(home, "Library", "Application Support", "dagnats")
	} else {
		// Linux: XDG_DATA_HOME or ~/.local/share/
		xdgDataHome := os.Getenv("XDG_DATA_HOME")
		if xdgDataHome != "" {
			dataDir = filepath.Join(xdgDataHome, "dagnats")
		} else {
			dataDir = filepath.Join(home, ".local", "share", "dagnats")
		}
	}

	if dataDir == "" {
		panic("dataDir resolved to empty string")
	}

	return Config{
		DataDir:                      dataDir,
		HTTPAddr:                     defaultHTTPAddr,
		NATSPort:                     defaultNATSPort,
		LeafRemotes:                  nil,
		MaxStoreBytes:                defaultMaxStoreBytes,
		MaxMemoryBytes:               defaultMaxMemoryBytes,
		MaxActiveRunsPerRoot:         defaultMaxActiveRunsPerRoot,
		MaxDefsPerRoot:               defaultMaxDefsPerRoot,
		MaxGenerationDepth:           engine.MaxNestingDepth,
		MaxRegistersPerMinutePerRoot: defaultMaxRegistersPerMinutePerRoot,
	}
}

// ConfigFromEnv loads config from defaults, config file, then env vars.
// Config file is dagnats.yaml in CWD. Missing file is not an error.
// Panics if DataDir is empty or MaxStoreBytes <= 0 after resolution.
func ConfigFromEnv() Config {
	cfg, _, err := ConfigWithPath("")
	if err != nil {
		log.Fatalf("config load failed: %v", err)
	}
	return cfg
}

// ConfigWithPath loads config using an explicit path or standard search.
// Returns the resolved config and the path of the file that was loaded
// (empty string if no file was found). When configPath is non-empty, the
// file must exist or an error is returned.
// Panics if DataDir is empty or MaxStoreBytes <= 0 after resolution.
func ConfigWithPath(
	configPath string,
) (Config, string, error) {
	if len(configPath) > 4096 {
		panic("ConfigWithPath: configPath exceeds max length")
	}

	cfg := DefaultConfig()

	loadedPath, err := resolveAndLoadConfig(
		configPath, &cfg,
	)
	if err != nil {
		return Config{}, "", err
	}

	applyEnvOverrides(&cfg)

	if err := applyRunsMaxAgeEnv(&cfg); err != nil {
		return Config{}, "", err
	}

	if err := applyRuntimeBoundsEnv(&cfg); err != nil {
		return Config{}, "", err
	}

	if len(cfg.Workers) > 0 {
		if err := validateWorkerConfigs(cfg.Workers); err != nil {
			return Config{}, "", fmt.Errorf(
				"invalid worker config: %w", err,
			)
		}
	}

	validateClusterConfig(&cfg)

	if cfg.DataDir == "" {
		panic("DataDir is empty after config resolution")
	}
	if cfg.MaxStoreBytes <= 0 {
		panic(fmt.Sprintf(
			"MaxStoreBytes <= 0: %d", cfg.MaxStoreBytes,
		))
	}

	return cfg, loadedPath, nil
}

// resolveAndLoadConfig picks the config file and loads it into cfg.
// When explicit is non-empty, that file must exist. Otherwise,
// standard directories are searched in priority order.
func resolveAndLoadConfig(
	explicit string, cfg *Config,
) (string, error) {
	if cfg == nil {
		panic("resolveAndLoadConfig: cfg is nil")
	}

	if explicit != "" {
		return loadExplicitConfig(explicit, cfg)
	}

	return loadFirstFoundConfig(cfg)
}

// loadExplicitConfig loads a user-specified config file.
// Returns an error if the file does not exist.
func loadExplicitConfig(
	path string, cfg *Config,
) (string, error) {
	if cfg == nil {
		panic("loadExplicitConfig: cfg is nil")
	}
	if path == "" {
		panic("loadExplicitConfig: path is empty")
	}

	_, err := os.Stat(path)
	if err != nil {
		return "", fmt.Errorf(
			"config file not found: %s", path,
		)
	}

	if err := loadConfigFile(path, cfg); err != nil {
		return "", fmt.Errorf(
			"load config file: %w", err,
		)
	}
	return path, nil
}

// loadFirstFoundConfig searches standard directories for a config
// file and loads the first one found. Returns "" if none found.
func loadFirstFoundConfig(
	cfg *Config,
) (string, error) {
	if cfg == nil {
		panic("loadFirstFoundConfig: cfg is nil")
	}

	candidates := configSearchPaths()
	for _, path := range candidates {
		if _, err := os.Stat(path); err != nil {
			continue
		}
		if err := loadConfigFile(path, cfg); err != nil {
			return "", fmt.Errorf(
				"load config file %s: %w", path, err,
			)
		}
		return path, nil
	}
	return "", nil
}

// configSearchPaths returns the ordered list of config file
// locations to search. CWD first, then XDG, then /etc on Linux.
func configSearchPaths() []string {
	const maxPaths = 4
	paths := make([]string, 0, maxPaths)

	// 1. Current working directory
	paths = append(paths, "dagnats.yaml")

	// 2. XDG_CONFIG_HOME or ~/.config
	xdgConfigHome := os.Getenv("XDG_CONFIG_HOME")
	if xdgConfigHome == "" {
		home, err := os.UserHomeDir()
		if err == nil {
			xdgConfigHome = filepath.Join(
				home, ".config",
			)
		}
	}
	if xdgConfigHome != "" {
		paths = append(paths, filepath.Join(
			xdgConfigHome, "dagnats", "dagnats.yaml",
		))
	}

	// 3. System-wide on Linux only
	if runtime.GOOS == "linux" {
		paths = append(paths, filepath.Join(
			"/etc", "dagnats", "dagnats.yaml",
		))
	}

	return paths
}

// applyEnvOverrides applies environment variable overrides to cfg.
func applyEnvOverrides(cfg *Config) {
	if cfg == nil {
		panic("applyEnvOverrides: cfg is nil")
	}

	if val := os.Getenv("DAGNATS_DATA_DIR"); val != "" {
		cfg.DataDir = val
	}
	if val := os.Getenv("DAGNATS_HTTP_ADDR"); val != "" {
		cfg.HTTPAddr = val
	}
	if val := os.Getenv("DAGNATS_NATS_PORT"); val != "" {
		if port, err := strconv.Atoi(val); err == nil {
			cfg.NATSPort = port
		}
	}
	if val := os.Getenv("DAGNATS_LEAF_REMOTES"); val != "" {
		remotes := strings.Split(val, ",")
		if len(remotes) > maxLeafRemotes {
			remotes = remotes[:maxLeafRemotes]
		}
		cfg.LeafRemotes = remotes
	}
	if val := os.Getenv("DAGNATS_LEAF_CREDENTIALS"); val != "" {
		cfg.LeafCredentials = val
	}
	if val := os.Getenv("DAGNATS_NATS_CLUSTER_NAME"); val != "" {
		cfg.NATSClusterName = val
	}
	if val := os.Getenv("DAGNATS_NATS_CLUSTER_ROUTES"); val != "" {
		routes := strings.Split(val, ",")
		for i := range routes {
			routes[i] = strings.TrimSpace(routes[i])
		}
		if len(routes) > maxClusterRoutes {
			routes = routes[:maxClusterRoutes]
		}
		cfg.NATSClusterRoutes = routes
	}
	if val := os.Getenv("DAGNATS_NATS_CLUSTER_AUTH_TOKEN"); val != "" {
		cfg.NATSClusterAuthToken = val
	}
	if val := os.Getenv("DAGNATS_NATS_JETSTREAM_REPLICAS"); val != "" {
		if r, err := strconv.Atoi(val); err == nil {
			cfg.NATSJetStreamReplicas = r
		}
	}
	if val := os.Getenv("DAGNATS_MONITOR_PORT"); val != "" {
		if port, err := strconv.Atoi(val); err == nil {
			cfg.MonitorPort = port
		}
	}
	if val := os.Getenv("DAGNATS_MAX_STORE_BYTES"); val != "" {
		if maxBytes, err := strconv.ParseInt(val, 10, 64); err == nil {
			cfg.MaxStoreBytes = maxBytes
		}
	}
	if val := os.Getenv("DAGNATS_MAX_MEMORY_BYTES"); val != "" {
		if maxBytes, err := strconv.ParseInt(val, 10, 64); err == nil {
			cfg.MaxMemoryBytes = maxBytes
		}
	}
	if val := os.Getenv("DAGNATS_NATS_WS_PORT"); val != "" {
		if port, err := strconv.Atoi(val); err == nil {
			cfg.NATSWebsocketPort = port
		}
	}
	if val := os.Getenv("DAGNATS_NATS_WS_NO_TLS"); val != "" {
		// Accept the conventional truthy values; anything
		// else leaves the existing (false) default.
		switch strings.ToLower(val) {
		case "1", "true", "yes", "on":
			cfg.NATSWebsocketNoTLS = true
		}
	}

	if val := os.Getenv(
		"OTEL_EXPORTER_OTLP_ENDPOINT",
	); val != "" {
		cfg.OTLPEndpoint = val
	}

	// Override worker configs from env vars
	for i := range cfg.Workers {
		envTask := strings.ToUpper(
			strings.ReplaceAll(
				cfg.Workers[i].Task, "-", "_",
			),
		)
		if val := os.Getenv(
			"DAGNATS_WORKER_" + envTask + "_EXEC",
		); val != "" {
			cfg.Workers[i].Exec = val
		}
		if val := os.Getenv(
			"DAGNATS_WORKER_" + envTask + "_HTTP",
		); val != "" {
			cfg.Workers[i].HTTP = val
		}
		if val := os.Getenv(
			"DAGNATS_WORKER_" + envTask + "_HTTP_METHOD",
		); val != "" {
			cfg.Workers[i].HTTPMethod = val
		}
	}
}

// applyRunsMaxAgeEnv resolves DAGNATS_RUNS_MAX_AGE into cfg.RunsMaxAge (#453).
// Unset or empty leaves the disabled default. An explicit "0" stays disabled.
// A valid window enables the sweeper; an invalid value is a hard error so a
// typo fails fast at config load rather than silently disabling retention.
func applyRunsMaxAgeEnv(cfg *Config) error {
	if cfg == nil {
		panic("applyRunsMaxAgeEnv: cfg is nil")
	}
	val := os.Getenv("DAGNATS_RUNS_MAX_AGE")
	if val == "" {
		return nil
	}
	dur, err := parseRetentionDuration(val)
	if err != nil {
		return fmt.Errorf("invalid DAGNATS_RUNS_MAX_AGE %q: %w", val, err)
	}
	if dur < 0 {
		return fmt.Errorf(
			"invalid DAGNATS_RUNS_MAX_AGE %q: must not be negative", val,
		)
	}
	cfg.RunsMaxAge = dur
	return nil
}

// applyRuntimeBoundsEnv resolves the four per-runtime bound env vars (#378)
// through the SAME validating parsers the config-file path uses, so a
// negative value or a depth above the engine ceiling is a hard load error
// rather than a silent pass that would disable the cap (negatives) or open
// the ghost-run hole (loose depth). Unset vars leave the resolved defaults.
func applyRuntimeBoundsEnv(cfg *Config) error {
	if cfg == nil {
		panic("applyRuntimeBoundsEnv: cfg is nil")
	}
	type boundEnv struct {
		env   string
		parse func(string) (int, error)
		set   func(*Config, int)
	}
	atoi := func(key string) func(string) (int, error) {
		return func(v string) (int, error) { return atoiNonNegative(v, key) }
	}
	binds := []boundEnv{
		{"DAGNATS_MAX_ACTIVE_RUNS_PER_ROOT",
			atoi("max_active_runs_per_root"),
			func(c *Config, n int) { c.MaxActiveRunsPerRoot = n }},
		{"DAGNATS_MAX_DEFS_PER_ROOT",
			atoi("max_defs_per_root"),
			func(c *Config, n int) { c.MaxDefsPerRoot = n }},
		{"DAGNATS_MAX_GENERATION_DEPTH",
			parseGenerationDepth,
			func(c *Config, n int) { c.MaxGenerationDepth = n }},
		{"DAGNATS_MAX_REGISTERS_PER_MINUTE_PER_ROOT",
			atoi("max_registers_per_minute_per_root"),
			func(c *Config, n int) { c.MaxRegistersPerMinutePerRoot = n }},
	}
	for _, b := range binds {
		val := os.Getenv(b.env)
		if val == "" {
			continue
		}
		n, err := b.parse(val)
		if err != nil {
			return fmt.Errorf("env %s: %w", b.env, err)
		}
		b.set(cfg, n)
	}
	return nil
}

// parseRetentionDuration parses a retention window. It accepts any Go
// duration string ("720h", "30m") plus the operator-friendly "<n>d" (days)
// and "<n>w" (weeks) suffixes. The numeric portion must be a non-negative
// integer for the d/w forms.
func parseRetentionDuration(val string) (time.Duration, error) {
	if val == "" {
		panic("parseRetentionDuration: val must not be empty")
	}
	last := val[len(val)-1]
	if last != 'd' && last != 'w' {
		return time.ParseDuration(val)
	}
	count, err := strconv.Atoi(val[:len(val)-1])
	if err != nil {
		return 0, fmt.Errorf("bad %c-suffixed duration: %w", last, err)
	}
	if count < 0 {
		return 0, fmt.Errorf("duration must not be negative")
	}
	unit := 24 * time.Hour
	if last == 'w' {
		unit = 7 * 24 * time.Hour
	}
	return time.Duration(count) * unit, nil
}

// loadConfigFile reads a simple key: value config file and applies it to cfg.
// Missing file is not an error. Unknown keys are logged as warnings.
// File is bounded at maxConfigFileLines lines.
// Panics if cfg is nil or path is empty.
func loadConfigFile(path string, cfg *Config) error {
	if cfg == nil {
		panic("loadConfigFile: cfg is nil")
	}
	if path == "" {
		panic("loadConfigFile: path is empty")
	}

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // Missing file is not an error
		}
		return fmt.Errorf("open config file: %w", err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0

	for scanner.Scan() && lineNum < maxConfigFileLines {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Parse key: value
		parts := strings.SplitN(line, ":", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])

		if err := applyConfigValue(key, val, lineNum, cfg); err != nil {
			// Unknown keys are tolerated (forward-compat with newer
			// config schemas); a KNOWN key with an invalid value is a
			// hard error so a typo fails fast at load instead of
			// silently falling back to the default (matches the env
			// path for runs_max_age etc.).
			if errors.Is(err, errUnknownConfigKey) {
				log.Printf("Warning: line %d: %v", lineNum, err)
				continue
			}
			return fmt.Errorf("config line %d: %w", lineNum, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan config file: %w", err)
	}

	return nil
}

// applyConfigValue applies a single config key-value pair to cfg.
// Returns error for unknown keys or parse failures.
func applyConfigValue(key, val string, lineNum int, cfg *Config) error {
	if cfg == nil {
		panic("applyConfigValue: cfg is nil")
	}
	if lineNum <= 0 {
		panic(fmt.Sprintf("applyConfigValue: invalid lineNum %d", lineNum))
	}

	switch key {
	case "data_dir":
		cfg.DataDir = val
	case "http_addr":
		cfg.HTTPAddr = val
	case "nats_port":
		port, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("invalid nats_port: %w", err)
		}
		cfg.NATSPort = port
	case "leaf_remotes":
		remotes := strings.Split(val, ",")
		for i := range remotes {
			remotes[i] = strings.TrimSpace(remotes[i])
		}
		if len(remotes) > maxLeafRemotes {
			remotes = remotes[:maxLeafRemotes]
		}
		cfg.LeafRemotes = remotes
	case "leaf_credentials":
		cfg.LeafCredentials = val
	case "nats_cluster_name":
		cfg.NATSClusterName = val
	case "nats_cluster_routes":
		routes := strings.Split(val, ",")
		for i := range routes {
			routes[i] = strings.TrimSpace(routes[i])
		}
		if len(routes) > maxClusterRoutes {
			routes = routes[:maxClusterRoutes]
		}
		cfg.NATSClusterRoutes = routes
	case "nats_cluster_auth_token":
		cfg.NATSClusterAuthToken = val
	case "nats_jetstream_replicas":
		r, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("invalid nats_jetstream_replicas: %w", err)
		}
		cfg.NATSJetStreamReplicas = r
	case "monitor_port":
		port, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("invalid monitor_port: %w", err)
		}
		cfg.MonitorPort = port
	case "max_store_bytes":
		maxBytes, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid max_store_bytes: %w", err)
		}
		cfg.MaxStoreBytes = maxBytes
	case "max_memory_bytes":
		maxBytes, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid max_memory_bytes: %w", err)
		}
		cfg.MaxMemoryBytes = maxBytes
	case "runs_max_age":
		dur, err := parseRetentionDuration(val)
		if err != nil {
			return fmt.Errorf("invalid runs_max_age: %w", err)
		}
		if dur < 0 {
			return fmt.Errorf(
				"invalid runs_max_age: must not be negative")
		}
		cfg.RunsMaxAge = dur
	case "max_active_runs_per_root":
		n, err := atoiNonNegative(val, "max_active_runs_per_root")
		if err != nil {
			return err
		}
		cfg.MaxActiveRunsPerRoot = n
	case "max_defs_per_root":
		n, err := atoiNonNegative(val, "max_defs_per_root")
		if err != nil {
			return err
		}
		cfg.MaxDefsPerRoot = n
	case "max_generation_depth":
		n, err := parseGenerationDepth(val)
		if err != nil {
			return err
		}
		cfg.MaxGenerationDepth = n
	case "max_registers_per_minute_per_root":
		n, err := atoiNonNegative(val, "max_registers_per_minute_per_root")
		if err != nil {
			return err
		}
		cfg.MaxRegistersPerMinutePerRoot = n
	case "nats_ws_port":
		port, err := strconv.Atoi(val)
		if err != nil {
			return fmt.Errorf("invalid nats_ws_port: %w", err)
		}
		cfg.NATSWebsocketPort = port
	case "nats_ws_no_tls":
		switch strings.ToLower(val) {
		case "1", "true", "yes", "on":
			cfg.NATSWebsocketNoTLS = true
		case "0", "false", "no", "off", "":
			cfg.NATSWebsocketNoTLS = false
		default:
			return fmt.Errorf(
				"invalid nats_ws_no_tls: %q", val)
		}
	case "otlp_endpoint":
		cfg.OTLPEndpoint = val
	default:
		if strings.HasPrefix(key, "worker.") {
			return applyWorkerConfigValue(key, val, cfg)
		}
		return fmt.Errorf("%w: %s", errUnknownConfigKey, key)
	}

	return nil
}

// atoiNonNegative parses a non-negative int config value, rejecting both
// malformed and negative input with a key-named wrapped error. A negative
// bound is meaningless (it would treat every request as over-quota); a
// hard error at load time fails fast on the typo rather than silently
// bricking runtime registration.
func atoiNonNegative(val, key string) (int, error) {
	n, err := strconv.Atoi(val)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %w", key, err)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid %s: must not be negative", key)
	}
	return n, nil
}

// parseGenerationDepth parses max_generation_depth and rejects loosening
// past the orchestrator's hard ceiling. The orchestrator's handleWorkflowSpawn
// enforces engine.MaxNestingDepth unconditionally; config that exceeds it
// would make the API spawn-gate pass while the orchestrator silently drops
// the spawn — handing the caller a runID for a run that is never created
// (a ghost run). So config may only TIGHTEN: a value in 0..engine.MaxNestingDepth
// is accepted (0 means "unset" and resolves to the ceiling default later);
// anything above the ceiling is rejected. The API gate and the orchestrator
// ceiling can then never diverge.
func parseGenerationDepth(val string) (int, error) {
	n, err := atoiNonNegative(val, "max_generation_depth")
	if err != nil {
		return 0, err
	}
	if n > engine.MaxNestingDepth {
		return 0, fmt.Errorf(
			"invalid max_generation_depth: %d exceeds engine ceiling %d "+
				"(config may only tighten the cap, not loosen it)",
			n, engine.MaxNestingDepth)
	}
	return n, nil
}

// applyWorkerConfigValue parses a worker.{task}.{field} key.
func applyWorkerConfigValue(
	key, val string, cfg *Config,
) error {
	if cfg == nil {
		panic("applyWorkerConfigValue: cfg is nil")
	}
	if key == "" {
		panic("applyWorkerConfigValue: key is empty")
	}

	parts := strings.SplitN(key, ".", 3)
	if len(parts) != 3 || parts[0] != "worker" {
		return fmt.Errorf(
			"invalid worker key format: %s", key,
		)
	}

	task := parts[1]
	field := parts[2]

	if task == "" {
		return fmt.Errorf(
			"worker key has empty task: %s", key,
		)
	}

	idx := -1
	for i := range cfg.Workers {
		if cfg.Workers[i].Task == task {
			idx = i
			break
		}
	}
	if idx == -1 {
		if len(cfg.Workers) >= maxWorkerConfigs {
			return fmt.Errorf(
				"max worker configs (%d) exceeded",
				maxWorkerConfigs,
			)
		}
		cfg.Workers = append(
			cfg.Workers, WorkerConfig{Task: task},
		)
		idx = len(cfg.Workers) - 1
	}

	switch field {
	case "exec":
		cfg.Workers[idx].Exec = val
	case "http":
		cfg.Workers[idx].HTTP = val
	case "http_method":
		cfg.Workers[idx].HTTPMethod = val
	default:
		return fmt.Errorf(
			"unknown worker field: %s", field,
		)
	}

	return nil
}

// validateClusterConfig panics with a clear message if cluster config
// is internally inconsistent. TigerStyle: programmer-error invariants
// are panics, not returned errors.
//
// Rules:
//   - Cluster mode is detected by len(NATSClusterRoutes) > 0.
//   - Clustering requires NATSClusterName non-empty.
//   - NATSClusterRoutes must have between 2 and maxClusterRoutes entries.
//   - NATSJetStreamReplicas must be in {0, 1, 3, 5}.
//   - Cluster mode and leaf mode are mutually exclusive.
func validateClusterConfig(cfg *Config) {
	if cfg == nil {
		panic("validateClusterConfig: cfg is nil")
	}

	switch cfg.NATSJetStreamReplicas {
	case 0, 1, 3, 5:
	default:
		panic(fmt.Sprintf(
			"nats_jetstream_replicas must be 0, 1, 3, or 5; got %d",
			cfg.NATSJetStreamReplicas,
		))
	}

	clustered := len(cfg.NATSClusterRoutes) > 0
	if !clustered {
		return
	}

	if cfg.NATSClusterName == "" {
		panic("nats_cluster_name is required when nats_cluster_routes is set")
	}
	if len(cfg.NATSClusterRoutes) < 2 {
		panic(fmt.Sprintf(
			"nats_cluster_routes needs at least 2 entries (3-node minimum); got %d",
			len(cfg.NATSClusterRoutes),
		))
	}
	if len(cfg.NATSClusterRoutes) > maxClusterRoutes {
		panic(fmt.Sprintf(
			"nats_cluster_routes capped at %d; got %d",
			maxClusterRoutes, len(cfg.NATSClusterRoutes),
		))
	}
	if len(cfg.LeafRemotes) > 0 {
		panic("nats_cluster_routes and leaf_remotes are mutually exclusive")
	}
}

// validateWorkerConfigs checks worker config consistency.
func validateWorkerConfigs(
	workers []WorkerConfig,
) error {
	if len(workers) > maxWorkerConfigs {
		panic(
			"validateWorkerConfigs: exceeds max bound",
		)
	}

	seen := make(map[string]bool, len(workers))
	for _, w := range workers {
		if w.Task == "" {
			return fmt.Errorf(
				"worker config: task name is empty",
			)
		}
		if seen[w.Task] {
			return fmt.Errorf(
				"worker config: duplicate task %q",
				w.Task,
			)
		}
		seen[w.Task] = true

		hasExec := w.Exec != ""
		hasHTTP := w.HTTP != ""
		if hasExec && hasHTTP {
			return fmt.Errorf(
				"worker %q: both exec and http set",
				w.Task,
			)
		}
		if !hasExec && !hasHTTP {
			return fmt.Errorf(
				"worker %q: must have exec or http",
				w.Task,
			)
		}
	}
	return nil
}
