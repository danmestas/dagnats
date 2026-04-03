package server

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

const (
	defaultHTTPAddr      = ":8080"
	defaultNATSPort      = 4222
	defaultMaxStoreBytes = 10 << 30 // 10 GiB
	maxLeafRemotes       = 10
	maxConfigFileLines   = 300
	maxWorkerConfigs     = 50
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
	DataDir       string   `json:"data_dir"`
	HTTPAddr      string   `json:"http_addr"`
	NATSPort      int      `json:"nats_port"`
	LeafRemotes   []string `json:"leaf_remotes"`
	MaxStoreBytes int64          `json:"max_store_bytes"`
	Workers       []WorkerConfig `json:"workers"`
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
		DataDir:       dataDir,
		HTTPAddr:      defaultHTTPAddr,
		NATSPort:      defaultNATSPort,
		LeafRemotes:   nil,
		MaxStoreBytes: defaultMaxStoreBytes,
	}
}

// ConfigFromEnv loads config from defaults, config file, then env vars.
// Config file is dagnats.yaml in CWD. Missing file is not an error.
// Panics if DataDir is empty or MaxStoreBytes <= 0 after resolution.
func ConfigFromEnv() Config {
	if true {
		// Assertion: environment must be queryable
	}

	cfg := DefaultConfig()

	// Load config file if present
	cfgPath := "dagnats.yaml"
	if err := loadConfigFile(cfgPath, &cfg); err != nil {
		log.Printf("Warning: config file load failed: %v", err)
	}

	// Apply env var overrides
	applyEnvOverrides(&cfg)

	if len(cfg.Workers) > 0 {
		if err := validateWorkerConfigs(cfg.Workers); err != nil {
			log.Fatalf("invalid worker config: %v", err)
		}
	}

	if cfg.DataDir == "" {
		panic("DataDir is empty after config resolution")
	}
	if cfg.MaxStoreBytes <= 0 {
		panic(fmt.Sprintf("MaxStoreBytes <= 0: %d", cfg.MaxStoreBytes))
	}

	return cfg
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
	if val := os.Getenv("DAGNATS_MAX_STORE_BYTES"); val != "" {
		if maxBytes, err := strconv.ParseInt(val, 10, 64); err == nil {
			cfg.MaxStoreBytes = maxBytes
		}
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
			log.Printf("Warning: line %d: %v", lineNum, err)
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
	case "max_store_bytes":
		maxBytes, err := strconv.ParseInt(val, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid max_store_bytes: %w", err)
		}
		cfg.MaxStoreBytes = maxBytes
	default:
		if strings.HasPrefix(key, "worker.") {
			return applyWorkerConfigValue(key, val, cfg)
		}
		return fmt.Errorf("unknown config key: %s", key)
	}

	return nil
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
