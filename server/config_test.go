// Methodology: Pure unit tests using TDD. Each test verifies config parsing,
// defaults, and env var overrides with explicit positive and negative
// assertions. No NATS or external dependencies. Bounded timeouts not needed.

package server

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/engine"
)

func TestDefaultConfig_HasPlatformDataDir(t *testing.T) {
	cfg := DefaultConfig()

	// Positive: dataDir must not be empty
	if cfg.DataDir == "" {
		t.Fatal("DefaultConfig() returned empty DataDir")
	}

	// Positive: on darwin, must be in ~/Library/Application Support/
	if runtime.GOOS == "darwin" {
		home, err := os.UserHomeDir()
		if err != nil {
			t.Fatalf("os.UserHomeDir() failed: %v", err)
		}
		expectedPrefix := filepath.Join(home, "Library", "Application Support", "dagnats")
		if cfg.DataDir != expectedPrefix {
			t.Errorf("DataDir = %q, want %q", cfg.DataDir, expectedPrefix)
		}
	}
}

func TestDefaultConfig_PortsAndLimits(t *testing.T) {
	cfg := DefaultConfig()

	// Positive: verify all expected defaults
	if cfg.HTTPAddr != defaultHTTPAddr {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, defaultHTTPAddr)
	}
	if cfg.NATSPort != defaultNATSPort {
		t.Errorf("NATSPort = %d, want %d", cfg.NATSPort, defaultNATSPort)
	}
	if cfg.MaxStoreBytes != defaultMaxStoreBytes {
		t.Errorf("MaxStoreBytes = %d, want %d", cfg.MaxStoreBytes, defaultMaxStoreBytes)
	}

	// Negative: LeafRemotes should be empty
	if len(cfg.LeafRemotes) != 0 {
		t.Errorf("LeafRemotes = %v, want empty slice", cfg.LeafRemotes)
	}
}

func TestConfigFromEnv_OverridesDefaults(t *testing.T) {
	// Set all env vars
	t.Setenv("DAGNATS_DATA_DIR", "/tmp/dagnats-test")
	t.Setenv("DAGNATS_HTTP_ADDR", ":9999")
	t.Setenv("DAGNATS_NATS_PORT", "5555")
	t.Setenv("DAGNATS_LEAF_REMOTES", "nats://leaf1:7422,nats://leaf2:7422")
	t.Setenv("DAGNATS_MAX_STORE_BYTES", "1073741824")

	cfg := ConfigFromEnv()

	// Positive: all env vars overridden
	if cfg.DataDir != "/tmp/dagnats-test" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/tmp/dagnats-test")
	}
	if cfg.HTTPAddr != ":9999" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":9999")
	}
	if cfg.NATSPort != 5555 {
		t.Errorf("NATSPort = %d, want %d", cfg.NATSPort, 5555)
	}
	if len(cfg.LeafRemotes) != 2 {
		t.Fatalf("len(LeafRemotes) = %d, want 2", len(cfg.LeafRemotes))
	}
	if cfg.LeafRemotes[0] != "nats://leaf1:7422" {
		t.Errorf("LeafRemotes[0] = %q, want %q", cfg.LeafRemotes[0], "nats://leaf1:7422")
	}
	if cfg.MaxStoreBytes != 1073741824 {
		t.Errorf("MaxStoreBytes = %d, want %d", cfg.MaxStoreBytes, 1073741824)
	}

	// Negative: should not have default values
	if cfg.NATSPort == defaultNATSPort {
		t.Errorf("NATSPort still has default value %d", defaultNATSPort)
	}
}

func TestConfigFromEnv_NoEnvUsesDefaults(t *testing.T) {
	// Clear all env vars
	os.Unsetenv("DAGNATS_DATA_DIR")
	os.Unsetenv("DAGNATS_HTTP_ADDR")
	os.Unsetenv("DAGNATS_NATS_PORT")
	os.Unsetenv("DAGNATS_LEAF_REMOTES")
	os.Unsetenv("DAGNATS_MAX_STORE_BYTES")

	cfg := ConfigFromEnv()

	// Positive: should match DefaultConfig
	defaultCfg := DefaultConfig()
	if cfg.DataDir != defaultCfg.DataDir {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, defaultCfg.DataDir)
	}
	if cfg.HTTPAddr != defaultHTTPAddr {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, defaultHTTPAddr)
	}
	if cfg.NATSPort != defaultNATSPort {
		t.Errorf("NATSPort = %d, want %d", cfg.NATSPort, defaultNATSPort)
	}

	// Negative: LeafRemotes should be empty
	if len(cfg.LeafRemotes) != 0 {
		t.Errorf("LeafRemotes = %v, want empty", cfg.LeafRemotes)
	}
}

func TestConfigFromEnv_LeafRemotesCapped(t *testing.T) {
	// Create 12 leaf remotes
	remotes := make([]string, 12)
	for i := 0; i < 12; i++ {
		remotes[i] = "nats://leaf" + string(rune('0'+i)) + ":7422"
	}
	t.Setenv("DAGNATS_LEAF_REMOTES", strings.Join(remotes, ","))

	cfg := ConfigFromEnv()

	// Positive: should be capped at maxLeafRemotes (10)
	if len(cfg.LeafRemotes) != maxLeafRemotes {
		t.Errorf("len(LeafRemotes) = %d, want %d", len(cfg.LeafRemotes), maxLeafRemotes)
	}

	// Negative: should not have all 12
	if len(cfg.LeafRemotes) == 12 {
		t.Errorf("LeafRemotes not capped, got all 12 remotes")
	}
}

func TestLoadConfigFile_ParsesAllFields(t *testing.T) {
	// Create temp file with all fields
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "test.yaml")
	content := `data_dir: /tmp/test-data
http_addr: :7070
nats_port: 6060
leaf_remotes: nats://remote1:7422,nats://remote2:7422
max_store_bytes: 5368709120
`
	if err := os.WriteFile(cfgPath, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	cfg := DefaultConfig()
	err := loadConfigFile(cfgPath, &cfg)

	// Positive: no error and all fields parsed
	if err != nil {
		t.Fatalf("loadConfigFile() failed: %v", err)
	}
	if cfg.DataDir != "/tmp/test-data" {
		t.Errorf("DataDir = %q, want %q", cfg.DataDir, "/tmp/test-data")
	}
	if cfg.HTTPAddr != ":7070" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":7070")
	}
	if cfg.NATSPort != 6060 {
		t.Errorf("NATSPort = %d, want %d", cfg.NATSPort, 6060)
	}
	if len(cfg.LeafRemotes) != 2 {
		t.Fatalf("len(LeafRemotes) = %d, want 2", len(cfg.LeafRemotes))
	}
	if cfg.MaxStoreBytes != 5368709120 {
		t.Errorf("MaxStoreBytes = %d, want %d", cfg.MaxStoreBytes, 5368709120)
	}

	// Negative: should not have default values
	if cfg.NATSPort == defaultNATSPort {
		t.Errorf("NATSPort unchanged, still %d", defaultNATSPort)
	}
}

func TestLoadConfigFile_MissingFileIsNotError(t *testing.T) {
	cfg := DefaultConfig()
	origDataDir := cfg.DataDir

	err := loadConfigFile("/nonexistent/path/to/config.yaml", &cfg)

	// Positive: no error returned
	if err != nil {
		t.Errorf("loadConfigFile() with missing file returned error: %v", err)
	}

	// Negative: config should be unchanged
	if cfg.DataDir != origDataDir {
		t.Errorf("DataDir changed from %q to %q", origDataDir, cfg.DataDir)
	}
}

func TestLoadConfigFile_UnknownKeysIgnored(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "test.yaml")
	content := `unknown_key: some_value
http_addr: :8888
another_unknown: 12345
`
	if err := os.WriteFile(cfgPath, []byte(content), 0o600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	cfg := DefaultConfig()
	err := loadConfigFile(cfgPath, &cfg)

	// Positive: no error and known key parsed
	if err != nil {
		t.Fatalf("loadConfigFile() failed: %v", err)
	}
	if cfg.HTTPAddr != ":8888" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, ":8888")
	}

	// Negative: unknown keys should not cause failure
	if err != nil {
		t.Errorf("Unknown keys caused error: %v", err)
	}
}

func TestLoadConfigFile_ParsesWorkerEntries(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "dagnats.yaml")
	content := "worker.run-tests.exec: go test ./...\n" +
		"worker.notify.http: https://example.com/hook\n" +
		"worker.check.http: https://example.com/check\n" +
		"worker.check.http_method: PUT\n"
	if err := os.WriteFile(
		cfgPath, []byte(content), 0644,
	); err != nil {
		t.Fatal(err)
	}

	cfg := DefaultConfig()
	if err := loadConfigFile(cfgPath, &cfg); err != nil {
		t.Fatalf("loadConfigFile: %v", err)
	}

	// Positive: 3 workers parsed
	if len(cfg.Workers) != 3 {
		t.Fatalf("Workers = %d, want 3",
			len(cfg.Workers))
	}

	// Positive: exec worker correct
	found := false
	for _, w := range cfg.Workers {
		if w.Task == "run-tests" {
			found = true
			if w.Exec != "go test ./..." {
				t.Errorf("Exec = %q, want %q",
					w.Exec, "go test ./...")
			}
			if w.HTTP != "" {
				t.Errorf("HTTP = %q, want empty",
					w.HTTP)
			}
		}
	}
	if !found {
		t.Error("worker 'run-tests' not found")
	}

	// Positive: http worker with method
	for _, w := range cfg.Workers {
		if w.Task == "check" {
			if w.HTTPMethod != "PUT" {
				t.Errorf("HTTPMethod = %q, want %q",
					w.HTTPMethod, "PUT")
			}
		}
	}
}

func TestValidateWorkerConfigs_RejectsDuplicates(
	t *testing.T,
) {
	workers := []WorkerConfig{
		{Task: "dup", Exec: "echo a"},
		{Task: "dup", Exec: "echo b"},
	}
	err := validateWorkerConfigs(workers)
	// Positive: error returned
	if err == nil {
		t.Fatal("expected error for duplicates")
	}
	// Positive: error mentions duplicate
	if !strings.Contains(err.Error(), "duplicate") {
		t.Errorf("error = %q, want 'duplicate'",
			err.Error())
	}
}

func TestValidateWorkerConfigs_RejectsBothExecAndHTTP(
	t *testing.T,
) {
	workers := []WorkerConfig{
		{Task: "bad", Exec: "echo", HTTP: "http://x"},
	}
	err := validateWorkerConfigs(workers)
	// Positive: error returned
	if err == nil {
		t.Fatal("expected error for both exec and http")
	}
	// Negative: no panic
	if strings.Contains(err.Error(), "panic") {
		t.Error("unexpected panic in error message")
	}
}

func TestValidateWorkerConfigs_RejectsNeitherExecNorHTTP(
	t *testing.T,
) {
	workers := []WorkerConfig{
		{Task: "empty"},
	}
	err := validateWorkerConfigs(workers)
	// Positive: error returned
	if err == nil {
		t.Fatal("expected error for neither exec nor http")
	}
	// Positive: error mentions must have
	if !strings.Contains(err.Error(), "must have") {
		t.Errorf("error = %q, want 'must have'",
			err.Error())
	}
}

func TestConfigLeafCredentials(t *testing.T) {
	// Positive: default is empty
	cfg := DefaultConfig()
	if cfg.LeafCredentials != "" {
		t.Fatalf(
			"default LeafCredentials = %q, want empty",
			cfg.LeafCredentials,
		)
	}

	// Positive: env var sets it
	t.Setenv("DAGNATS_LEAF_CREDENTIALS", "/tmp/ngs.creds")
	cfg2 := DefaultConfig()
	applyEnvOverrides(&cfg2)
	if cfg2.LeafCredentials != "/tmp/ngs.creds" {
		t.Fatalf(
			"LeafCredentials = %q, want /tmp/ngs.creds",
			cfg2.LeafCredentials,
		)
	}
}

func TestConfigFileLeafCredentials(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "test.yaml")
	os.WriteFile(cfgFile, []byte(
		"leaf_credentials: /etc/nats/leaf.creds\n",
	), 0644)

	cfg := DefaultConfig()
	if err := loadConfigFile(cfgFile, &cfg); err != nil {
		t.Fatalf("loadConfigFile: %v", err)
	}
	// Positive: value loaded from file
	if cfg.LeafCredentials != "/etc/nats/leaf.creds" {
		t.Fatalf(
			"LeafCredentials = %q, want /etc/nats/leaf.creds",
			cfg.LeafCredentials,
		)
	}
}

func TestConfigMonitorPort(t *testing.T) {
	// Positive: default is 0 (disabled)
	cfg := DefaultConfig()
	if cfg.MonitorPort != 0 {
		t.Fatalf(
			"default MonitorPort = %d, want 0",
			cfg.MonitorPort,
		)
	}

	// Positive: env var sets it
	t.Setenv("DAGNATS_MONITOR_PORT", "8222")
	cfg2 := DefaultConfig()
	applyEnvOverrides(&cfg2)
	if cfg2.MonitorPort != 8222 {
		t.Fatalf(
			"MonitorPort = %d, want 8222",
			cfg2.MonitorPort,
		)
	}
}

func TestConfigFileMonitorPort(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "test.yaml")
	os.WriteFile(cfgFile, []byte(
		"monitor_port: 8222\n",
	), 0644)

	cfg := DefaultConfig()
	if err := loadConfigFile(cfgFile, &cfg); err != nil {
		t.Fatalf("loadConfigFile: %v", err)
	}
	// Positive: value loaded from file
	if cfg.MonitorPort != 8222 {
		t.Fatalf(
			"MonitorPort = %d, want 8222",
			cfg.MonitorPort,
		)
	}
}

func TestApplyEnvOverrides_NATSCluster(t *testing.T) {
	t.Setenv("DAGNATS_NATS_CLUSTER_NAME", "dagnats-prod")
	t.Setenv("DAGNATS_NATS_CLUSTER_ROUTES", "nats://node-1:6222,nats://node-2:6222")
	t.Setenv("DAGNATS_NATS_CLUSTER_AUTH_TOKEN", "secret-tok")
	t.Setenv("DAGNATS_NATS_JETSTREAM_REPLICAS", "3")

	cfg := DefaultConfig()
	applyEnvOverrides(&cfg)

	if cfg.NATSClusterName != "dagnats-prod" {
		t.Errorf("NATSClusterName = %q, want %q", cfg.NATSClusterName, "dagnats-prod")
	}
	if got := cfg.NATSClusterRoutes; len(got) != 2 || got[0] != "nats://node-1:6222" {
		t.Errorf("NATSClusterRoutes = %v", got)
	}
	if cfg.NATSClusterAuthToken != "secret-tok" {
		t.Errorf("NATSClusterAuthToken = %q", cfg.NATSClusterAuthToken)
	}
	if cfg.NATSJetStreamReplicas != 3 {
		t.Errorf("NATSJetStreamReplicas = %d, want 3", cfg.NATSJetStreamReplicas)
	}
}

func TestApplyConfigValue_NATSCluster(t *testing.T) {
	cfg := DefaultConfig()
	cases := []struct {
		key, val string
		check    func(*testing.T, *Config)
	}{
		{"nats_cluster_name", "dagnats-staging", func(t *testing.T, c *Config) {
			if c.NATSClusterName != "dagnats-staging" {
				t.Errorf("NATSClusterName = %q", c.NATSClusterName)
			}
		}},
		{"nats_cluster_routes", "nats://a:6222, nats://b:6222", func(t *testing.T, c *Config) {
			if len(c.NATSClusterRoutes) != 2 {
				t.Fatalf("want 2 routes, got %v", c.NATSClusterRoutes)
			}
		}},
		{"nats_cluster_auth_token", "tok", func(t *testing.T, c *Config) {
			if c.NATSClusterAuthToken != "tok" {
				t.Errorf("token = %q", c.NATSClusterAuthToken)
			}
		}},
		{"nats_jetstream_replicas", "5", func(t *testing.T, c *Config) {
			if c.NATSJetStreamReplicas != 5 {
				t.Errorf("replicas = %d", c.NATSJetStreamReplicas)
			}
		}},
	}
	for _, tc := range cases {
		if err := applyConfigValue(tc.key, tc.val, 1, &cfg); err != nil {
			t.Fatalf("applyConfigValue(%s, %s): %v", tc.key, tc.val, err)
		}
		tc.check(t, &cfg)
	}
}

func TestValidateClusterConfig(t *testing.T) {
	cases := []struct {
		name      string
		mut       func(*Config)
		wantPanic string // substring match on panic message
	}{
		{
			name: "cluster requires name",
			mut: func(c *Config) {
				c.NATSClusterRoutes = []string{"nats://a:6222", "nats://b:6222"}
			},
			wantPanic: "nats_cluster_name",
		},
		{
			name: "cluster requires at least 2 routes (3-node minimum)",
			mut: func(c *Config) {
				c.NATSClusterName = "x"
				c.NATSClusterRoutes = []string{"nats://a:6222"}
			},
			wantPanic: "nats_cluster_routes",
		},
		{
			name: "replicas must be 0, 1, 3, or 5",
			mut: func(c *Config) {
				c.NATSJetStreamReplicas = 4
			},
			wantPanic: "nats_jetstream_replicas",
		},
		{
			name: "negative replicas rejected",
			mut: func(c *Config) {
				c.NATSJetStreamReplicas = -1
			},
			wantPanic: "nats_jetstream_replicas",
		},
		{
			name: "routes over cap rejected",
			mut: func(c *Config) {
				c.NATSClusterName = "x"
				routes := make([]string, 11)
				for i := 0; i < 11; i++ {
					routes[i] = fmt.Sprintf("nats://node-%d:6222", i)
				}
				c.NATSClusterRoutes = routes
			},
			wantPanic: "nats_cluster_routes",
		},
		{
			name: "leaf and cluster mutually exclusive",
			mut: func(c *Config) {
				c.NATSClusterName = "x"
				c.NATSClusterRoutes = []string{"nats://a:6222", "nats://b:6222"}
				c.LeafRemotes = []string{"nats://hub:7422"}
			},
			wantPanic: "leaf_remotes",
		},
		{
			name: "valid clustered config",
			mut: func(c *Config) {
				c.NATSClusterName = "dagnats"
				c.NATSClusterRoutes = []string{
					"nats://a:6222",
					"nats://b:6222",
				}
				c.NATSJetStreamReplicas = 3
			},
			wantPanic: "",
		},
		{
			name:      "valid standalone config",
			mut:       func(c *Config) {},
			wantPanic: "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := DefaultConfig()
			tc.mut(&cfg)

			defer func() {
				r := recover()
				if tc.wantPanic == "" {
					if r != nil {
						t.Errorf("unexpected panic: %v", r)
					}
					return
				}
				if r == nil {
					t.Errorf("expected panic containing %q, got none", tc.wantPanic)
					return
				}
				if !strings.Contains(fmt.Sprint(r), tc.wantPanic) {
					t.Errorf("panic = %v, want substring %q", r, tc.wantPanic)
				}
			}()

			validateClusterConfig(&cfg)
		})
	}
}

// TestRuntimeBounds_Defaults proves the per-runtime bound defaults (#378)
// resolve to the ratified values and that the depth default is the SAME
// value as engine.MaxNestingDepth — guarding the const->config reuse so a
// future bump of MaxNestingDepth keeps the default in lock-step.
func TestRuntimeBounds_Defaults(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.MaxActiveRunsPerRoot != 100 {
		t.Errorf("MaxActiveRunsPerRoot = %d, want 100", cfg.MaxActiveRunsPerRoot)
	}
	if cfg.MaxDefsPerRoot != 500 {
		t.Errorf("MaxDefsPerRoot = %d, want 500", cfg.MaxDefsPerRoot)
	}
	if cfg.MaxRegistersPerMinutePerRoot != 60 {
		t.Errorf("MaxRegistersPerMinutePerRoot = %d, want 60",
			cfg.MaxRegistersPerMinutePerRoot)
	}
	// The depth default must equal the existing engine cap — not a forked
	// literal. A literal 3 here would silently drift if the engine const
	// changes.
	if cfg.MaxGenerationDepth != engine.MaxNestingDepth {
		t.Errorf("MaxGenerationDepth = %d, want engine.MaxNestingDepth (%d)",
			cfg.MaxGenerationDepth, engine.MaxNestingDepth)
	}
}

// TestRuntimeBounds_EnvOverride proves each of the four numeric bounds is
// overridable via its DAGNATS_* env var.
func TestRuntimeBounds_EnvOverride(t *testing.T) {
	t.Setenv("DAGNATS_MAX_ACTIVE_RUNS_PER_ROOT", "7")
	t.Setenv("DAGNATS_MAX_DEFS_PER_ROOT", "9")
	// Depth can only TIGHTEN (<= engine ceiling); 2 is a valid override.
	t.Setenv("DAGNATS_MAX_GENERATION_DEPTH", "2")
	t.Setenv("DAGNATS_MAX_REGISTERS_PER_MINUTE_PER_ROOT", "13")

	cfg := ConfigFromEnv()

	if cfg.MaxActiveRunsPerRoot != 7 {
		t.Errorf("MaxActiveRunsPerRoot = %d, want 7", cfg.MaxActiveRunsPerRoot)
	}
	if cfg.MaxDefsPerRoot != 9 {
		t.Errorf("MaxDefsPerRoot = %d, want 9", cfg.MaxDefsPerRoot)
	}
	if cfg.MaxGenerationDepth != 2 {
		t.Errorf("MaxGenerationDepth = %d, want 2", cfg.MaxGenerationDepth)
	}
	if cfg.MaxRegistersPerMinutePerRoot != 13 {
		t.Errorf("MaxRegistersPerMinutePerRoot = %d, want 13",
			cfg.MaxRegistersPerMinutePerRoot)
	}
	// Negative: a default value would mean the env var was ignored.
	if cfg.MaxActiveRunsPerRoot == defaultMaxActiveRunsPerRoot {
		t.Error("MaxActiveRunsPerRoot still holds the default")
	}
}

// TestRuntimeBounds_FileOverrideAndNegativeRejected proves the config-file
// path parses all four keys and REJECTS a negative value with a hard error.
func TestRuntimeBounds_FileOverrideAndNegativeRejected(t *testing.T) {
	cases := []struct {
		key, val string
		check    func(t *testing.T, c *Config)
	}{
		{"max_active_runs_per_root", "12", func(t *testing.T, c *Config) {
			if c.MaxActiveRunsPerRoot != 12 {
				t.Errorf("MaxActiveRunsPerRoot = %d", c.MaxActiveRunsPerRoot)
			}
		}},
		{"max_defs_per_root", "34", func(t *testing.T, c *Config) {
			if c.MaxDefsPerRoot != 34 {
				t.Errorf("MaxDefsPerRoot = %d", c.MaxDefsPerRoot)
			}
		}},
		{"max_generation_depth", "2", func(t *testing.T, c *Config) {
			if c.MaxGenerationDepth != 2 {
				t.Errorf("MaxGenerationDepth = %d", c.MaxGenerationDepth)
			}
		}},
		{"max_registers_per_minute_per_root", "6", func(t *testing.T, c *Config) {
			if c.MaxRegistersPerMinutePerRoot != 6 {
				t.Errorf("MaxRegistersPerMinutePerRoot = %d",
					c.MaxRegistersPerMinutePerRoot)
			}
		}},
	}
	for _, tc := range cases {
		var cfg Config
		if err := applyConfigValue(tc.key, tc.val, 1, &cfg); err != nil {
			t.Fatalf("applyConfigValue(%s, %s): %v", tc.key, tc.val, err)
		}
		tc.check(t, &cfg)
	}

	// Negative space: a negative value is rejected on every key.
	for _, key := range []string{
		"max_active_runs_per_root", "max_defs_per_root",
		"max_generation_depth", "max_registers_per_minute_per_root",
	} {
		var cfg Config
		if err := applyConfigValue(key, "-1", 1, &cfg); err == nil {
			t.Fatalf("applyConfigValue(%s, -1) accepted a negative value", key)
		}
	}
}

// TestRuntimeBounds_DepthConfigLooseningRejected proves config/env can only
// TIGHTEN the generation-depth cap, never loosen it past the orchestrator's
// hard ceiling (engine.MaxNestingDepth). A loose value would make the API
// spawn-gate pass while handleWorkflowSpawn silently drops the spawn — a
// ghost run (caller gets a runID for a run that is never created). Both the
// file path and the env path must reject loosening at load.
func TestRuntimeBounds_DepthConfigLooseningRejected(t *testing.T) {
	loose := engine.MaxNestingDepth + 5

	// File path: applyConfigValue rejects a value above the ceiling.
	var cfg Config
	err := applyConfigValue(
		"max_generation_depth", strconv.Itoa(loose), 1, &cfg,
	)
	if err == nil {
		t.Fatalf("applyConfigValue accepted max_generation_depth=%d "+
			"(ceiling %d)", loose, engine.MaxNestingDepth)
	}

	// Exactly the ceiling is allowed (tighten-to-equal is fine).
	var atCeiling Config
	if err := applyConfigValue(
		"max_generation_depth", strconv.Itoa(engine.MaxNestingDepth), 1,
		&atCeiling,
	); err != nil {
		t.Fatalf("applyConfigValue rejected the ceiling value: %v", err)
	}

	// Env path: a loose env value is a hard load error (not a silent pass).
	t.Setenv("DAGNATS_MAX_GENERATION_DEPTH", strconv.Itoa(loose))
	if _, _, err := ConfigWithPath(""); err == nil {
		t.Fatalf("ConfigWithPath accepted env DAGNATS_MAX_GENERATION_DEPTH=%d",
			loose)
	}
}

// TestRuntimeBounds_EnvNegativeRejected proves a negative env value is a
// hard load error rather than silently flowing through and DISABLING the
// cap (count >= negative is always false).
func TestRuntimeBounds_EnvNegativeRejected(t *testing.T) {
	for _, env := range []string{
		"DAGNATS_MAX_ACTIVE_RUNS_PER_ROOT",
		"DAGNATS_MAX_DEFS_PER_ROOT",
		"DAGNATS_MAX_GENERATION_DEPTH",
		"DAGNATS_MAX_REGISTERS_PER_MINUTE_PER_ROOT",
	} {
		t.Run(env, func(t *testing.T) {
			t.Setenv(env, "-1")
			if _, _, err := ConfigWithPath(""); err == nil {
				t.Fatalf("ConfigWithPath accepted %s=-1 (disables the cap)", env)
			}
		})
	}
}

// TestRuntimeBounds_DefaultConstsMatchAPILayer guards against drift between
// the server-side default consts and the internal/api defaults that
// NewServiceWithLimits falls back to. The two must stay in lock-step or a
// config-default and a service-default would silently disagree.
func TestRuntimeBounds_DefaultConstsMatchAPILayer(t *testing.T) {
	if defaultMaxActiveRunsPerRoot != api.DefaultMaxActiveRunsPerRoot {
		t.Errorf("server defaultMaxActiveRunsPerRoot=%d != api=%d",
			defaultMaxActiveRunsPerRoot, api.DefaultMaxActiveRunsPerRoot)
	}
	if defaultMaxDefsPerRoot != api.DefaultMaxDefsPerRoot {
		t.Errorf("server defaultMaxDefsPerRoot=%d != api=%d",
			defaultMaxDefsPerRoot, api.DefaultMaxDefsPerRoot)
	}
	if defaultMaxRegistersPerMinutePerRoot !=
		api.DefaultMaxRegistersPerMinutePerRoot {
		t.Errorf("server defaultMaxRegistersPerMinutePerRoot=%d != api=%d",
			defaultMaxRegistersPerMinutePerRoot,
			api.DefaultMaxRegistersPerMinutePerRoot)
	}
}
