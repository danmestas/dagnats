// Methodology: Pure unit tests using TDD. Each test verifies config parsing,
// defaults, and env var overrides with explicit positive and negative
// assertions. No NATS or external dependencies. Bounded timeouts not needed.

package server

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
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
	os.Setenv("DAGNATS_DATA_DIR", "/tmp/dagnats-test")
	os.Setenv("DAGNATS_HTTP_ADDR", ":9999")
	os.Setenv("DAGNATS_NATS_PORT", "5555")
	os.Setenv("DAGNATS_LEAF_REMOTES", "nats://leaf1:7422,nats://leaf2:7422")
	os.Setenv("DAGNATS_MAX_STORE_BYTES", "1073741824")
	defer func() {
		os.Unsetenv("DAGNATS_DATA_DIR")
		os.Unsetenv("DAGNATS_HTTP_ADDR")
		os.Unsetenv("DAGNATS_NATS_PORT")
		os.Unsetenv("DAGNATS_LEAF_REMOTES")
		os.Unsetenv("DAGNATS_MAX_STORE_BYTES")
	}()

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
	os.Setenv("DAGNATS_LEAF_REMOTES", strings.Join(remotes, ","))
	defer os.Unsetenv("DAGNATS_LEAF_REMOTES")

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
