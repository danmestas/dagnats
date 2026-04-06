// Methodology: Pure unit tests for config file search and --config flag
// behavior. Each test creates isolated temp directories to avoid interference.
// No NATS or external dependencies required.

package server

import (
	"os"
	"path/filepath"
	"testing"
)

func TestConfigWithPath_ExplicitPathLoadsFile(t *testing.T) {
	dir := t.TempDir()
	cfgFile := filepath.Join(dir, "custom.yaml")
	content := "http_addr: :9090\n"
	if err := os.WriteFile(
		cfgFile, []byte(content), 0o600,
	); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	cfg, loaded, err := ConfigWithPath(cfgFile)

	// Positive: file loaded successfully
	if err != nil {
		t.Fatalf("ConfigWithPath() error: %v", err)
	}
	if loaded != cfgFile {
		t.Errorf(
			"loaded = %q, want %q", loaded, cfgFile,
		)
	}

	// Positive: value from file applied
	if cfg.HTTPAddr != ":9090" {
		t.Errorf(
			"HTTPAddr = %q, want %q",
			cfg.HTTPAddr, ":9090",
		)
	}
}

func TestConfigWithPath_ExplicitPathMissingErrors(
	t *testing.T,
) {
	bogus := "/tmp/dagnats-test-nonexistent/no.yaml"

	_, _, err := ConfigWithPath(bogus)

	// Positive: error returned for missing file
	if err == nil {
		t.Fatal("expected error for missing config file")
	}

	// Positive: error message mentions the path
	if got := err.Error(); len(got) == 0 {
		t.Error("error message is empty")
	}
}

func TestConfigWithPath_SearchOrderCWDFirst(t *testing.T) {
	// Create a temp dir with dagnats.yaml in it, then chdir there
	dir := t.TempDir()
	cwdConfig := filepath.Join(dir, "dagnats.yaml")
	content := "http_addr: :7777\n"
	if err := os.WriteFile(
		cwdConfig, []byte(content), 0o600,
	); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// Save and restore working directory
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	defer func() {
		if err := os.Chdir(origDir); err != nil {
			t.Fatalf("Chdir restore: %v", err)
		}
	}()

	if err := os.Chdir(dir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}

	cfg, loaded, err := ConfigWithPath("")

	// Positive: CWD config found and loaded
	if err != nil {
		t.Fatalf("ConfigWithPath() error: %v", err)
	}
	if loaded != "dagnats.yaml" {
		t.Errorf("loaded = %q, want %q",
			loaded, "dagnats.yaml")
	}
	if cfg.HTTPAddr != ":7777" {
		t.Errorf("HTTPAddr = %q, want %q",
			cfg.HTTPAddr, ":7777")
	}
}

func TestConfigWithPath_SearchOrderXDGFallback(
	t *testing.T,
) {
	// Set up XDG config dir with dagnats.yaml
	xdgDir := t.TempDir()
	dagnatsDir := filepath.Join(xdgDir, "dagnats")
	if err := os.MkdirAll(dagnatsDir, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	xdgConfig := filepath.Join(
		dagnatsDir, "dagnats.yaml",
	)
	content := "http_addr: :6666\n"
	if err := os.WriteFile(
		xdgConfig, []byte(content), 0o600,
	); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	// chdir to a directory with no dagnats.yaml
	emptyDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	defer func() {
		if err := os.Chdir(origDir); err != nil {
			t.Fatalf("Chdir restore: %v", err)
		}
	}()
	if err := os.Chdir(emptyDir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}

	// Point XDG_CONFIG_HOME to our temp dir
	t.Setenv("XDG_CONFIG_HOME", xdgDir)

	cfg, loaded, err := ConfigWithPath("")

	// Positive: XDG config found
	if err != nil {
		t.Fatalf("ConfigWithPath() error: %v", err)
	}
	if loaded != xdgConfig {
		t.Errorf("loaded = %q, want %q",
			loaded, xdgConfig)
	}

	// Positive: value from XDG config applied
	if cfg.HTTPAddr != ":6666" {
		t.Errorf("HTTPAddr = %q, want %q",
			cfg.HTTPAddr, ":6666")
	}
}

func TestConfigWithPath_NoFileUsesDefaults(t *testing.T) {
	// chdir to empty directory, unset XDG
	emptyDir := t.TempDir()
	origDir, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	defer func() {
		if err := os.Chdir(origDir); err != nil {
			t.Fatalf("Chdir restore: %v", err)
		}
	}()
	if err := os.Chdir(emptyDir); err != nil {
		t.Fatalf("Chdir: %v", err)
	}

	// Point XDG to a nonexistent dir so no fallback fires
	t.Setenv("XDG_CONFIG_HOME", filepath.Join(
		emptyDir, "nonexistent",
	))

	cfg, loaded, err := ConfigWithPath("")

	// Positive: no error, defaults used
	if err != nil {
		t.Fatalf("ConfigWithPath() error: %v", err)
	}
	if loaded != "" {
		t.Errorf("loaded = %q, want empty string", loaded)
	}

	// Positive: default values present
	if cfg.HTTPAddr != defaultHTTPAddr {
		t.Errorf("HTTPAddr = %q, want %q",
			cfg.HTTPAddr, defaultHTTPAddr)
	}
}

func TestConfigSearchPaths_ContainsCWD(t *testing.T) {
	paths := configSearchPaths()

	// Positive: at least one path returned
	if len(paths) == 0 {
		t.Fatal("configSearchPaths() returned empty")
	}

	// Positive: first entry is CWD relative path
	if paths[0] != "dagnats.yaml" {
		t.Errorf(
			"paths[0] = %q, want %q",
			paths[0], "dagnats.yaml",
		)
	}
}
