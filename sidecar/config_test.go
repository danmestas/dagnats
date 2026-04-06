// Methodology: Pure unit tests using TDD. Each test verifies config
// defaults, YAML loading, and validation with explicit positive and
// negative assertions. No external dependencies. No shared state.

package sidecar

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	// Positive: all defaults are set correctly
	if cfg.Listen != "0.0.0.0:4318" {
		t.Errorf("Listen = %q, want %q",
			cfg.Listen, "0.0.0.0:4318")
	}
	if cfg.Storage.Type != "local" {
		t.Errorf("Storage.Type = %q, want %q",
			cfg.Storage.Type, "local")
	}
	if cfg.Storage.LocalPath != "./telemetry-data" {
		t.Errorf("Storage.LocalPath = %q, want %q",
			cfg.Storage.LocalPath, "./telemetry-data")
	}
	if cfg.MCP.Listen != "" {
		t.Errorf("MCP.Listen = %q, want empty",
			cfg.MCP.Listen)
	}

	// Negative: optional sections are nil
	if cfg.Backend != nil {
		t.Errorf("Backend = %+v, want nil", cfg.Backend)
	}
	if cfg.Storage.S3 != nil {
		t.Errorf("Storage.S3 = %+v, want nil",
			cfg.Storage.S3)
	}
}

func TestLoadConfig_Defaults(t *testing.T) {
	// Non-existent file should return defaults without error.
	path := filepath.Join(t.TempDir(), "nonexistent.yaml")

	cfg, err := LoadConfig(path)

	// Positive: no error
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}

	// Positive: matches defaults
	if cfg.Listen != defaultListen {
		t.Errorf("Listen = %q, want %q",
			cfg.Listen, defaultListen)
	}
	if cfg.Storage.Type != defaultStorType {
		t.Errorf("Storage.Type = %q, want %q",
			cfg.Storage.Type, defaultStorType)
	}
}

func TestLoadConfig_YAML(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "sidecar.yaml")
	content := `listen: "0.0.0.0:4319"
storage:
  type: s3
  local_path: /data/telemetry
  s3:
    endpoint: https://s3.example.com
    bucket: my-telemetry
    region: us-west-2
backend:
  endpoint: https://otel.example.com:4317
  headers:
    Authorization: Bearer secret-token
mcp:
  listen: "localhost:9090"
`
	if err := os.WriteFile(
		cfgPath, []byte(content), 0o600,
	); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)

	// Positive: no error
	if err != nil {
		t.Fatalf("LoadConfig() error = %v, want nil", err)
	}

	// Positive: all fields parsed
	if cfg.Listen != "0.0.0.0:4319" {
		t.Errorf("Listen = %q, want %q",
			cfg.Listen, "0.0.0.0:4319")
	}
	if cfg.Storage.Type != "s3" {
		t.Errorf("Storage.Type = %q, want %q",
			cfg.Storage.Type, "s3")
	}
	if cfg.Storage.S3 == nil {
		t.Fatal("Storage.S3 is nil, want non-nil")
	}
	if cfg.Storage.S3.Bucket != "my-telemetry" {
		t.Errorf("S3.Bucket = %q, want %q",
			cfg.Storage.S3.Bucket, "my-telemetry")
	}
	if cfg.Storage.S3.Region != "us-west-2" {
		t.Errorf("S3.Region = %q, want %q",
			cfg.Storage.S3.Region, "us-west-2")
	}
	if cfg.Storage.S3.Endpoint != "https://s3.example.com" {
		t.Errorf("S3.Endpoint = %q, want %q",
			cfg.Storage.S3.Endpoint,
			"https://s3.example.com")
	}
	if cfg.Backend == nil {
		t.Fatal("Backend is nil, want non-nil")
	}
	if cfg.Backend.Endpoint != "https://otel.example.com:4317" {
		t.Errorf("Backend.Endpoint = %q, want %q",
			cfg.Backend.Endpoint,
			"https://otel.example.com:4317")
	}
	if cfg.Backend.Headers["Authorization"] != "Bearer secret-token" {
		t.Errorf("Backend.Headers[Authorization] = %q",
			cfg.Backend.Headers["Authorization"])
	}
	if cfg.MCP.Listen != "localhost:9090" {
		t.Errorf("MCP.Listen = %q, want %q",
			cfg.MCP.Listen, "localhost:9090")
	}

	// Negative: local_path overridden from default
	if cfg.Storage.LocalPath == defaultLocalPath {
		t.Errorf("Storage.LocalPath still default %q",
			defaultLocalPath)
	}
}

func TestLoadConfig_OversizedFile(t *testing.T) {
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "big.yaml")
	// Write a file just over the 10KB limit.
	data := make([]byte, maxConfigFileBytes+1)
	for i := range data {
		data[i] = '#'
	}
	if err := os.WriteFile(
		cfgPath, data, 0o600,
	); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	_, err := LoadConfig(cfgPath)

	// Positive: error returned
	if err == nil {
		t.Fatal("LoadConfig() should fail for oversized file")
	}

	// Positive: error message mentions size
	if !strings.Contains(err.Error(), "exceeds") {
		t.Errorf("error = %q, want mention of 'exceeds'",
			err.Error())
	}
}

func TestValidate_Valid(t *testing.T) {
	cfg := DefaultConfig()

	err := cfg.Validate()

	// Positive: default config is always valid
	if err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}

	// Negative: defaults should not trigger S3 validation
	if cfg.Storage.S3 != nil {
		t.Error("default config has non-nil S3")
	}
}

func TestValidate_InvalidStorageType(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storage.Type = "redis"

	err := cfg.Validate()

	// Positive: error returned
	if err == nil {
		t.Fatal("Validate() should fail for type 'redis'")
	}

	// Positive: error mentions the bad type
	if !strings.Contains(err.Error(), "redis") {
		t.Errorf("error = %q, want mention of 'redis'",
			err.Error())
	}
}

func TestValidate_S3WithoutConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storage.Type = "s3"
	cfg.Storage.S3 = nil

	err := cfg.Validate()

	// Positive: error returned
	if err == nil {
		t.Fatal("Validate() should fail when s3 config missing")
	}

	// Positive: error mentions s3
	if !strings.Contains(err.Error(), "s3") {
		t.Errorf("error = %q, want mention of 's3'",
			err.Error())
	}
}

func TestValidate_EmptyListen(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Listen = ""

	err := cfg.Validate()

	// Positive: error returned
	if err == nil {
		t.Fatal("Validate() should fail for empty listen")
	}

	// Positive: error mentions listen
	if !strings.Contains(err.Error(), "listen") {
		t.Errorf("error = %q, want mention of 'listen'",
			err.Error())
	}
}

func TestValidate_S3WithConfig(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Storage.Type = "s3"
	cfg.Storage.S3 = &S3Config{
		Endpoint: "https://s3.example.com",
		Bucket:   "telemetry",
		Region:   "us-east-1",
	}

	err := cfg.Validate()

	// Positive: valid S3 config passes
	if err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}

	// Negative: should not reject well-formed S3
	if cfg.Storage.S3.Bucket != "telemetry" {
		t.Error("S3 config was modified during validation")
	}
}
