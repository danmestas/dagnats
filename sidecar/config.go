package sidecar

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"gopkg.in/yaml.v3"
)

const (
	defaultListen    = "0.0.0.0:4318"
	defaultStorType  = "local"
	defaultLocalPath = "./telemetry-data"

	// Bound config file size to prevent unbounded reads.
	maxConfigFileBytes = 10 * 1024
)

// SidecarConfig holds the full sidecar configuration.
type SidecarConfig struct {
	Listen     string           `yaml:"listen"`
	Storage    StorageConfig    `yaml:"storage"`
	Backend    *BackendConfig   `yaml:"backend,omitempty"`
	MCP        MCPConfig        `yaml:"mcp"`
	Supervisor SupervisorConfig `yaml:"supervisor"`
}

// StorageConfig controls where telemetry data is persisted.
type StorageConfig struct {
	Type      string    `yaml:"type"`
	LocalPath string    `yaml:"local_path"`
	S3        *S3Config `yaml:"s3,omitempty"`
}

// S3Config holds S3-compatible storage settings.
type S3Config struct {
	Endpoint string `yaml:"endpoint"`
	Bucket   string `yaml:"bucket"`
	Region   string `yaml:"region"`
}

// BackendConfig points to an upstream OTel backend for forwarding.
type BackendConfig struct {
	Endpoint string            `yaml:"endpoint"`
	Headers  map[string]string `yaml:"headers,omitempty"`
}

// MCPConfig controls the DuckDB MCP server transport.
type MCPConfig struct {
	Listen string `yaml:"listen"`
}

// SupervisorConfig controls the supervisor health endpoint.
type SupervisorConfig struct {
	Listen string `yaml:"listen"`
}

// DefaultConfig returns config with sensible zero-config defaults.
// Every field has a safe value so the sidecar works out of the box.
func DefaultConfig() *SidecarConfig {
	return &SidecarConfig{
		Listen: defaultListen,
		Storage: StorageConfig{
			Type:      defaultStorType,
			LocalPath: defaultLocalPath,
		},
		MCP: MCPConfig{
			Listen: "", // empty = stdio transport
		},
		Supervisor: SupervisorConfig{
			Listen: "localhost:4320",
		},
	}
}

// LoadConfig reads a YAML file and merges it over defaults.
// Returns defaults when the file does not exist.
// File size is bounded at 10KB to prevent abuse.
func LoadConfig(path string) (*SidecarConfig, error) {
	if path == "" {
		panic("LoadConfig: path is empty")
	}

	cfg := DefaultConfig()

	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return cfg, nil
		}
		return nil, fmt.Errorf("open config: %w", err)
	}
	defer f.Close()

	data, err := readBounded(f, maxConfigFileBytes)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return cfg, nil
}

// readBounded reads up to maxBytes from r.
// Returns an error if the content exceeds the limit.
func readBounded(r io.Reader, maxBytes int64) ([]byte, error) {
	if r == nil {
		panic("readBounded: reader is nil")
	}
	if maxBytes <= 0 {
		panic(fmt.Sprintf(
			"readBounded: invalid maxBytes %d", maxBytes,
		))
	}

	limited := io.LimitReader(r, maxBytes+1)
	data, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}

	if int64(len(data)) > maxBytes {
		return nil, fmt.Errorf(
			"config file exceeds %d bytes", maxBytes,
		)
	}

	return data, nil
}

// Validate checks that the config is internally consistent.
// Returns nil when all invariants hold.
func (c *SidecarConfig) Validate() error {
	if c == nil {
		panic("Validate: config is nil")
	}

	if c.Listen == "" {
		return fmt.Errorf("listen address must not be empty")
	}

	if c.Supervisor.Listen == "" {
		return fmt.Errorf("supervisor listen address must not be empty")
	}

	validTypes := map[string]bool{
		"local": true,
		"s3":    true,
	}
	if !validTypes[c.Storage.Type] {
		return fmt.Errorf(
			"invalid storage type %q: must be \"local\" or \"s3\"",
			c.Storage.Type,
		)
	}

	if c.Storage.Type == "s3" && c.Storage.S3 == nil {
		return fmt.Errorf(
			"storage type \"s3\" requires s3 config block",
		)
	}

	return nil
}
