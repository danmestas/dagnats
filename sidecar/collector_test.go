// Methodology: Each test generates Collector YAML from a SidecarConfig,
// then parses it back with yaml.v3 to verify structural correctness.
// We test both the presence and absence of optional sections.

package sidecar

import (
	"os"
	"path/filepath"
	"testing"

	"gopkg.in/yaml.v3"
)

// collectorConfig mirrors the OTel Collector YAML structure
// just enough to verify generated output.
type collectorConfig struct {
	Receivers  map[string]any `yaml:"receivers"`
	Processors map[string]any `yaml:"processors"`
	Exporters  map[string]any `yaml:"exporters"`
	Service    serviceConfig  `yaml:"service"`
}

type serviceConfig struct {
	Pipelines map[string]pipelineConfig `yaml:"pipelines"`
}

type pipelineConfig struct {
	Receivers  []string `yaml:"receivers"`
	Processors []string `yaml:"processors"`
	Exporters  []string `yaml:"exporters"`
}

func parseCollectorYAML(
	t *testing.T, data []byte,
) collectorConfig {
	t.Helper()
	var cc collectorConfig
	if err := yaml.Unmarshal(data, &cc); err != nil {
		t.Fatalf("parse generated YAML: %v", err)
	}
	return cc
}

func TestGenerateCollectorConfig_LocalOnly(t *testing.T) {
	cfg := DefaultConfig()

	data, err := GenerateCollectorConfig(cfg)
	if err != nil {
		t.Fatalf("GenerateCollectorConfig: %v", err)
	}

	cc := parseCollectorYAML(t, data)

	// Verify otlp receiver exists.
	if _, ok := cc.Receivers["otlp"]; !ok {
		t.Error("expected otlp receiver")
	}

	// Verify batch processor exists.
	if _, ok := cc.Processors["batch"]; !ok {
		t.Error("expected batch processor")
	}

	// Verify parquet exporter present, backend absent.
	if _, ok := cc.Exporters["otlphttp/parquet"]; !ok {
		t.Error("expected otlphttp/parquet exporter")
	}
	if _, ok := cc.Exporters["otlphttp/backend"]; ok {
		t.Error("unexpected otlphttp/backend exporter")
	}

	// Verify all 3 pipelines exist with correct exporters.
	for _, name := range []string{"traces", "metrics", "logs"} {
		p, ok := cc.Service.Pipelines[name]
		if !ok {
			t.Errorf("expected %s pipeline", name)
			continue
		}
		if len(p.Exporters) != 1 {
			t.Errorf(
				"%s: expected 1 exporter, got %d",
				name, len(p.Exporters),
			)
		}
	}
}

func TestGenerateCollectorConfig_WithBackend(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Backend = &BackendConfig{
		Endpoint: "https://otel.example.com:4318",
		Headers: map[string]string{
			"Authorization": "Bearer secret",
		},
	}

	data, err := GenerateCollectorConfig(cfg)
	if err != nil {
		t.Fatalf("GenerateCollectorConfig: %v", err)
	}

	cc := parseCollectorYAML(t, data)

	// Verify backend exporter present.
	if _, ok := cc.Exporters["otlphttp/backend"]; !ok {
		t.Error("expected otlphttp/backend exporter")
	}

	// Verify all pipelines include both exporters.
	for _, name := range []string{"traces", "metrics", "logs"} {
		p, ok := cc.Service.Pipelines[name]
		if !ok {
			t.Errorf("expected %s pipeline", name)
			continue
		}
		if len(p.Exporters) != 2 {
			t.Errorf(
				"%s: expected 2 exporters, got %d",
				name, len(p.Exporters),
			)
		}
	}
}

func TestGenerateCollectorConfig_ListenAddress(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Listen = "127.0.0.1:9999"

	data, err := GenerateCollectorConfig(cfg)
	if err != nil {
		t.Fatalf("GenerateCollectorConfig: %v", err)
	}

	cc := parseCollectorYAML(t, data)

	// Dig into the otlp receiver to find the endpoint.
	otlp, ok := cc.Receivers["otlp"]
	if !ok {
		t.Fatal("expected otlp receiver")
	}

	// Navigate: otlp -> protocols -> http -> endpoint.
	protocols := otlp.(map[string]any)["protocols"]
	httpCfg := protocols.(map[string]any)["http"]
	endpoint := httpCfg.(map[string]any)["endpoint"]

	if endpoint != "127.0.0.1:9999" {
		t.Errorf(
			"expected endpoint 127.0.0.1:9999, got %v",
			endpoint,
		)
	}

	// Verify YAML is parseable (already done above) and
	// has the correct listen address.
	if cc.Receivers == nil {
		t.Error("receivers must not be nil")
	}
}

func TestWriteCollectorConfig(t *testing.T) {
	cfg := DefaultConfig()
	dir := t.TempDir()
	path := filepath.Join(dir, "collector.yaml")

	err := WriteCollectorConfig(cfg, path)
	if err != nil {
		t.Fatalf("WriteCollectorConfig: %v", err)
	}

	// Verify file exists and is non-empty.
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() == 0 {
		t.Error("written file is empty")
	}

	// Parse file contents to verify valid YAML.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}

	cc := parseCollectorYAML(t, data)
	if _, ok := cc.Receivers["otlp"]; !ok {
		t.Error("expected otlp receiver in written file")
	}
}
