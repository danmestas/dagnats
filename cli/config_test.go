// cli/config_test.go
// Tests for the config show command.
// Methodology: unit tests that capture stdout from runConfigShowCmd and
// verify both human-readable and JSON output contain expected fields.
// No NATS required -- config is purely local.
package cli

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestConfigShowHumanOutput(t *testing.T) {
	output := captureOutput(func() {
		runConfigShowCmd([]string{})
	})

	// Positive: output should contain key config fields
	if !strings.Contains(output, "data_dir") {
		t.Fatal("human output should contain data_dir label")
	}
	if !strings.Contains(output, "nats_port") {
		t.Fatal("human output should contain nats_port label")
	}

	// Negative: should not be valid JSON (it is human-readable)
	var discard map[string]any
	if err := json.Unmarshal([]byte(output), &discard); err == nil {
		t.Fatal("human output should not be valid JSON")
	}
}

func TestConfigShowJSONOutput(t *testing.T) {
	output := captureOutput(func() {
		runConfigShowCmd([]string{"--json"})
	})

	// Positive: should be valid JSON with expected keys
	var cfg map[string]any
	if err := json.Unmarshal([]byte(output), &cfg); err != nil {
		t.Fatalf("output should be valid JSON: %v\n%s", err, output)
	}
	if _, ok := cfg["data_dir"]; !ok {
		t.Fatal("JSON should contain data_dir key")
	}
	if _, ok := cfg["nats_port"]; !ok {
		t.Fatal("JSON should contain nats_port key")
	}

	// Negative: should not contain human-readable labels
	if strings.Contains(output, "data_dir:") {
		// JSON uses "data_dir": (with quotes), not bare "data_dir:"
		lines := strings.Split(output, "\n")
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "data_dir:") {
				t.Fatal("JSON output should not have bare key: lines")
			}
		}
	}
}
