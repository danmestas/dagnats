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

// TestConfigShowHidesSecrets pins that neither form of config show prints
// the cluster auth token or a password embedded in a leaf remote URL,
// while still saying they are set and showing the rest of the URL.
func TestConfigShowHidesSecrets(t *testing.T) {
	const token, password = "cluster-token-s3cret", "leaf-pa55word"
	t.Setenv("DAGNATS_NATS_CLUSTER_AUTH_TOKEN", token)
	t.Setenv("DAGNATS_LEAF_REMOTES", "nats-leaf://hub:"+password+"@eu.example:7422")
	for _, args := range [][]string{{}, {"--json"}} {
		output := captureOutput(func() { runConfigShowCmd(args) })
		if strings.Contains(output, token) || strings.Contains(output, password) {
			t.Fatalf("config show %v printed a secret:\n%s", args, output)
		}
		if !strings.Contains(output, "eu.example:7422") {
			t.Fatalf("config show %v dropped the leaf remote host:\n%s", args, output)
		}
	}
	var cfg map[string]any
	output := captureOutput(func() { runConfigShowCmd([]string{"--json"}) })
	if err := json.Unmarshal([]byte(output), &cfg); err != nil {
		t.Fatalf("JSON output: %v", err)
	}
	if cfg["nats_cluster_auth_token"] != redactedSecret {
		t.Fatalf("nats_cluster_auth_token = %v, want %q", cfg["nats_cluster_auth_token"], redactedSecret)
	}
}
