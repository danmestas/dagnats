// agent/config_test.go
// Tests for AgentConfig validation and serialization.
// Methodology: Each test verifies positive behavior and negative space.

package agent

import (
	"testing"
	"time"
)

func TestValidateConfig_ValidMinimal(t *testing.T) {
	cfg := AgentConfig{
		Name:         "test-agent",
		SystemPrompt: "You are a helpful assistant.",
		Model:        "claude-sonnet-4-6",
		Provider:     "anthropic",
	}
	err := ValidateConfig(cfg)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	// Negative: empty name should fail.
	cfg.Name = ""
	err = ValidateConfig(cfg)
	if err == nil {
		t.Fatal("expected error for empty name")
	}
}

func TestValidateConfig_EmptyFields(t *testing.T) {
	cases := []struct {
		name string
		cfg  AgentConfig
	}{
		{"empty system prompt", AgentConfig{
			Name: "a", Model: "m", Provider: "p",
		}},
		{"empty model", AgentConfig{
			Name: "a", SystemPrompt: "s", Provider: "p",
		}},
		{"empty provider", AgentConfig{
			Name: "a", SystemPrompt: "s", Model: "m",
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateConfig(tc.cfg)
			if err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestValidateConfig_Temperature(t *testing.T) {
	base := AgentConfig{
		Name: "a", SystemPrompt: "s", Model: "m", Provider: "p",
	}
	// Valid temperatures
	for _, temp := range []float64{0, 0.5, 1.0, 2.0} {
		base.Temperature = temp
		if err := ValidateConfig(base); err != nil {
			t.Fatalf("temp %.1f should be valid: %v", temp, err)
		}
	}
	// Invalid temperatures
	base.Temperature = -0.1
	if err := ValidateConfig(base); err == nil {
		t.Fatal("negative temperature should fail")
	}
	base.Temperature = 2.1
	if err := ValidateConfig(base); err == nil {
		t.Fatal("temperature > 2 should fail")
	}
}

func TestValidateConfig_SandboxRequiresWorkspaceDir(t *testing.T) {
	cfg := AgentConfig{
		Name: "a", SystemPrompt: "s", Model: "m", Provider: "p",
		Sandbox: &SandboxConfig{},
	}
	err := ValidateConfig(cfg)
	if err == nil {
		t.Fatal("expected error for empty workspace_dir")
	}
	cfg.Sandbox.WorkspaceDir = "/workspace"
	err = ValidateConfig(cfg)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
}

func TestEffectiveDefaults(t *testing.T) {
	cfg := AgentConfig{}
	if cfg.EffectiveMaxTokens() != configMaxTokensDefault {
		t.Fatalf(
			"expected %d, got %d",
			configMaxTokensDefault, cfg.EffectiveMaxTokens(),
		)
	}
	if cfg.EffectiveMaxTurns() != configMaxTurnsDefault {
		t.Fatalf(
			"expected %d, got %d",
			configMaxTurnsDefault, cfg.EffectiveMaxTurns(),
		)
	}
	// Explicit values override defaults.
	cfg.MaxTokens = 8192
	cfg.MaxTurns = 10
	if cfg.EffectiveMaxTokens() != 8192 {
		t.Fatal("expected explicit MaxTokens")
	}
	if cfg.EffectiveMaxTurns() != 10 {
		t.Fatal("expected explicit MaxTurns")
	}
}

func TestSandboxEffectiveDefaults(t *testing.T) {
	s := SandboxConfig{}
	if s.EffectiveBashTimeout() != configBashTimeoutDefault {
		t.Fatalf(
			"expected %v, got %v",
			configBashTimeoutDefault, s.EffectiveBashTimeout(),
		)
	}
	if s.EffectiveBashMaxOutput() != configBashMaxOutputDefault {
		t.Fatalf(
			"expected %d, got %d",
			configBashMaxOutputDefault, s.EffectiveBashMaxOutput(),
		)
	}
	// Explicit values override.
	s.BashTimeout = 60 * time.Second
	s.BashMaxOutput = 2048
	if s.EffectiveBashTimeout() != 60*time.Second {
		t.Fatal("expected explicit BashTimeout")
	}
	if s.EffectiveBashMaxOutput() != 2048 {
		t.Fatal("expected explicit BashMaxOutput")
	}
}

func TestMarshalUnmarshalConfig(t *testing.T) {
	cfg := AgentConfig{
		Name:         "round-trip",
		SystemPrompt: "test prompt",
		Model:        "claude-sonnet-4-6",
		Provider:     "anthropic",
		MaxTokens:    2048,
		Temperature:  0.7,
		Tools:        []string{"read_file", "bash"},
		MaxTurns:     20,
		Sandbox: &SandboxConfig{
			WorkspaceDir:  "/workspace",
			AllowedPaths:  []string{"/etc/hosts"},
			BashTimeout:   10 * time.Second,
			BashMaxOutput: 4096,
			NetworkAccess: true,
		},
	}
	data, err := MarshalConfig(cfg)
	if err != nil {
		t.Fatalf("marshal failed: %v", err)
	}
	got, err := UnmarshalConfig(data)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if got.Name != cfg.Name {
		t.Fatalf("name mismatch: %q vs %q", got.Name, cfg.Name)
	}
	if got.Sandbox == nil || got.Sandbox.WorkspaceDir != "/workspace" {
		t.Fatal("sandbox not preserved through round-trip")
	}
}
