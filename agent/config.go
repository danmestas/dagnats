// agent/config.go

// AgentConfig defines the identity and capabilities of an agent.
// Stored in NATS KV bucket "agent_configs", referenced by name from
// StepDef.AgentConfigRef. The config determines which LLM provider to
// call, what tools are available, and the security boundary for execution.
package agent

import (
	"encoding/json"
	"fmt"
	"time"
)

// AgentConfig is the immutable declaration of an agent's capabilities.
// Name must be unique within the agent_configs KV bucket.
type AgentConfig struct {
	Name         string         `json:"name"`
	SystemPrompt string         `json:"system_prompt"`
	Model        string         `json:"model"`
	Provider     string         `json:"provider"`
	MaxTokens    int            `json:"max_tokens"`
	Temperature  float64        `json:"temperature"`
	Tools        []string       `json:"tools"`
	MaxTurns     int            `json:"max_turns"`
	Sandbox      *SandboxConfig `json:"sandbox,omitempty"`
}

// SandboxConfig defines the security boundary for tool execution.
// All tools run within these constraints. WorkspaceDir is the root
// for all file operations — no reads or writes escape it.
type SandboxConfig struct {
	WorkspaceDir  string        `json:"workspace_dir"`
	AllowedPaths  []string      `json:"allowed_paths,omitempty"`
	BashTimeout   time.Duration `json:"bash_timeout"`
	BashMaxOutput int           `json:"bash_max_output"`
	NetworkAccess bool          `json:"network_access"`
}

// configMaxTokensDefault is used when AgentConfig.MaxTokens is zero.
const configMaxTokensDefault = 4096

// configMaxTurnsDefault is used when AgentConfig.MaxTurns is zero.
const configMaxTurnsDefault = 50

// configBashTimeoutDefault is used when SandboxConfig.BashTimeout is zero.
const configBashTimeoutDefault = 30 * time.Second

// configBashMaxOutputDefault is used when SandboxConfig.BashMaxOutput is zero.
const configBashMaxOutputDefault = 1024 * 1024 // 1MB

// ValidateConfig checks an AgentConfig for structural errors. Returns
// nil if the config is valid. Does not access external systems.
func ValidateConfig(cfg AgentConfig) error {
	if cfg.Name == "" {
		return fmt.Errorf("agent config: name must not be empty")
	}
	if cfg.SystemPrompt == "" {
		return fmt.Errorf(
			"agent config %q: system_prompt must not be empty",
			cfg.Name,
		)
	}
	if cfg.Model == "" {
		return fmt.Errorf(
			"agent config %q: model must not be empty",
			cfg.Name,
		)
	}
	if cfg.Provider == "" {
		return fmt.Errorf(
			"agent config %q: provider must not be empty",
			cfg.Name,
		)
	}
	if cfg.Temperature < 0 || cfg.Temperature > 2 {
		return fmt.Errorf(
			"agent config %q: temperature must be in [0, 2]",
			cfg.Name,
		)
	}
	if cfg.Sandbox != nil {
		if cfg.Sandbox.WorkspaceDir == "" {
			return fmt.Errorf(
				"agent config %q: sandbox workspace_dir must not be empty",
				cfg.Name,
			)
		}
	}
	return nil
}

// EffectiveMaxTokens returns MaxTokens or the default if unset.
func (c AgentConfig) EffectiveMaxTokens() int {
	if c.MaxTokens > 0 {
		return c.MaxTokens
	}
	return configMaxTokensDefault
}

// EffectiveMaxTurns returns MaxTurns or the default if unset.
func (c AgentConfig) EffectiveMaxTurns() int {
	if c.MaxTurns > 0 {
		return c.MaxTurns
	}
	return configMaxTurnsDefault
}

// EffectiveBashTimeout returns BashTimeout or the default if unset.
func (s SandboxConfig) EffectiveBashTimeout() time.Duration {
	if s.BashTimeout > 0 {
		return s.BashTimeout
	}
	return configBashTimeoutDefault
}

// EffectiveBashMaxOutput returns BashMaxOutput or the default if unset.
func (s SandboxConfig) EffectiveBashMaxOutput() int {
	if s.BashMaxOutput > 0 {
		return s.BashMaxOutput
	}
	return configBashMaxOutputDefault
}

// MarshalConfig serializes an AgentConfig to JSON.
func MarshalConfig(cfg AgentConfig) ([]byte, error) {
	return json.Marshal(cfg)
}

// UnmarshalConfig deserializes an AgentConfig from JSON.
func UnmarshalConfig(data []byte) (AgentConfig, error) {
	var cfg AgentConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return AgentConfig{}, fmt.Errorf("unmarshal agent config: %w", err)
	}
	return cfg, nil
}
