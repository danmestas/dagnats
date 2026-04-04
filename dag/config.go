package dag

import (
	"encoding/json"
	"fmt"
	"time"
)

// SleepConfig holds configuration for sleep steps.
// Duration is the durable delay the engine waits before completing.
type SleepConfig struct {
	Duration time.Duration `json:"duration"`
}

// MarshalConfig serializes a config struct into raw JSON for
// StepDef.Config. Panics on nil or marshal failure — both are
// programmer errors that should be caught at build time.
func MarshalConfig(cfg interface{}) json.RawMessage {
	if cfg == nil {
		panic("MarshalConfig: cfg must not be nil")
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		panic("MarshalConfig: " + err.Error())
	}
	return data
}

// ParseAgentLoopConfig extracts AgentLoopConfig from a StepDef's
// Config field. Returns an error if the step type is wrong, Config
// is nil, or the JSON is malformed.
func ParseAgentLoopConfig(
	step StepDef,
) (AgentLoopConfig, error) {
	if step.Type != StepTypeAgentLoop {
		return AgentLoopConfig{}, fmt.Errorf(
			"step %q: expected AgentLoop, got %s",
			step.ID, step.Type,
		)
	}
	if step.Config == nil {
		return AgentLoopConfig{}, fmt.Errorf(
			"step %q: Config is nil for AgentLoop", step.ID,
		)
	}
	var cfg AgentLoopConfig
	if err := json.Unmarshal(step.Config, &cfg); err != nil {
		return AgentLoopConfig{}, fmt.Errorf(
			"step %q: unmarshal AgentLoopConfig: %w",
			step.ID, err,
		)
	}
	return cfg, nil
}

// ParseMapConfig extracts MapConfig from a StepDef's Config field.
func ParseMapConfig(step StepDef) (MapConfig, error) {
	if step.Type != StepTypeMap {
		return MapConfig{}, fmt.Errorf(
			"step %q: expected Map, got %s",
			step.ID, step.Type,
		)
	}
	if step.Config == nil {
		return MapConfig{}, fmt.Errorf(
			"step %q: Config is nil for Map", step.ID,
		)
	}
	var cfg MapConfig
	if err := json.Unmarshal(step.Config, &cfg); err != nil {
		return MapConfig{}, fmt.Errorf(
			"step %q: unmarshal MapConfig: %w", step.ID, err,
		)
	}
	return cfg, nil
}

// ParseSleepConfig extracts SleepConfig from a StepDef's Config
// field.
func ParseSleepConfig(step StepDef) (SleepConfig, error) {
	if step.Type != StepTypeSleep {
		return SleepConfig{}, fmt.Errorf(
			"step %q: expected Sleep, got %s",
			step.ID, step.Type,
		)
	}
	if step.Config == nil {
		return SleepConfig{}, fmt.Errorf(
			"step %q: Config is nil for Sleep", step.ID,
		)
	}
	var cfg SleepConfig
	if err := json.Unmarshal(step.Config, &cfg); err != nil {
		return SleepConfig{}, fmt.Errorf(
			"step %q: unmarshal SleepConfig: %w", step.ID, err,
		)
	}
	return cfg, nil
}

// ParseWaitForEventConfig extracts WaitForEventOpts from a
// StepDef's Config field.
func ParseWaitForEventConfig(
	step StepDef,
) (WaitForEventOpts, error) {
	if step.Type != StepTypeWaitForEvent {
		return WaitForEventOpts{}, fmt.Errorf(
			"step %q: expected WaitForEvent, got %s",
			step.ID, step.Type,
		)
	}
	if step.Config == nil {
		return WaitForEventOpts{}, fmt.Errorf(
			"step %q: Config is nil for WaitForEvent", step.ID,
		)
	}
	var cfg WaitForEventOpts
	if err := json.Unmarshal(step.Config, &cfg); err != nil {
		return WaitForEventOpts{}, fmt.Errorf(
			"step %q: unmarshal WaitForEventOpts: %w",
			step.ID, err,
		)
	}
	return cfg, nil
}
