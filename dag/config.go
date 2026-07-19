package dag

import (
	"encoding/json"
	"fmt"
	"time"
)

// SleepConfig holds configuration for sleep steps. Exactly one of the
// three forms must be set; ParseSleepConfig enforces that so no
// downstream caller can observe an ambiguous config.
//
// Duration fixes the delay at def-registration time. Cron and
// UntilInputPath defer it to dispatch: Cron sleeps until the next
// occurrence strictly after dispatch, UntilInputPath reads a deadline
// from the run input (an RFC3339 instant or integer milliseconds).
type SleepConfig struct {
	Duration       time.Duration `json:"duration,omitempty"`
	Cron           string        `json:"cron,omitempty"`
	UntilInputPath string        `json:"until_input_path,omitempty"`
}

// formCount reports how many of the three mutually-exclusive forms are
// set. Used to enforce the exactly-one invariant at parse time.
func (c SleepConfig) formCount() int {
	const formCountMax = 3

	count := 0
	if c.Duration != 0 {
		count++
	}
	if c.Cron != "" {
		count++
	}
	if c.UntilInputPath != "" {
		count++
	}

	if count < 0 {
		panic("formCount: count must not be negative")
	}
	// Catches a fourth form being added to the struct without a
	// matching branch here — callers rely on this counting every form.
	if count > formCountMax {
		panic("formCount: count exceeds the number of known forms")
	}
	return count
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
	// Enforced here rather than only at registration because the config
	// round-trips through KV and is re-parsed at dispatch: a hand-edited
	// or older def must not slip an ambiguous form past the dispatcher.
	if n := cfg.formCount(); n != 1 {
		return SleepConfig{}, fmt.Errorf(
			"step %q: sleep config must set exactly one of "+
				"duration, cron, until_input_path; got %d",
			step.ID, n,
		)
	}
	return cfg, nil
}

// ParseSubWorkflowConfig extracts SubWorkflowConfig from a
// StepDef's Config field.
func ParseSubWorkflowConfig(
	step StepDef,
) (SubWorkflowConfig, error) {
	if step.Type != StepTypeSubWorkflow {
		return SubWorkflowConfig{}, fmt.Errorf(
			"step %q: expected SubWorkflow, got %s",
			step.ID, step.Type,
		)
	}
	if step.Config == nil {
		return SubWorkflowConfig{}, fmt.Errorf(
			"step %q: Config is nil for SubWorkflow", step.ID,
		)
	}
	var cfg SubWorkflowConfig
	if err := json.Unmarshal(step.Config, &cfg); err != nil {
		return SubWorkflowConfig{}, fmt.Errorf(
			"step %q: unmarshal SubWorkflowConfig: %w",
			step.ID, err,
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

// ParsePlannerConfig extracts PlannerConfig from a StepDef's Config
// field. Returns an error if the step type is wrong, Config is nil,
// or the JSON is malformed.
func ParsePlannerConfig(
	step StepDef,
) (PlannerConfig, error) {
	if step.Type != StepTypePlanner {
		return PlannerConfig{}, fmt.Errorf(
			"step %q: expected Planner, got %s",
			step.ID, step.Type,
		)
	}
	if step.Config == nil {
		return PlannerConfig{}, fmt.Errorf(
			"step %q: Config is nil for Planner", step.ID,
		)
	}
	var cfg PlannerConfig
	if err := json.Unmarshal(step.Config, &cfg); err != nil {
		return PlannerConfig{}, fmt.Errorf(
			"step %q: unmarshal PlannerConfig: %w",
			step.ID, err,
		)
	}
	return cfg, nil
}
