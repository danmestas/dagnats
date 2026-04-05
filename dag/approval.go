package dag

import (
	"encoding/json"
	"fmt"
	"time"
)

// maxApprovalTimeout bounds how long an approval gate can wait.
// Seven days is long enough for any human review cycle while
// preventing forgotten gates from blocking resources indefinitely.
const maxApprovalTimeout = 168 * time.Hour

// ApprovalConfig holds configuration for approval gate steps.
// Subject is the NATS subject where a notification is published
// when the approval is requested. External systems subscribe to
// this subject and present approve/reject actions to humans.
type ApprovalConfig struct {
	Timeout     time.Duration     `json:"timeout"`
	Subject     string            `json:"subject"`
	Description string            `json:"description,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// ParseApprovalConfig extracts ApprovalConfig from a StepDef's
// Config field. Returns an error if the step type is wrong,
// Config is nil, or the JSON is malformed.
func ParseApprovalConfig(
	step StepDef,
) (ApprovalConfig, error) {
	if step.Type != StepTypeApproval {
		return ApprovalConfig{}, fmt.Errorf(
			"step %q: expected Approval, got %s",
			step.ID, step.Type,
		)
	}
	if step.Config == nil {
		return ApprovalConfig{}, fmt.Errorf(
			"step %q: Config is nil for Approval", step.ID,
		)
	}
	var cfg ApprovalConfig
	if err := json.Unmarshal(step.Config, &cfg); err != nil {
		return ApprovalConfig{}, fmt.Errorf(
			"step %q: unmarshal ApprovalConfig: %w",
			step.ID, err,
		)
	}
	return cfg, nil
}

// validateApprovalStep checks Approval step configuration.
// Subject must be non-empty, timeout must be positive and
// bounded by maxApprovalTimeout to prevent resource leaks.
func validateApprovalStep(step StepDef) error {
	if step.ID == "" {
		panic("validateApprovalStep: step.ID must not be empty")
	}
	if step.Type != StepTypeApproval {
		return nil
	}
	cfg, err := ParseApprovalConfig(step)
	if err != nil {
		return fmt.Errorf(
			"step %q: invalid approval config: %w",
			step.ID, err,
		)
	}
	if cfg.Subject == "" {
		return fmt.Errorf(
			"step %q: Approval.Subject must not be empty",
			step.ID,
		)
	}
	if cfg.Timeout <= 0 {
		return fmt.Errorf(
			"step %q: Approval.Timeout must be positive",
			step.ID,
		)
	}
	if cfg.Timeout > maxApprovalTimeout {
		return fmt.Errorf(
			"step %q: Approval.Timeout %v exceeds max %v",
			step.ID, cfg.Timeout, maxApprovalTimeout,
		)
	}
	return nil
}
