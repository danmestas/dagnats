package dag

import (
	"fmt"
	"time"
)

const maxSleepDuration = 365 * 24 * time.Hour

func validateSleepStep(step StepDef) error {
	if step.ID == "" {
		panic("validateSleepStep: step.ID must not be empty")
	}
	if step.Type != StepTypeSleep {
		return nil
	}
	if maxSleepDuration <= 0 {
		panic("validateSleepStep: maxSleepDuration must be positive")
	}
	cfg, err := ParseSleepConfig(step)
	if err != nil {
		return fmt.Errorf(
			"step %q: invalid sleep config: %w", step.ID, err)
	}
	if cfg.Duration <= 0 {
		return fmt.Errorf(
			"step %q: sleep duration must be positive, got %v",
			step.ID, cfg.Duration)
	}
	if cfg.Duration > maxSleepDuration {
		return fmt.Errorf(
			"step %q: sleep duration %v exceeds max %v",
			step.ID, cfg.Duration, maxSleepDuration)
	}
	return nil
}
