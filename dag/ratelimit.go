package dag

import (
	"fmt"
	"time"
)

// RateLimit configures global per-task-type rate limiting.
type RateLimit struct {
	Limit  int           `json:"limit"`
	Period time.Duration `json:"period"`
}

// KeyedRateLimit configures per-key rate limiting using a dot-path expression.
type KeyedRateLimit struct {
	Key    string        `json:"key"`
	Limit  int           `json:"limit"`
	Period time.Duration `json:"period"`
	Units  int           `json:"units"`
}

func validateRateLimit(step StepDef) error {
	if step.ID == "" {
		panic("validateRateLimit: step.ID must not be empty")
	}
	if step.RateLimit == nil && step.KeyedRateLimit == nil {
		return nil
	}
	if step.RateLimit != nil && step.KeyedRateLimit != nil {
		panic("validateRateLimit: cannot set both RateLimit and KeyedRateLimit")
	}
	if step.RateLimit != nil {
		if step.RateLimit.Limit <= 0 {
			return fmt.Errorf("step %q: rate limit must be positive", step.ID)
		}
		if step.RateLimit.Period <= 0 {
			return fmt.Errorf(
				"step %q: rate limit period must be positive", step.ID,
			)
		}
	}
	if step.KeyedRateLimit != nil {
		if step.KeyedRateLimit.Key == "" {
			return fmt.Errorf(
				"step %q: keyed rate limit key must not be empty", step.ID,
			)
		}
		if step.KeyedRateLimit.Limit <= 0 {
			return fmt.Errorf(
				"step %q: keyed rate limit must be positive", step.ID,
			)
		}
		if step.KeyedRateLimit.Period <= 0 {
			return fmt.Errorf(
				"step %q: keyed rate limit period must be positive",
				step.ID,
			)
		}
		if step.KeyedRateLimit.Units <= 0 {
			return fmt.Errorf(
				"step %q: keyed rate limit units must be positive", step.ID,
			)
		}
	}
	return nil
}
