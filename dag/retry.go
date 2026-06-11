package dag

import (
	"encoding/json"
	"fmt"
	"math"
	"time"
)

// RetryStrategy selects the backoff algorithm for step retries.
type RetryStrategy int

const (
	RetryFixed       RetryStrategy = iota // Same delay every attempt
	RetryLinear                           // delay * attempt
	RetryExponential                      // delay * multiplier^(attempt-1)
)

var retryStrategyStrings = [...]string{
	"fixed", "linear", "exponential",
}

func (s RetryStrategy) String() string {
	if int(s) < len(retryStrategyStrings) {
		return retryStrategyStrings[s]
	}
	panic(fmt.Sprintf("unknown RetryStrategy %d", s))
}

func (s RetryStrategy) MarshalJSON() ([]byte, error) {
	return json.Marshal(s.String())
}

func (s *RetryStrategy) UnmarshalJSON(data []byte) error {
	var str string
	if err := json.Unmarshal(data, &str); err != nil {
		return err
	}
	for i, v := range retryStrategyStrings {
		if v == str {
			*s = RetryStrategy(i)
			return nil
		}
	}
	return fmt.Errorf("unknown RetryStrategy: %q", str)
}

// RetryAttemptCountMax bounds MaxAttempts on any retry policy.
// Validate rejects larger values at definition time so the engine's
// retry scheduler can assert the bound as a true unreachable
// invariant rather than a config-reachable one (see
// internal/engine scheduleRetryBackoff).
const RetryAttemptCountMax = 100_000

// RetryPolicy configures retry behavior for a step or as a workflow
// default. MaxAttempts=0 means no retries.
type RetryPolicy struct {
	MaxAttempts  int           `json:"max_attempts"`
	Strategy     RetryStrategy `json:"strategy"`
	InitialDelay time.Duration `json:"initial_delay"`
	MaxDelay     time.Duration `json:"max_delay"`
	Multiplier   float64       `json:"multiplier,omitempty"`
}

// ResolveRetryPolicy returns the effective retry policy for a step.
// Resolution order: step Retry → workflow DefaultRetry → legacy
// Retries field → nil (no retries).
func ResolveRetryPolicy(
	wfDef WorkflowDef, stepDef StepDef,
) *RetryPolicy {
	if stepDef.Retry != nil {
		return stepDef.Retry
	}
	if wfDef.DefaultRetry != nil {
		return wfDef.DefaultRetry
	}
	if stepDef.Retries > 0 {
		return &RetryPolicy{
			MaxAttempts:  stepDef.Retries,
			Strategy:     RetryFixed,
			InitialDelay: 5 * time.Second,
			MaxDelay:     5 * time.Second,
		}
	}
	return nil
}

// CalculateDelay returns the delay before the next retry attempt.
// Attempt is 1-based (first retry = attempt 1).
func CalculateDelay(
	policy RetryPolicy, attempt int,
) time.Duration {
	if attempt < 1 {
		panic("CalculateDelay: attempt must be >= 1")
	}
	var delay time.Duration
	switch policy.Strategy {
	case RetryFixed:
		delay = policy.InitialDelay
	case RetryLinear:
		delay = policy.InitialDelay * time.Duration(attempt)
	case RetryExponential:
		d := float64(policy.InitialDelay) *
			math.Pow(policy.Multiplier, float64(attempt-1))
		delay = time.Duration(d)
	default:
		delay = policy.InitialDelay
	}
	if policy.MaxDelay > 0 && delay > policy.MaxDelay {
		delay = policy.MaxDelay
	}
	return delay
}
