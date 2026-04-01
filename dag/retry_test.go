package dag

// Methodology: unit tests for retry policy types and logic.
// Pure — no NATS dependency.

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRetryStrategyStringAndJSON(t *testing.T) {
	// Positive: string representation
	if RetryFixed.String() != "fixed" {
		t.Fatalf("RetryFixed.String() = %q", RetryFixed.String())
	}
	if RetryLinear.String() != "linear" {
		t.Fatalf("RetryLinear.String() = %q", RetryLinear.String())
	}
	if RetryExponential.String() != "exponential" {
		t.Fatalf("RetryExponential.String() = %q",
			RetryExponential.String())
	}

	// Positive: JSON round-trip
	data, err := json.Marshal(RetryExponential)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var got RetryStrategy
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if got != RetryExponential {
		t.Fatalf("round-trip = %v, want Exponential", got)
	}
}

func TestRetryPolicyJSON(t *testing.T) {
	p := RetryPolicy{
		MaxAttempts:  3,
		Strategy:     RetryExponential,
		InitialDelay: 1 * time.Second,
		MaxDelay:     30 * time.Second,
		Multiplier:   2.0,
	}

	data, err := json.Marshal(p)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got RetryPolicy
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Positive: fields round-trip
	if got.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", got.MaxAttempts)
	}
	if got.Strategy != RetryExponential {
		t.Fatalf("Strategy = %v, want Exponential", got.Strategy)
	}
	if got.Multiplier != 2.0 {
		t.Fatalf("Multiplier = %f, want 2.0", got.Multiplier)
	}
}

func TestResolveRetryPolicyStepOverridesWorkflow(t *testing.T) {
	stepPolicy := &RetryPolicy{
		MaxAttempts: 5, Strategy: RetryExponential,
		InitialDelay: 2 * time.Second, Multiplier: 3.0,
	}
	wfDefault := &RetryPolicy{
		MaxAttempts: 2, Strategy: RetryFixed,
		InitialDelay: 1 * time.Second,
	}
	wfDef := WorkflowDef{DefaultRetry: wfDefault}
	stepDef := StepDef{Retry: stepPolicy}

	// Positive: step policy wins
	got := ResolveRetryPolicy(wfDef, stepDef)
	if got.MaxAttempts != 5 {
		t.Fatalf("MaxAttempts = %d, want 5", got.MaxAttempts)
	}
	if got.Strategy != RetryExponential {
		t.Fatalf("Strategy = %v, want Exponential", got.Strategy)
	}
}

func TestResolveRetryPolicyFallsToWorkflow(t *testing.T) {
	wfDefault := &RetryPolicy{
		MaxAttempts: 2, Strategy: RetryFixed,
		InitialDelay: 1 * time.Second,
	}
	wfDef := WorkflowDef{DefaultRetry: wfDefault}
	stepDef := StepDef{}

	// Positive: workflow default used
	got := ResolveRetryPolicy(wfDef, stepDef)
	if got.MaxAttempts != 2 {
		t.Fatalf("MaxAttempts = %d, want 2", got.MaxAttempts)
	}
}

func TestResolveRetryPolicyLegacyRetries(t *testing.T) {
	wfDef := WorkflowDef{}
	stepDef := StepDef{Retries: 3}

	// Positive: synthesized from legacy field
	got := ResolveRetryPolicy(wfDef, stepDef)
	if got == nil {
		t.Fatalf("expected non-nil policy from Retries=3")
	}
	if got.MaxAttempts != 3 {
		t.Fatalf("MaxAttempts = %d, want 3", got.MaxAttempts)
	}
	if got.Strategy != RetryFixed {
		t.Fatalf("Strategy = %v, want Fixed", got.Strategy)
	}
}

func TestResolveRetryPolicyNilWhenNone(t *testing.T) {
	got := ResolveRetryPolicy(WorkflowDef{}, StepDef{})
	if got != nil {
		t.Fatalf("expected nil, got %+v", got)
	}
}

func TestCalculateDelayFixed(t *testing.T) {
	p := RetryPolicy{
		Strategy: RetryFixed, InitialDelay: 5 * time.Second,
	}
	// Positive: same delay every attempt
	if d := CalculateDelay(p, 1); d != 5*time.Second {
		t.Fatalf("attempt 1 = %v, want 5s", d)
	}
	if d := CalculateDelay(p, 3); d != 5*time.Second {
		t.Fatalf("attempt 3 = %v, want 5s", d)
	}
}

func TestCalculateDelayLinear(t *testing.T) {
	p := RetryPolicy{
		Strategy: RetryLinear, InitialDelay: 2 * time.Second,
	}
	// Positive: delay * attempt
	if d := CalculateDelay(p, 1); d != 2*time.Second {
		t.Fatalf("attempt 1 = %v, want 2s", d)
	}
	if d := CalculateDelay(p, 3); d != 6*time.Second {
		t.Fatalf("attempt 3 = %v, want 6s", d)
	}
}

func TestCalculateDelayExponential(t *testing.T) {
	p := RetryPolicy{
		Strategy: RetryExponential, InitialDelay: 1 * time.Second,
		Multiplier: 2.0, MaxDelay: 30 * time.Second,
	}
	// Positive: 1s, 2s, 4s, 8s...
	if d := CalculateDelay(p, 1); d != 1*time.Second {
		t.Fatalf("attempt 1 = %v, want 1s", d)
	}
	if d := CalculateDelay(p, 3); d != 4*time.Second {
		t.Fatalf("attempt 3 = %v, want 4s", d)
	}

	// Positive: capped at MaxDelay
	if d := CalculateDelay(p, 10); d != 30*time.Second {
		t.Fatalf("attempt 10 = %v, want 30s (capped)", d)
	}
}

func TestCalculateDelayPanicsOnZeroAttempt(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic for attempt 0")
		}
	}()
	CalculateDelay(RetryPolicy{Strategy: RetryFixed}, 0)
}
