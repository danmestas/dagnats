// dag/ratelimit_test.go

// Tests for rate limit types and validation.
// Methodology: verify that rate limit configuration is validated correctly,
// that StepRef methods set fields properly, and that invalid configs are rejected.
package dag

import (
	"strings"
	"testing"
	"time"
)

func TestRateLimitValidationZeroLimit(t *testing.T) {
	b := NewWorkflow("test")
	b.Task("a", "task-a").WithRateLimit(RateLimit{Limit: 0, Period: time.Minute})
	_, err := b.Build()
	if err == nil {
		t.Fatal("expected error for zero rate limit")
	}
	if !strings.Contains(err.Error(), "positive") {
		t.Fatalf("error should mention positive, got: %v", err)
	}
}

func TestRateLimitValidationZeroPeriod(t *testing.T) {
	b := NewWorkflow("test")
	b.Task("a", "task-a").WithRateLimit(RateLimit{Limit: 100, Period: 0})
	_, err := b.Build()
	if err == nil {
		t.Fatal("expected error for zero period")
	}
	if !strings.Contains(err.Error(), "positive") {
		t.Fatalf("error should mention positive, got: %v", err)
	}
}

func TestKeyedRateLimitValidationEmptyKey(t *testing.T) {
	b := NewWorkflow("test")
	b.Task("a", "task-a").WithKeyedRateLimit(KeyedRateLimit{
		Key:    "",
		Limit:  10,
		Period: time.Minute,
		Units:  1,
	})
	_, err := b.Build()
	if err == nil {
		t.Fatal("expected error for empty key")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("error should mention empty, got: %v", err)
	}
}

func TestKeyedRateLimitValidationZeroLimit(t *testing.T) {
	b := NewWorkflow("test")
	b.Task("a", "task-a").WithKeyedRateLimit(KeyedRateLimit{
		Key:    "data.user_id",
		Limit:  0,
		Period: time.Minute,
		Units:  1,
	})
	_, err := b.Build()
	if err == nil {
		t.Fatal("expected error for zero limit")
	}
	if !strings.Contains(err.Error(), "positive") {
		t.Fatalf("error should mention positive, got: %v", err)
	}
}

func TestKeyedRateLimitValidationZeroUnits(t *testing.T) {
	b := NewWorkflow("test")
	b.Task("a", "task-a").WithKeyedRateLimit(KeyedRateLimit{
		Key:    "data.user_id",
		Limit:  10,
		Period: time.Minute,
		Units:  0,
	})
	_, err := b.Build()
	if err == nil {
		t.Fatal("expected error for zero units")
	}
	if !strings.Contains(err.Error(), "positive") {
		t.Fatalf("error should mention positive, got: %v", err)
	}
}

func TestRateLimitBuilderSetsFields(t *testing.T) {
	b := NewWorkflow("test")
	b.Task("a", "task-a").WithRateLimit(RateLimit{
		Limit:  100,
		Period: time.Minute,
	})
	wf, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if wf.Steps[0].RateLimit == nil {
		t.Fatal("rate limit must be set")
	}
	if wf.Steps[0].RateLimit.Limit != 100 {
		t.Fatalf("limit = %d, want 100", wf.Steps[0].RateLimit.Limit)
	}
	if wf.Steps[0].RateLimit.Period != time.Minute {
		t.Fatalf("period = %v, want 1m", wf.Steps[0].RateLimit.Period)
	}
}

func TestKeyedRateLimitBuilderSetsFields(t *testing.T) {
	b := NewWorkflow("test")
	b.Task("a", "task-a").WithKeyedRateLimit(KeyedRateLimit{
		Key:    "data.user_id",
		Limit:  50,
		Period: time.Hour,
		Units:  2,
	})
	wf, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if wf.Steps[0].KeyedRateLimit == nil {
		t.Fatal("keyed rate limit must be set")
	}
	krl := wf.Steps[0].KeyedRateLimit
	if krl.Key != "data.user_id" {
		t.Fatalf("key = %q, want %q", krl.Key, "data.user_id")
	}
	if krl.Limit != 50 {
		t.Fatalf("limit = %d, want 50", krl.Limit)
	}
	if krl.Period != time.Hour {
		t.Fatalf("period = %v, want 1h", krl.Period)
	}
	if krl.Units != 2 {
		t.Fatalf("units = %d, want 2", krl.Units)
	}
}

func TestRateLimitChaining(t *testing.T) {
	b := NewWorkflow("test")
	a := b.Task("a", "task-a")
	b.Task("b", "task-b").
		After(a).
		WithRateLimit(RateLimit{Limit: 10, Period: time.Second}).
		WithTimeout(5 * time.Second)
	wf, err := b.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	step := findStep(wf, "b")
	if step == nil {
		t.Fatal("step 'b' not found")
	}
	if step.RateLimit == nil {
		t.Fatal("rate limit must be set")
	}
	if step.Timeout != 5*time.Second {
		t.Fatalf("timeout = %v, want 5s", step.Timeout)
	}
}
