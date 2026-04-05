// dag/priority_test.go
// Tests for priority resolution: rule matching, defaults, clamping.
package dag

import (
	"encoding/json"
	"testing"
)

func TestResolvePriorityMatchesRule(t *testing.T) {
	cfg := &PriorityConfig{
		Key:   "tier",
		Rules: map[string]int{"enterprise": 300, "pro": 60},
	}
	input := json.RawMessage(`{"tier":"enterprise"}`)
	offset := ResolvePriority(cfg, input)
	if offset != 300 {
		t.Fatalf("offset = %d, want 300", offset)
	}
	input2 := json.RawMessage(`{"tier":"free"}`)
	if ResolvePriority(cfg, input2) != 0 {
		t.Fatal("unmatched should return 0")
	}
}

func TestResolvePriorityDefaultOffset(t *testing.T) {
	cfg := &PriorityConfig{
		Key: "tier", Rules: map[string]int{"vip": 600},
		DefaultOffset: -100,
	}
	input := json.RawMessage(`{"tier":"free"}`)
	if ResolvePriority(cfg, input) != -100 {
		t.Fatal("should return DefaultOffset")
	}
	if ResolvePriority(nil, input) != 0 {
		t.Fatal("nil config should return 0")
	}
}

func TestResolvePriorityClampsOffset(t *testing.T) {
	cfg := &PriorityConfig{
		Key: "tier", Rules: map[string]int{"whale": 9999},
	}
	input := json.RawMessage(`{"tier":"whale"}`)
	if ResolvePriority(cfg, input) != 600 {
		t.Fatal("should clamp to 600")
	}
	cfg2 := &PriorityConfig{
		Key: "tier", Rules: map[string]int{"bot": -9999},
	}
	input2 := json.RawMessage(`{"tier":"bot"}`)
	if ResolvePriority(cfg2, input2) != -600 {
		t.Fatal("should clamp to -600")
	}
}

func TestResolvePriorityBadPath(t *testing.T) {
	cfg := &PriorityConfig{
		Key: "missing.path", Rules: map[string]int{"x": 100},
	}
	input := json.RawMessage(`{"other":"val"}`)
	if ResolvePriority(cfg, input) != 0 {
		t.Fatal("bad path should return 0")
	}
}
