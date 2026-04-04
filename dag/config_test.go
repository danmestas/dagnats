// dag/config_test.go

// Tests for Config parse helpers: type mismatch, nil config, valid
// config, and MarshalConfig. Methodology: each test asserts both a
// positive and negative condition per the 2-assertion minimum.
package dag

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestMarshalConfigNilPanics(t *testing.T) {
	defer func() {
		r := recover()
		// Positive: panics on nil
		if r == nil {
			t.Fatal("expected panic for nil cfg")
		}
		// Positive: panic message is descriptive
		if !strings.Contains(r.(string), "nil") {
			t.Fatalf("panic = %v", r)
		}
	}()
	MarshalConfig(nil)
}

func TestMarshalConfigRoundTrip(t *testing.T) {
	cfg := &AgentLoopConfig{MaxIterations: 5}
	raw := MarshalConfig(cfg)
	// Positive: non-nil result
	if raw == nil {
		t.Fatal("MarshalConfig returned nil")
	}
	var got AgentLoopConfig
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	// Positive: round-trip preserves value
	if got.MaxIterations != 5 {
		t.Fatalf("MaxIterations = %d, want 5", got.MaxIterations)
	}
}

func TestParseAgentLoopConfigWrongType(t *testing.T) {
	step := StepDef{ID: "s", Type: StepTypeNormal}
	_, err := ParseAgentLoopConfig(step)
	// Positive: error for wrong type
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
	// Positive: error mentions expected type
	if !strings.Contains(err.Error(), "AgentLoop") {
		t.Fatalf("error = %q", err)
	}
}

func TestParseAgentLoopConfigNilConfig(t *testing.T) {
	step := StepDef{
		ID: "s", Type: StepTypeAgentLoop, Config: nil,
	}
	_, err := ParseAgentLoopConfig(step)
	// Positive: error for nil config
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	// Positive: error mentions nil
	if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("error = %q", err)
	}
}

func TestParseAgentLoopConfigValid(t *testing.T) {
	step := StepDef{
		ID:   "s",
		Type: StepTypeAgentLoop,
		Config: MarshalConfig(
			&AgentLoopConfig{MaxIterations: 10},
		),
	}
	cfg, err := ParseAgentLoopConfig(step)
	// Positive: no error
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Positive: correct value
	if cfg.MaxIterations != 10 {
		t.Fatalf("MaxIterations = %d, want 10", cfg.MaxIterations)
	}
}

func TestParseMapConfigWrongType(t *testing.T) {
	step := StepDef{ID: "s", Type: StepTypeNormal}
	_, err := ParseMapConfig(step)
	// Positive: error returned
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
	// Positive: mentions Map
	if !strings.Contains(err.Error(), "Map") {
		t.Fatalf("error = %q", err)
	}
}

func TestParseMapConfigNilConfig(t *testing.T) {
	step := StepDef{
		ID: "s", Type: StepTypeMap, Config: nil,
	}
	_, err := ParseMapConfig(step)
	// Positive: error for nil
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	// Positive: mentions nil
	if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("error = %q", err)
	}
}

func TestParseMapConfigValid(t *testing.T) {
	step := StepDef{
		ID:     "s",
		Type:   StepTypeMap,
		Config: MarshalConfig(&MapConfig{MaxItems: 500}),
	}
	cfg, err := ParseMapConfig(step)
	// Positive: no error
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Positive: correct value
	if cfg.MaxItems != 500 {
		t.Fatalf("MaxItems = %d, want 500", cfg.MaxItems)
	}
}

func TestParseSleepConfigWrongType(t *testing.T) {
	step := StepDef{ID: "s", Type: StepTypeNormal}
	_, err := ParseSleepConfig(step)
	// Positive: error
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
	// Positive: mentions Sleep
	if !strings.Contains(err.Error(), "Sleep") {
		t.Fatalf("error = %q", err)
	}
}

func TestParseSleepConfigNilConfig(t *testing.T) {
	step := StepDef{
		ID: "s", Type: StepTypeSleep, Config: nil,
	}
	_, err := ParseSleepConfig(step)
	// Positive: error
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	// Positive: mentions nil
	if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("error = %q", err)
	}
}

func TestParseSleepConfigValid(t *testing.T) {
	step := StepDef{
		ID:   "s",
		Type: StepTypeSleep,
		Config: MarshalConfig(
			&SleepConfig{Duration: 5 * time.Second},
		),
	}
	cfg, err := ParseSleepConfig(step)
	// Positive: no error
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Positive: correct value
	if cfg.Duration != 5*time.Second {
		t.Fatalf("Duration = %v, want 5s", cfg.Duration)
	}
}

func TestParseWaitForEventConfigWrongType(t *testing.T) {
	step := StepDef{ID: "s", Type: StepTypeNormal}
	_, err := ParseWaitForEventConfig(step)
	// Positive: error
	if err == nil {
		t.Fatal("expected error for wrong type")
	}
	// Positive: mentions WaitForEvent
	if !strings.Contains(err.Error(), "WaitForEvent") {
		t.Fatalf("error = %q", err)
	}
}

func TestParseWaitForEventConfigNilConfig(t *testing.T) {
	step := StepDef{
		ID: "s", Type: StepTypeWaitForEvent, Config: nil,
	}
	_, err := ParseWaitForEventConfig(step)
	// Positive: error
	if err == nil {
		t.Fatal("expected error for nil config")
	}
	// Positive: mentions nil
	if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("error = %q", err)
	}
}

func TestParseWaitForEventConfigValid(t *testing.T) {
	opts := WaitForEventOpts{
		Event:   "test.event",
		Match:   Match{Left: "x", Op: MatchOpEq, Right: "input.y"},
		Timeout: 30 * time.Second,
	}
	step := StepDef{
		ID:     "s",
		Type:   StepTypeWaitForEvent,
		Config: MarshalConfig(&opts),
	}
	cfg, err := ParseWaitForEventConfig(step)
	// Positive: no error
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Positive: correct event name
	if cfg.Event != "test.event" {
		t.Fatalf("Event = %q, want test.event", cfg.Event)
	}
}
