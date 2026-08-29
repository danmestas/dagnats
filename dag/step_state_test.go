// dag/step_state_test.go

// Tests for StepState.StartedAt/CompletedAt (#626): JSON round-trip
// fidelity for the additive per-step timestamp fields. Methodology: assert
// the marshaled bytes omit the fields when unset (omitempty), assert they
// survive a marshal/unmarshal round trip when set, and assert legacy JSON
// without the fields deserializes to nil pointers rather than erroring.
package dag

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

func TestStepState_Timestamps_OmittedWhenZero(t *testing.T) {
	t.Parallel()
	state := StepState{Status: StepStatusPending}

	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if bytes.Contains(data, []byte(`"started_at"`)) {
		t.Fatalf("zero StepState must omit started_at: %s", data)
	}
	if bytes.Contains(data, []byte(`"completed_at"`)) {
		t.Fatalf("zero StepState must omit completed_at: %s", data)
	}
}

func TestStepState_Timestamps_RoundTrip(t *testing.T) {
	t.Parallel()
	started := time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC)
	completed := started.Add(5 * time.Second)
	state := StepState{
		Status:      StepStatusCompleted,
		StartedAt:   &started,
		CompletedAt: &completed,
	}

	data, err := json.Marshal(state)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !bytes.Contains(data, []byte("started_at")) {
		t.Fatalf("set StartedAt must be present: %s", data)
	}
	if !bytes.Contains(data, []byte("completed_at")) {
		t.Fatalf("set CompletedAt must be present: %s", data)
	}

	var decoded StepState
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if decoded.StartedAt == nil || !decoded.StartedAt.Equal(started) {
		t.Fatalf("StartedAt round-trip = %v, want %v",
			decoded.StartedAt, started)
	}
	if decoded.CompletedAt == nil || !decoded.CompletedAt.Equal(completed) {
		t.Fatalf("CompletedAt round-trip = %v, want %v",
			decoded.CompletedAt, completed)
	}
}

func TestStepState_Timestamps_LegacyJSONDeserializesNil(t *testing.T) {
	t.Parallel()
	legacy := []byte(
		`{"status":"completed","attempts":1,"output":"eyJvayI6dHJ1ZX0="}`,
	)

	var decoded StepState
	if err := json.Unmarshal(legacy, &decoded); err != nil {
		t.Fatalf("Unmarshal legacy snapshot: %v", err)
	}
	if decoded.StartedAt != nil {
		t.Fatalf("legacy snapshot must deserialize StartedAt nil, got %v",
			decoded.StartedAt)
	}
	if decoded.CompletedAt != nil {
		t.Fatalf(
			"legacy snapshot must deserialize CompletedAt nil, got %v",
			decoded.CompletedAt,
		)
	}
}
