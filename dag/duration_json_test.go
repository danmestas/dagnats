package dag

// Methodology: pure JSON round-trip tests, no NATS. Workflow JSON
// written by people uses Go duration strings ("30s"); JSON written by
// dagnats itself uses integer nanoseconds. Every duration field must
// accept both, reject a malformed string with an error naming the
// field, and keep marshalling as nanoseconds so stored definitions
// are unchanged.

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestWorkflowDefAcceptsDurationStrings(t *testing.T) {
	data := []byte(`{
		"name": "d", "version": "1.0", "timeout": "1h",
		"default_retry": {"max_attempts": 3, "strategy": "exponential",
			"initial_delay": "2s", "max_delay": "1m", "multiplier": 2},
		"cancel_on": [{"event": "e", "match": {"left": "a", "op": "eq",
			"right": "a"}, "timeout": "10m"}],
		"steps": [{"id": "s", "task": "t", "timeout": "30s",
			"rate_limit": {"limit": 5, "period": "1s"},
			"keyed_rate_limit": {"key": "k", "limit": 1, "period": "2s",
				"units": 1}}]
	}`)
	var def WorkflowDef
	if err := json.Unmarshal(data, &def); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	checks := map[string][2]time.Duration{
		"workflow timeout":  {def.Timeout, time.Hour},
		"initial_delay":     {def.DefaultRetry.InitialDelay, 2 * time.Second},
		"max_delay":         {def.DefaultRetry.MaxDelay, time.Minute},
		"cancel_on timeout": {def.CancelOn[0].Timeout, 10 * time.Minute},
		"step timeout":      {def.Steps[0].Timeout, 30 * time.Second},
		"rate_limit":        {def.Steps[0].RateLimit.Period, time.Second},
		"keyed_rate_limit":  {def.Steps[0].KeyedRateLimit.Period, 2 * time.Second},
	}
	for name, c := range checks {
		if c[0] != c[1] {
			t.Errorf("%s = %v, want %v", name, c[0], c[1])
		}
	}
	if def.Steps[0].Task != "t" || def.Name != "d" {
		t.Fatalf("non-duration fields lost: %+v", def)
	}
}

func TestDurationFieldsStillAcceptNanoseconds(t *testing.T) {
	data := []byte(`{"id": "s", "task": "t", "timeout": 30000000000}`)
	var step StepDef
	if err := json.Unmarshal(data, &step); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if step.Timeout != 30*time.Second {
		t.Fatalf("timeout = %v, want 30s", step.Timeout)
	}
	out, err := json.Marshal(step)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	if !strings.Contains(string(out), `"timeout":30000000000`) {
		t.Fatalf("marshal must stay nanoseconds, got %s", out)
	}
}

func TestStepConfigAcceptsDurationStrings(t *testing.T) {
	loop, err := ParseAgentLoopConfig(StepDef{
		ID: "l", Task: "t", Type: StepTypeAgentLoop,
		Config: json.RawMessage(
			`{"max_iterations": 3, "max_duration": "10m", "loop_delay": "2s"}`),
	})
	if err != nil {
		t.Fatalf("ParseAgentLoopConfig: %v", err)
	}
	if loop.MaxDuration != 10*time.Minute || loop.LoopDelay != 2*time.Second {
		t.Fatalf("loop durations = %v, %v", loop.MaxDuration, loop.LoopDelay)
	}
	sleep, err := ParseSleepConfig(StepDef{
		ID: "z", Task: "t", Type: StepTypeSleep,
		Config: json.RawMessage(`{"duration": "90s"}`),
	})
	if err != nil {
		t.Fatalf("ParseSleepConfig: %v", err)
	}
	if sleep.Duration != 90*time.Second {
		t.Fatalf("sleep duration = %v, want 90s", sleep.Duration)
	}
	var approval ApprovalConfig
	if err := json.Unmarshal(
		[]byte(`{"timeout": "24h", "subject": "x"}`), &approval); err != nil {
		t.Fatalf("approval: %v", err)
	}
	if approval.Timeout != 24*time.Hour {
		t.Fatalf("approval timeout = %v, want 24h", approval.Timeout)
	}
	var wait WaitForEventOpts
	if err := json.Unmarshal([]byte(`{"timeout": "5m"}`), &wait); err != nil {
		t.Fatalf("wait: %v", err)
	}
	if wait.Timeout != 5*time.Minute {
		t.Fatalf("wait timeout = %v, want 5m", wait.Timeout)
	}
}

func TestMalformedDurationStringNamesTheField(t *testing.T) {
	var step StepDef
	err := json.Unmarshal(
		[]byte(`{"id": "s", "task": "t", "timeout": "soon"}`), &step)
	if err == nil {
		t.Fatal("want an error for a malformed duration")
	}
	if !strings.Contains(err.Error(), "timeout") {
		t.Fatalf("error should name the field: %v", err)
	}
	// Negative space: a JSON null leaves the zero value, no error.
	var policy RetryPolicy
	if err := json.Unmarshal([]byte(`null`), &policy); err != nil {
		t.Fatalf("null must decode cleanly: %v", err)
	}
}

func TestUnknownStepTypeIsAnError(t *testing.T) {
	var step StepDef
	err := json.Unmarshal(
		[]byte(`{"id": "s", "task": "t", "type": "bogus"}`), &step)
	if err == nil {
		t.Fatal("want an error for an unknown step type")
	}
	if !strings.Contains(err.Error(), "bogus") {
		t.Fatalf("error should name the value: %v", err)
	}
}
