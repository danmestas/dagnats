// dag/task_type_test.go
// Tests for ValidTaskType: the charset/shape rule that keeps a StepDef.Task
// safe to publish verbatim as a NATS subject token (issue #674).
// Methodology: table-driven valid/invalid cases, plus a Validate-level
// integration test proving a step with an invalid Task is rejected with a
// message naming the offending step. Positive + negative space per test.
package dag

import (
	"strings"
	"testing"
)

func TestValidTaskType(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		wantErr bool
	}{
		{"simple", "build", false},
		{"dotted_production_type", "dagger.call", false},
		{"multi_dotted", "build.linux", false},
		{"hyphen_underscore_digit", "go-test_1", false},
		{"empty", "", true},
		{"leading_space", " a", true},
		{"internal_space", "a b", true},
		{"wildcard_star", "a*", true},
		{"wildcard_gt", "a>", true},
		{"leading_dot", ".a", true},
		{"trailing_dot", "a.", true},
		{"empty_token", "a..b", true},
		{"129_chars", strings.Repeat("a", 129), true},
		{"128_chars_ok", strings.Repeat("a", 128), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidTaskType(tc.in)
			if tc.wantErr && err == nil {
				t.Fatalf("ValidTaskType(%q) = nil, want an error", tc.in)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("ValidTaskType(%q) = %v, want nil", tc.in, err)
			}
		})
	}
}

// TestValidateRejectsInvalidStepTask proves dag.Validate itself — not just
// ci/compile.go's mirrored check — refuses a step whose Task is unsafe to
// publish verbatim, and that the error names the offending step so a
// direct POST /workflows caller (which never runs ci/compile.go) gets a
// useful 400.
func TestValidateRejectsInvalidStepTask(t *testing.T) {
	def := WorkflowDef{Name: "bad-task", Version: "1", Steps: []StepDef{
		{ID: "step-a", Task: "go test", Type: StepTypeNormal},
	}}
	err := Validate(def)
	if err == nil {
		t.Fatal("Validate() = nil, want an error for an unsafe task type")
	}
	if !strings.Contains(err.Error(), `"step-a"`) {
		t.Fatalf("Validate() error = %q, want it to name step %q",
			err.Error(), "step-a")
	}

	// Negative: a safe task type does not trip this check.
	okDef := WorkflowDef{Name: "ok-task", Version: "1", Steps: []StepDef{
		{ID: "step-a", Task: "dagger.call", Type: StepTypeNormal},
	}}
	if err := Validate(okDef); err != nil {
		t.Fatalf("Validate(safe task type) = %v, want nil", err)
	}
}
