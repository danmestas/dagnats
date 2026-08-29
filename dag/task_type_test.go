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

// TestValidateRejectsInvalidWorkerGroup proves WorkerGroup is checked
// against the same subject-token rule as Task — StepSubject appends it
// as its own token ("task.{Task}.{WorkerGroup}.{runID}"), so an unsafe
// value is exactly as dangerous as an unsafe Task (issue #674 review).
func TestValidateRejectsInvalidWorkerGroup(t *testing.T) {
	def := WorkflowDef{Name: "bad-group", Version: "1", Steps: []StepDef{
		{
			ID: "step-a", Task: "render", WorkerGroup: "gpu fast",
			Type: StepTypeNormal,
		},
	}}
	err := Validate(def)
	if err == nil {
		t.Fatal("Validate() = nil, want an error for an unsafe worker_group")
	}
	if !strings.Contains(err.Error(), `"step-a"`) {
		t.Fatalf("Validate() error = %q, want it to name step %q",
			err.Error(), "step-a")
	}

	// Negative: a safe worker_group does not trip this check.
	okDef := WorkflowDef{Name: "ok-group", Version: "1", Steps: []StepDef{
		{
			ID: "step-a", Task: "render", WorkerGroup: "gpu-fast",
			Type: StepTypeNormal,
		},
	}}
	if err := Validate(okDef); err != nil {
		t.Fatalf("Validate(safe worker_group) = %v, want nil", err)
	}
}

// TestValidateRejectsDottedTaskWithWorkerGroup is the regression guard
// for the filter/durable-name collision found in review:
// consumername.FilterFor("render.gpu", "") and FilterFor("render",
// "gpu") derive the identical filter subject AND durable name, so a
// workflow that pairs a dotted Task with a WorkerGroup can silently
// collide with an unrelated ungrouped step. Both halves stay legal on
// their own — only the combination on one step is rejected.
func TestValidateRejectsDottedTaskWithWorkerGroup(t *testing.T) {
	// Positive: dotted task + worker_group together is rejected.
	combined := WorkflowDef{Name: "dotted-plus-group", Version: "1",
		Steps: []StepDef{
			{
				ID: "step-a", Task: "render.gpu", WorkerGroup: "fast",
				Type: StepTypeNormal,
			},
		}}
	err := Validate(combined)
	if err == nil {
		t.Fatal(
			"Validate() = nil, want an error for a dotted task " +
				"combined with worker_group",
		)
	}
	if !strings.Contains(err.Error(), `"step-a"`) {
		t.Fatalf("Validate() error = %q, want it to name step %q",
			err.Error(), "step-a")
	}

	// Negative, form 1: dotted task alone (no worker_group) stays legal.
	dottedAlone := WorkflowDef{Name: "dotted-alone", Version: "1",
		Steps: []StepDef{
			{ID: "step-a", Task: "render.gpu", Type: StepTypeNormal},
		}}
	if err := Validate(dottedAlone); err != nil {
		t.Fatalf("Validate(dotted task, no worker_group) = %v, want nil",
			err)
	}

	// Negative, form 2: undotted task with worker_group stays legal.
	undottedWithGroup := WorkflowDef{Name: "undotted-with-group",
		Version: "1", Steps: []StepDef{
			{
				ID: "step-a", Task: "render", WorkerGroup: "gpu",
				Type: StepTypeNormal,
			},
		}}
	if err := Validate(undottedWithGroup); err != nil {
		t.Fatalf(
			"Validate(undotted task, worker_group set) = %v, want nil",
			err,
		)
	}
}
