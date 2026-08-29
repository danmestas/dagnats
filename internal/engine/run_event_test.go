// internal/engine/run_event_test.go
// Tests for subjectToken (the workflow-name sanitizer feeding the
// event.run.{workflow}.{runID}.{status} subject) and runEventSubject.
// Methodology: table-driven cases covering allowed characters, disallowed
// characters, empty input, and the 128-char bound. Positive space asserts
// the exact sanitized output; negative space asserts the bound is enforced
// and never panics on adversarial input.
package engine

import (
	"strings"
	"testing"
)

func TestSubjectTokenSanitizesDisallowedChars(t *testing.T) {
	cases := map[string]string{
		"my-workflow":     "my-workflow",
		"my_workflow":     "my_workflow",
		"My.Workflow 1!":  "My_Workflow_1_",
		"a/b\\c*d>e.f":    "a_b_c_d_e_f",
		"":                "",
		"already_safe-99": "already_safe-99",
	}
	for input, want := range cases {
		got := subjectToken(input)
		if got != want {
			t.Fatalf("subjectToken(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestSubjectTokenBoundedTo128Chars(t *testing.T) {
	long := strings.Repeat("a", 500)
	got := subjectToken(long)
	if len(got) != 128 {
		t.Fatalf("subjectToken length = %d, want 128", len(got))
	}
	short := "short"
	got2 := subjectToken(short)
	if len(got2) != len(short) {
		t.Fatalf("subjectToken must not pad short input: got %q", got2)
	}
}

func TestRunEventSubjectShape(t *testing.T) {
	got := runEventSubject("my workflow", "run-123", "completed")
	want := "event.run.my_workflow.run-123.completed"
	if got != want {
		t.Fatalf("runEventSubject = %q, want %q", got, want)
	}
	// Negative space: an empty workflow token still produces a
	// well-formed subject (no double dots) rather than panicking —
	// callers with an unset WorkflowID must not crash the publish path.
	got2 := runEventSubject("", "run-1", "failed")
	if strings.Contains(got2, "..") {
		t.Fatalf("runEventSubject(%q) produced double-dot subject: %q", "", got2)
	}
}
