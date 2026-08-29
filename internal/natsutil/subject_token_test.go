// internal/natsutil/subject_token_test.go
// Tests for SubjectToken (moved from internal/engine's run_event.go by
// #624 so the BUILD_LOGS subject builder can reuse it). Methodology:
// table-driven cases covering allowed characters, disallowed characters,
// empty input, and the 128-char bound. Positive space asserts the exact
// sanitized output; negative space asserts the bound is enforced and the
// function never panics on adversarial input.
package natsutil

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
		got := SubjectToken(input)
		if got != want {
			t.Fatalf("SubjectToken(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestSubjectTokenBoundedTo128Chars(t *testing.T) {
	long := strings.Repeat("a", 500)
	got := SubjectToken(long)
	if len(got) != 128 {
		t.Fatalf("SubjectToken length = %d, want 128", len(got))
	}
	short := "short"
	got2 := SubjectToken(short)
	if len(got2) != len(short) {
		t.Fatalf("SubjectToken must not pad short input: got %q", got2)
	}
}
