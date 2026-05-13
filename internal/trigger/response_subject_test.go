// trigger/response_subject_test.go
//
// Methodology: pure unit tests for the engine-private response subject
// helper. ADR-013 mandates that this string is produced in exactly one
// place; these tests pin the shape and the empty-runID assertion.
package trigger

import (
	"strings"
	"testing"
)

func TestResponseSubjectShape(t *testing.T) {
	got := ResponseSubject("run-abc")
	want := "dagnats.http.response.run-abc"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	// Negative: distinct runID yields distinct subject.
	other := ResponseSubject("run-xyz")
	if got == other {
		t.Fatalf("distinct runIDs must produce distinct subjects")
	}
}

func TestResponseSubjectEmptyRunIDPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for empty runID")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("recover() = %T, want string", r)
		}
		if !strings.Contains(msg, "runID") {
			t.Fatalf("panic %q must mention runID", msg)
		}
	}()
	_ = ResponseSubject("")
}

func TestResponseSubjectStable(t *testing.T) {
	// ADR-013: subject is engine-private. The shape must stay stable
	// because both API handler and engine respond-step compare it.
	if ResponseSubject("r1") != "dagnats.http.response.r1" {
		t.Fatal("shape drift: dagnats.http.response.<runID>")
	}
	if ResponseSubject("a/b") != "dagnats.http.response.a/b" {
		t.Fatal("runID is passed through verbatim")
	}
}
