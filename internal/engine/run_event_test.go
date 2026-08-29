// internal/engine/run_event_test.go
// Tests for runEventSubject, the event.run.{workflow}.{runID}.{status}
// subject builder. The workflow-name sanitizer it depends on
// (natsutil.SubjectToken) moved to internal/natsutil in #624 along with
// its own tests, so only the subject-shape tests remain here.
// Methodology: table-driven/spot cases asserting exact subject shape and
// that a degenerate (empty workflow) input never panics or produces a
// malformed double-dot subject.
package engine

import (
	"strings"
	"testing"
)

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
