// bridge/poll_sentinel_test.go
// Guards the poll boundary's copy of the task-type charset against the
// group sentinel (#695, #704).
//
// Methodology: assert against consumername.GroupSentinel rather than a
// literal so the test binds to the constant. Positive + negative space
// per test: the sentinel is rejected AND sentinel-free equivalents are
// accepted, so a blanket-reject regression cannot pass.
//
// WHY THIS FILE EXISTS SEPARATELY FROM dag's: bridge does NOT call
// dag.ValidTaskType — it carries its own charset in isTaskTypeByte. #704
// deleted the poll-time aliasing check (firstUnauthorizedAliasedReading)
// because FilterFor(t, g) can no longer equal FilterFor(t+"."+g, ""),
// which holds only while the sentinel stays out of BOTH validators. With
// poll_alias_test.go removed alongside that check, this is the only
// coverage standing between a one-character change here and the #695
// bypass, which was demonstrated against a live server: a token allowed
// "zz4" polling the ungrouped type "zz4.=alpha" would derive
// task.zz4.=alpha.* — the grouped queue's filter — while
// AllowsWorkerGroup is never consulted, because the request is
// ungrouped.
package bridge

import (
	"testing"

	"github.com/danmestas/dagnats/internal/consumername"
)

func TestValidateTaskTypeRejectsGroupSentinel(t *testing.T) {
	sentinel := consumername.GroupSentinel
	if sentinel == "" {
		t.Fatal("GroupSentinel must not be empty")
	}
	// The exact shape of the #695 bypass, plus the other positions a
	// caller could try.
	for _, in := range []string{
		"zz4." + sentinel + "alpha",
		sentinel,
		"a" + sentinel + "b",
		sentinel + "build",
		"build" + sentinel,
	} {
		if err := validateTaskType(in); err == nil {
			t.Errorf(
				"validateTaskType(%q) = nil, want rejection: admitting "+
					"the sentinel here lets an ungrouped poll derive a "+
					"grouped queue's filter (#695)", in,
			)
		}
	}
	for _, in := range []string{"zz4.alpha", "build", "a.b"} {
		if err := validateTaskType(in); err != nil {
			t.Errorf("validateTaskType(%q) = %v, want nil", in, err)
		}
	}
}

// TestIsTaskTypeByteRejectsGroupSentinel pins the charset predicate
// itself, so the invariant is caught at the byte level even if
// validateTaskType's surrounding shape checks are ever restructured.
func TestIsTaskTypeByteRejectsGroupSentinel(t *testing.T) {
	sentinel := consumername.GroupSentinel
	if len(sentinel) != 1 {
		t.Fatalf(
			"GroupSentinel %q must be one byte for this check", sentinel,
		)
	}
	if isTaskTypeByte(sentinel[0]) {
		t.Fatalf(
			"isTaskTypeByte(%q) = true: the sentinel must never be a "+
				"legal task-type byte (#695, #704)", sentinel,
		)
	}
	// Negative space: the predicate still admits ordinary bytes, so the
	// assertion above is not passing because everything is rejected.
	for _, c := range []byte{'a', 'Z', '7', '-', '_', '.'} {
		if !isTaskTypeByte(c) {
			t.Errorf("isTaskTypeByte(%q) = false, want true", string(c))
		}
	}
}
