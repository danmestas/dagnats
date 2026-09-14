// dag/task_type_sentinel_test.go
// Guards the charset invariant that #704's clean cut relies on: the
// group sentinel must be unreachable from a caller-supplied task type or
// worker group.
//
// Methodology: assert directly against consumername.GroupSentinel rather
// than a literal, so the test binds to the constant and cannot silently
// drift if the sentinel is ever changed again (it already moved once,
// from "@" to "=", when "@" turned out to be illegal in a NATS KV key).
// Positive + negative space: each case proves the sentinel is rejected
// AND that an otherwise-identical sentinel-free value is accepted, so a
// blanket-reject bug cannot pass.
//
// WHY THIS IS SECURITY-RELEVANT, not a charset nicety: #704 deleted the
// poll-time aliasing check (bridge.firstUnauthorizedAliasedReading) on
// the grounds that FilterFor(t, g) can no longer equal FilterFor(t+"."+
// g, ""). That holds ONLY while the sentinel cannot appear in a task
// type. Admit it and the #695 bypass returns verbatim: a token allowed
// "a" polls the ungrouped type "a.=b", deriving task.a.=b.* — the
// grouped queue's own filter — while AllowsWorkerGroup is never
// consulted because the request is ungrouped.
package dag

import (
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/consumername"
)

func TestValidTaskTypeRejectsGroupSentinel(t *testing.T) {
	sentinel := consumername.GroupSentinel
	if sentinel == "" {
		t.Fatal("GroupSentinel must not be empty")
	}
	// Negative space: every position a caller could try to smuggle it.
	for _, in := range []string{
		sentinel,
		"a" + sentinel + "b",
		sentinel + "build",
		"build" + sentinel,
		"a." + sentinel + "b",
	} {
		if err := ValidTaskType(in); err == nil {
			t.Errorf(
				"ValidTaskType(%q) = nil, want rejection: the sentinel "+
					"must be unreachable from a task type or the #695 "+
					"aliasing bypass returns", in,
			)
		}
	}
	// Positive space: the same shapes without the sentinel are fine, so
	// the rejection above is specific rather than a blanket failure.
	for _, in := range []string{"ab", "build", "a.b"} {
		if err := ValidTaskType(in); err != nil {
			t.Errorf("ValidTaskType(%q) = %v, want nil", in, err)
		}
	}
}

func TestValidWorkerGroupRejectsGroupSentinel(t *testing.T) {
	sentinel := consumername.GroupSentinel
	if strings.Contains("abcdefghijklmnopqrstuvwxyz0123456789-_", sentinel) {
		t.Fatalf(
			"GroupSentinel %q overlaps the legal group charset; it must "+
				"be outside it to stay unreachable from user input",
			sentinel,
		)
	}
	for _, in := range []string{
		sentinel,
		"gpu" + sentinel,
		sentinel + "gpu",
		"gp" + sentinel + "u",
	} {
		if err := ValidWorkerGroup(in); err == nil {
			t.Errorf(
				"ValidWorkerGroup(%q) = nil, want rejection", in,
			)
		}
	}
	if err := ValidWorkerGroup("gpu"); err != nil {
		t.Errorf("ValidWorkerGroup(\"gpu\") = %v, want nil", err)
	}
}
