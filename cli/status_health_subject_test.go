// cli/status_health_subject_test.go
// Tests parseTaskFromSubject, the status view's fallback for naming a
// consumer's task type when the durable name is not self-describing.
//
// Methodology: table-driven over the filter shapes actually produced by
// consumername.FilterFor, including the grouped shape #704 introduced.
// Positive + negative space: every grouped case is paired with the
// ungrouped filter for the CONCATENATED name, which is a different task
// type that must keep resolving to itself — that pair is the whole point
// of #704's sentinel and the reason this parser cannot simply drop the
// second-to-last token.
package cli

import (
	"testing"

	"github.com/danmestas/dagnats/internal/consumername"
)

func TestParseTaskFromSubject(t *testing.T) {
	sentinel := consumername.GroupSentinel
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"ungrouped_star", "task.greet.*", "greet"},
		{"ungrouped_gt", "task.greet.>", "greet"},
		{"dotted_ungrouped", "task.dagger.call.*", "dagger.call"},
		{
			"grouped",
			"task.greet." + sentinel + "fast.*",
			"greet",
		},
		{
			"dotted_task_grouped",
			"task.dagger.call." + sentinel + "fast.*",
			"dagger.call",
		},
		// The pair that proves the sentinel is load-bearing here: this
		// is a DIFFERENT task type from ("dagger.call", group "fast"),
		// and it must resolve to itself rather than being mistaken for
		// a grouped filter.
		{
			"ungrouped_type_matching_a_group_name",
			"task.dagger.call.fast.*",
			"dagger.call.fast",
		},
		{"not_a_task_subject", "event.run.x.y", ""},
		{"too_short", "task.", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseTaskFromSubject(tc.in)
			if got != tc.want {
				t.Fatalf(
					"parseTaskFromSubject(%q) = %q, want %q",
					tc.in, got, tc.want,
				)
			}
			// Negative space: the sentinel must never survive into
			// rendered output, whatever the input shape.
			if got != "" && containsSentinel(got, sentinel) {
				t.Fatalf(
					"parseTaskFromSubject(%q) = %q, which still "+
						"contains the group sentinel %q",
					tc.in, got, sentinel,
				)
			}
		})
	}
}

func containsSentinel(s, sentinel string) bool {
	for i := 0; i+len(sentinel) <= len(s); i++ {
		if s[i:i+len(sentinel)] == sentinel {
			return true
		}
	}
	return false
}
