// Methodology: pure unit test of Sections against ConfigFile's shape.
// Positive: the three sections are listed. Negative: no legacy flat
// server key is.
package configfile

import (
	"slices"
	"testing"
)

func TestSections_AreConfigFileTopLevelKeys(t *testing.T) {
	got := Sections()
	for _, want := range []string{"workflows", "triggers", "policy"} {
		if !slices.Contains(got, want) {
			t.Fatalf("Sections() = %v, missing %q", got, want)
		}
	}
	if slices.Contains(got, "http_addr") || len(got) != 3 {
		t.Fatalf("Sections() = %v, want exactly workflows, triggers, policy", got)
	}
}
