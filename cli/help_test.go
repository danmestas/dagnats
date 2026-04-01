// cli/help_test.go
// Methodology: direct unit tests for help flag detection. Covers exact
// matches, near-misses, and empty input.
package cli

import "testing"

func TestHasHelpFlagPositive(t *testing.T) {
	cases := [][]string{
		{"--help"},
		{"-h"},
		{"other", "--help"},
		{"-h", "other"},
		{"--verbose", "--help", "--json"},
	}
	for _, args := range cases {
		if !HasHelpFlag(args) {
			t.Errorf("HasHelpFlag(%v) = false, want true", args)
		}
	}
}

func TestHasHelpFlagNegative(t *testing.T) {
	cases := [][]string{
		{},
		{"--helper"},
		{"-help"},
		{"help"},
		{"--version"},
		{"run", "start"},
	}
	for _, args := range cases {
		if HasHelpFlag(args) {
			t.Errorf("HasHelpFlag(%v) = true, want false", args)
		}
	}
}
