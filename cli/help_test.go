// cli/help_test.go
// Methodology: direct unit tests for help flag detection and usage text.
// Covers exact matches, near-misses, empty input, and --json in help output.
package cli

import (
	"strings"
	"testing"
)

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

// TestRootUsageMentionsJSON verifies that the root help text includes
// the global --json flag documentation.
func TestRootUsageMentionsJSON(t *testing.T) {
	output := captureOutput(func() {
		printUsage()
	})

	// Positive: must contain Global flags and --json
	if !strings.Contains(output, "Global flags:") {
		t.Fatal("root usage missing 'Global flags:' section")
	}
	if !strings.Contains(output, "--json") {
		t.Fatal("root usage missing '--json' flag")
	}
}

// TestRunUsageMentionsJSON verifies that the run subcommand help
// text includes [--json] in the usage line.
func TestRunUsageMentionsJSON(t *testing.T) {
	output := captureOutput(func() {
		printRunUsage()
	})

	// Positive: usage line must include [--json]
	if !strings.Contains(output, "[--json]") {
		t.Fatal("run usage missing '[--json]'")
	}
	// Negative: must still contain the command keyword
	if !strings.Contains(output, "dagnats run") {
		t.Fatal("run usage missing 'dagnats run'")
	}
}
