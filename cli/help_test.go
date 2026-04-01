// cli/help_test.go
// Tests for shared help flag detection.
// Methodology: unit tests for HasHelpFlag with positive and negative cases.
package cli

import "testing"

func TestHasHelpFlagDetectsLongFlag(t *testing.T) {
	// Positive: --help should be detected
	if !HasHelpFlag([]string{"--help"}) {
		t.Fatal("HasHelpFlag should return true for --help")
	}

	// Negative: unrelated flags should not trigger help
	if HasHelpFlag([]string{"--verbose"}) {
		t.Fatal("HasHelpFlag should return false for --verbose")
	}
}

func TestHasHelpFlagDetectsShortFlag(t *testing.T) {
	// Positive: -h should be detected
	if !HasHelpFlag([]string{"-h"}) {
		t.Fatal("HasHelpFlag should return true for -h")
	}

	// Negative: empty args should not trigger help
	if HasHelpFlag([]string{}) {
		t.Fatal("HasHelpFlag should return false for empty args")
	}
}

func TestHasHelpFlagDetectsInMiddle(t *testing.T) {
	// Positive: --help among other args should be detected
	if !HasHelpFlag([]string{"list", "--help"}) {
		t.Fatal("HasHelpFlag should detect --help among args")
	}

	// Negative: partial match should not trigger
	if HasHelpFlag([]string{"--helper"}) {
		t.Fatal("HasHelpFlag should not match --helper")
	}
}
