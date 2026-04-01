// cli/version_test.go
// Tests for version output.
// Methodology: unit tests that verify printVersion produces correct
// output for both default and overridden version strings.
package cli

import (
	"strings"
	"testing"
)

func TestPrintVersionContainsVersionString(t *testing.T) {
	original := Version
	defer func() { Version = original }()

	Version = "1.2.3"
	output := captureOutput(func() {
		printVersion()
	})

	// Positive: output must contain the version number
	if !strings.Contains(output, "1.2.3") {
		t.Fatalf("expected '1.2.3' in output, got: %s", output)
	}

	// Positive: output must contain the program name
	if !strings.Contains(output, "dagnats") {
		t.Fatalf("expected 'dagnats' in output, got: %s", output)
	}
}

func TestPrintVersionDefaultIsDev(t *testing.T) {
	original := Version
	defer func() { Version = original }()

	Version = "dev"
	output := captureOutput(func() {
		printVersion()
	})

	// Positive: default version should be "dev"
	if !strings.Contains(output, "dev") {
		t.Fatalf("expected 'dev' in output, got: %s", output)
	}

	// Negative: should not contain placeholder text
	if strings.Contains(output, "unknown") {
		t.Fatal("output should not contain 'unknown'")
	}
}
