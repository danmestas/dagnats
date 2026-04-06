// cli/serve_test.go
// Methodology: Unit tests for the serve --dry-run flag parsing and
// dry-run output. No NATS required -- tests flag detection and output
// capture using the real server.ResolveConfig / server.PrintDryRun path.
package cli

import (
	"os"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/server"
)

func TestHasDryRunFlag_Present(t *testing.T) {
	args := []string{"--dry-run"}

	// Positive: should detect --dry-run
	if !hasDryRunFlag(args) {
		t.Fatal("expected --dry-run to be detected")
	}

	// Positive: should detect among other flags
	args2 := []string{"--verbose", "--dry-run", "--json"}
	if !hasDryRunFlag(args2) {
		t.Fatal("expected --dry-run among other flags")
	}
}

func TestHasDryRunFlag_Absent(t *testing.T) {
	args := []string{"--verbose", "--json"}

	// Negative: should not detect --dry-run
	if hasDryRunFlag(args) {
		t.Fatal("should not detect --dry-run when absent")
	}

	// Negative: empty args should not detect
	if hasDryRunFlag([]string{}) {
		t.Fatal("should not detect --dry-run in empty args")
	}
}

func TestServeDryRun_OutputContainsExpectedSections(t *testing.T) {
	// Capture stdout from PrintDryRun with a valid temp config
	output := captureOutput(func() {
		rc := server.ResolveConfig()
		server.PrintDryRun(os.Stdout, rc)
	})

	// Positive: output should contain config source header
	if !strings.Contains(output, "Config source:") {
		t.Fatalf(
			"output should contain 'Config source:', got:\n%s",
			output,
		)
	}

	// Positive: output should contain validation section
	if !strings.Contains(output, "Validation:") {
		t.Fatalf(
			"output should contain 'Validation:', got:\n%s",
			output,
		)
	}

	// Positive: output should contain data_dir key
	if !strings.Contains(output, "data_dir:") {
		t.Fatalf(
			"output should contain 'data_dir:', got:\n%s",
			output,
		)
	}

	// Positive: output should end with Config OK or INVALID
	hasOK := strings.Contains(output, "Config OK")
	hasInvalid := strings.Contains(output, "Config INVALID")
	if !hasOK && !hasInvalid {
		t.Fatal("output should contain 'Config OK' or 'Config INVALID'")
	}
}
