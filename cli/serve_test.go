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

// #370: --fail-on-port-conflict opts startup into a hard failure on a
// default-port conflict instead of the silent auto-fallback.
func TestApplyServeFlagOverrides_FailOnPortConflictPresent(t *testing.T) {
	cfg := server.DefaultConfig()

	// Positive: the bare flag sets the field true.
	if err := applyServeFlagOverrides(
		[]string{"--fail-on-port-conflict"}, &cfg,
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.FailOnPortConflict {
		t.Fatal("expected FailOnPortConflict true after bare flag")
	}

	// Positive: the bare flag among other flags also sets true.
	cfg2 := server.DefaultConfig()
	if err := applyServeFlagOverrides(
		[]string{"--nats-ws-port=9222", "--fail-on-port-conflict"},
		&cfg2,
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg2.FailOnPortConflict {
		t.Fatal("expected FailOnPortConflict true among other flags")
	}
}

func TestApplyServeFlagOverrides_FailOnPortConflictAbsent(t *testing.T) {
	cfg := server.DefaultConfig()

	// Negative space: unrelated flags leave the field false.
	if err := applyServeFlagOverrides(
		[]string{"--nats-ws-no-tls"}, &cfg,
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.FailOnPortConflict {
		t.Fatal("FailOnPortConflict should stay false when absent")
	}

	// Negative space: explicit =false sets/leaves the field false.
	cfg2 := server.DefaultConfig()
	if err := applyServeFlagOverrides(
		[]string{"--fail-on-port-conflict=false"}, &cfg2,
	); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg2.FailOnPortConflict {
		t.Fatal("FailOnPortConflict should be false for =false")
	}
}

func TestApplyServeFlagOverrides_FailOnPortConflictInvalid(t *testing.T) {
	cfg := server.DefaultConfig()

	// Positive: a garbage value RETURNS an error (never panics) and
	// names the flag — user input is validated, not asserted.
	err := applyServeFlagOverrides(
		[]string{"--fail-on-port-conflict=maybe"}, &cfg,
	)
	if err == nil {
		t.Fatal("expected error for invalid flag value")
	}
	if !strings.Contains(err.Error(), "fail-on-port-conflict") {
		t.Fatalf("error should name the flag, got: %v", err)
	}

	// Negative space: the field is unchanged on the error path.
	if cfg.FailOnPortConflict {
		t.Fatal("FailOnPortConflict should be unchanged on error")
	}
}
