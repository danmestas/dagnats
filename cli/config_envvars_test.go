// cli/config_envvars_test.go
// Methodology: snapshot the human-readable `dagnats config show`
// output and assert every console-relevant env var name appears
// exactly once. The list is the single source of truth (also
// referenced by docs/console.md), so we test it stays complete.
package cli

import (
	"strings"
	"testing"
)

// consoleRelevantEnvVarNames enumerates the env vars docs/console.md
// promises operators will see in `dagnats config show`. Drift between
// the two breaks the operator's mental model — keep both lists in
// sync.
var consoleRelevantEnvVarNames = []string{
	"DAGNATS_HTTP_ADDR",
	"DAGNATS_CONSOLE_PASSWORD",
	"DAGNATS_CONSOLE_TRUST_FORWARDED_AUTH",
	"CONSOLE_READ_ONLY",
	"CONSOLE_CSRF_SECRET",
	"METRICS_AUTH",
	"METRICS_BASIC_USER",
	"METRICS_BASIC_PASS",
}

func TestConfigShowListsEveryConsoleEnvVar(t *testing.T) {
	out := captureOutput(func() {
		runConfigShowCmd([]string{})
	})
	if !strings.Contains(out, "console env vars:") {
		t.Fatalf("expected 'console env vars:' header, got:\n%s", out)
	}
	for _, name := range consoleRelevantEnvVarNames {
		if !strings.Contains(out, name) {
			t.Errorf("config show output missing env var %q\n%s",
				name, out)
		}
	}
}

func TestConfigShowMasksSensitiveEnvVarValues(t *testing.T) {
	t.Setenv("CONSOLE_CSRF_SECRET", "shhh-do-not-print-me")
	t.Setenv("METRICS_BASIC_PASS", "another-secret")
	out := captureOutput(func() {
		runConfigShowCmd([]string{})
	})
	if strings.Contains(out, "shhh-do-not-print-me") {
		t.Errorf("config show leaked CONSOLE_CSRF_SECRET value")
	}
	if strings.Contains(out, "another-secret") {
		t.Errorf("config show leaked METRICS_BASIC_PASS value")
	}
	// Should report "(set)" for sensitive vars that have a value.
	if !strings.Contains(out, "(set)") {
		t.Errorf("expected '(set)' marker for sensitive vars, got:\n%s",
			out)
	}
}

func TestConfigShowSurfacesNonSensitiveEnvVarValues(t *testing.T) {
	t.Setenv("METRICS_AUTH", "forward")
	out := captureOutput(func() {
		runConfigShowCmd([]string{})
	})
	// Non-sensitive vars should print their value verbatim.
	if !strings.Contains(out, "METRICS_AUTH") {
		t.Fatalf("METRICS_AUTH row missing from output")
	}
	if !strings.Contains(out, "forward") {
		t.Errorf("METRICS_AUTH value 'forward' not visible:\n%s", out)
	}
}

func TestConfigShowReportsDefaultsWhenEnvUnset(t *testing.T) {
	// Clear any env from the surrounding shell.
	for _, name := range consoleRelevantEnvVarNames {
		t.Setenv(name, "")
	}
	out := captureOutput(func() {
		runConfigShowCmd([]string{})
	})
	if !strings.Contains(out, "(default)") {
		t.Errorf("expected '(default)' marker on unset vars, got:\n%s",
			out)
	}
}
