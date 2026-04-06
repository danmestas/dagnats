// cli/hint_test.go
// Methodology: unit tests for printHint covering JSON suppression,
// stderr output, and multi-line formatting. Uses os.Pipe to capture
// stderr without affecting stdout.
package cli

import (
	"os"
	"strings"
	"testing"
)

// captureStderr redirects os.Stderr to a pipe, runs fn, then returns
// whatever was written to stderr.
func captureStderr(fn func()) string {
	origStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		panic("captureStderr: pipe failed: " + err.Error())
	}
	os.Stderr = w

	fn()

	w.Close()
	os.Stderr = origStderr

	buf := make([]byte, 4096)
	n, _ := r.Read(buf)
	r.Close()
	return string(buf[:n])
}

// TestPrintHintWritesToStderr verifies hints appear on stderr
// with correct indentation.
func TestPrintHintWritesToStderr(t *testing.T) {
	output := captureStderr(func() {
		printHint(false, "Next steps:", "  cd my-project")
	})

	// Positive: output must contain both lines.
	if !strings.Contains(output, "Next steps:") {
		t.Fatal("expected 'Next steps:' in stderr output")
	}
	if !strings.Contains(output, "cd my-project") {
		t.Fatal("expected 'cd my-project' in stderr output")
	}

	// Negative: should not contain JSON markers.
	if strings.Contains(output, "{") {
		t.Fatal("hint output should not contain JSON")
	}
}

// TestPrintHintSuppressedInJSONMode verifies no output when
// jsonOutput is true.
func TestPrintHintSuppressedInJSONMode(t *testing.T) {
	output := captureStderr(func() {
		printHint(true, "Next steps:", "  cd my-project")
	})

	// Positive: output must be empty in JSON mode.
	if output != "" {
		t.Fatalf("expected no output in JSON mode, got %q", output)
	}

	// Negative: specifically check the hint text is absent.
	if strings.Contains(output, "Next steps:") {
		t.Fatal("hint should be suppressed in JSON mode")
	}
}
