// Methodology: Pure unit tests for --config flag extraction
// and config source display formatting. No external dependencies.

package cli

import (
	"bytes"
	"testing"
)

func TestExtractConfigFlag_Present(t *testing.T) {
	args := []string{
		"--config=/tmp/custom.yaml", "--json",
	}

	got := extractConfigFlag(args)

	// Positive: flag value extracted
	if got != "/tmp/custom.yaml" {
		t.Errorf(
			"extractConfigFlag() = %q, want %q",
			got, "/tmp/custom.yaml",
		)
	}

	// Negative: should not be empty
	if got == "" {
		t.Error("extractConfigFlag() returned empty")
	}
}

func TestExtractConfigFlag_Absent(t *testing.T) {
	args := []string{"--json"}

	got := extractConfigFlag(args)

	// Positive: empty when absent
	if got != "" {
		t.Errorf(
			"extractConfigFlag() = %q, want empty",
			got,
		)
	}

	// Negative: should not match other flags
	args2 := []string{"--configure=yes"}
	got2 := extractConfigFlag(args2)
	if got2 != "" {
		t.Errorf(
			"extractConfigFlag(--configure) = %q, "+
				"want empty", got2,
		)
	}
}

func TestPrintConfigSource_WithPath(t *testing.T) {
	var buf bytes.Buffer
	printConfigSource(&buf, "/etc/dagnats/dagnats.yaml")

	got := buf.String()

	// Positive: mentions the path
	if got == "" {
		t.Fatal("printConfigSource() wrote nothing")
	}

	// Positive: contains the file path
	want := "/etc/dagnats/dagnats.yaml"
	if !bytes.Contains(buf.Bytes(), []byte(want)) {
		t.Errorf("output = %q, want to contain %q",
			got, want)
	}
}

func TestPrintConfigSource_NoPath(t *testing.T) {
	var buf bytes.Buffer
	printConfigSource(&buf, "")

	got := buf.String()

	// Positive: mentions defaults
	if got == "" {
		t.Fatal("printConfigSource() wrote nothing")
	}

	// Positive: mentions "defaults"
	want := "defaults"
	if !bytes.Contains(buf.Bytes(), []byte(want)) {
		t.Errorf("output = %q, want to contain %q",
			got, want)
	}
}
