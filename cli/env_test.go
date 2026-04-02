// cli/env_test.go
// Methodology: unit tests for env var resolution with deprecation fallback.
// Uses t.Setenv for isolation. No NATS dependency. Covers: new name wins,
// old name falls back with warning, default used, panics on empty args.
package cli

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestGetEnvWithFallback_NewNameWins(t *testing.T) {
	t.Setenv("TEST_NEW_VAR", "new-value")
	t.Setenv("TEST_OLD_VAR", "old-value")

	got := GetEnvWithFallback("TEST_NEW_VAR", "TEST_OLD_VAR", "default")

	if got != "new-value" {
		t.Fatalf("expected 'new-value', got %q", got)
	}
	// Negative: old value must not leak through when new is set.
	if got == "old-value" {
		t.Fatal("old value should not win when new is set")
	}
}

func TestGetEnvWithFallback_OldNameFallback(t *testing.T) {
	// Only old name set; new name unset.
	t.Setenv("TEST_FALLBACK_OLD", "legacy-value")

	// Capture stderr to verify deprecation warning.
	origStderr := os.Stderr
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stderr = w

	got := GetEnvWithFallback(
		"TEST_FALLBACK_NEW", "TEST_FALLBACK_OLD", "default",
	)

	w.Close()
	os.Stderr = origStderr

	var buf bytes.Buffer
	const maxWarningBytes = 4096
	_, _ = buf.ReadFrom(&limitedReader{r: r, n: maxWarningBytes})
	r.Close()
	warning := buf.String()

	if got != "legacy-value" {
		t.Fatalf("expected 'legacy-value', got %q", got)
	}
	if !strings.Contains(warning, "deprecated") {
		t.Fatalf(
			"expected deprecation warning, got %q", warning,
		)
	}
}

func TestGetEnvWithFallback_Default(t *testing.T) {
	// Neither env var set.
	got := GetEnvWithFallback(
		"TEST_MISSING_NEW", "TEST_MISSING_OLD", "fallback",
	)

	if got != "fallback" {
		t.Fatalf("expected 'fallback', got %q", got)
	}
	// Negative: must not return empty string.
	if got == "" {
		t.Fatal("should not return empty when default is set")
	}
}

func TestGetEnvWithFallback_EmptyNewNamePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for empty newName")
		}
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "newName") {
			t.Fatalf("panic message should mention newName: %s", msg)
		}
	}()
	GetEnvWithFallback("", "OLD", "default")
}

func TestGetEnvWithFallback_EmptyOldNamePanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for empty oldName")
		}
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "oldName") {
			t.Fatalf("panic message should mention oldName: %s", msg)
		}
	}()
	GetEnvWithFallback("NEW", "", "default")
}

// limitedReader prevents unbounded reads from the pipe.
type limitedReader struct {
	r io.Reader
	n int64
}

func (lr *limitedReader) Read(p []byte) (int, error) {
	if lr.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > lr.n {
		p = p[:lr.n]
	}
	n, err := lr.r.Read(p)
	lr.n -= int64(n)
	return n, err
}
