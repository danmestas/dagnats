// worker/errors_test.go

// Tests for NonRetryableError: verifies wrapping, unwrapping, errors.As
// detection, and panic on nil. These are pure unit tests with no NATS.
package worker

import (
	"errors"
	"fmt"
	"testing"
)

func TestNonRetryableErrorWraps(t *testing.T) {
	inner := fmt.Errorf("bad input")
	nre := NewNonRetryableError(inner)

	if nre.Error() != "bad input" {
		t.Fatalf("Error() = %q, want %q", nre.Error(), "bad input")
	}
	if !errors.Is(nre, inner) {
		t.Fatal("errors.Is should match the wrapped error")
	}
}

func TestNonRetryableErrorAs(t *testing.T) {
	inner := fmt.Errorf("permanent failure")
	err := fmt.Errorf("handler: %w", NewNonRetryableError(inner))

	var nre *NonRetryableError
	if !errors.As(err, &nre) {
		t.Fatal("errors.As should find NonRetryableError in chain")
	}
	if nre.Err.Error() != "permanent failure" {
		t.Fatalf("unwrapped Err = %q, want %q", nre.Err.Error(), "permanent failure")
	}
}

func TestNonRetryableErrorNilPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil err, got nil")
		}
	}()
	NewNonRetryableError(nil)
}

func TestNonRetryableErrorNotDetectedOnPlainError(t *testing.T) {
	err := fmt.Errorf("transient failure")

	var nre *NonRetryableError
	if errors.As(err, &nre) {
		t.Fatal("errors.As should NOT find NonRetryableError on plain error")
	}
}
