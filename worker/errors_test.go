// worker/errors_test.go

// Tests for NonRetryableError: verifies wrapping, unwrapping, errors.As
// detection, and panic on nil. These are pure unit tests with no NATS.
package worker

import (
	"errors"
	"fmt"
	"testing"
	"time"
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

func TestNewRateLimitError_Valid(t *testing.T) {
	inner := fmt.Errorf("rate limited")
	rle := NewRateLimitError(inner, 30*time.Second)

	if rle.Err != inner {
		t.Fatal("Err field should be the original error")
	}
	if rle.RetryAfter != 30*time.Second {
		t.Fatalf("RetryAfter = %v, want 30s", rle.RetryAfter)
	}
	if rle.Error() != "rate limited" {
		t.Fatalf("Error() = %q, want %q", rle.Error(), "rate limited")
	}
}

func TestNewRateLimitError_NilErr_Panics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil err, got nil")
		}
	}()
	NewRateLimitError(nil, 5*time.Second)
}

func TestNewRateLimitError_ZeroDuration_Panics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for zero duration, got nil")
		}
	}()
	NewRateLimitError(fmt.Errorf("oops"), 0)
}

func TestRateLimitError_Unwrap(t *testing.T) {
	inner := fmt.Errorf("inner error")
	rle := NewRateLimitError(inner, 10*time.Second)

	if rle.Unwrap() != inner {
		t.Fatal("Unwrap should return the original error")
	}
	if !errors.Is(rle, inner) {
		t.Fatal("errors.Is should match the wrapped error")
	}
}

func TestRateLimitError_ErrorsAs(t *testing.T) {
	inner := fmt.Errorf("too many requests")
	err := fmt.Errorf("handler: %w", NewRateLimitError(inner, 60*time.Second))

	var rle *RateLimitError
	if !errors.As(err, &rle) {
		t.Fatal("errors.As should find RateLimitError in chain")
	}
	if rle.Err.Error() != "too many requests" {
		t.Fatalf("unwrapped Err = %q, want %q", rle.Err.Error(), "too many requests")
	}
	if rle.RetryAfter != 60*time.Second {
		t.Fatalf("RetryAfter = %v, want 60s", rle.RetryAfter)
	}
}
