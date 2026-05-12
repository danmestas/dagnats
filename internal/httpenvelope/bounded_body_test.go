// httpenvelope/bounded_body_test.go
//
// Methodology: pure unit tests, no NATS. Each test covers one bound:
// below-limit, at-limit, over-limit, empty, programmer-error. Two
// assertions per test, positive + negative space.
package httpenvelope

import (
	"bytes"
	"errors"
	"strings"
	"testing"
)

func TestBoundedBodyReadsWithinLimit(t *testing.T) {
	const max = int64(1024)
	body := bytes.NewReader([]byte("hello"))

	got, err := BoundedBody(body, max)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("got %q, want %q", got, "hello")
	}
}

func TestBoundedBodyAtExactLimit(t *testing.T) {
	const max = int64(5)
	body := bytes.NewReader([]byte("hello"))

	got, err := BoundedBody(body, max)
	if err != nil {
		t.Fatalf("at-limit should not error: %v", err)
	}
	if len(got) != 5 {
		t.Fatalf("len = %d, want 5", len(got))
	}
}

func TestBoundedBodyOverLimitReturnsErrBodyTooLarge(t *testing.T) {
	const max = int64(4)
	body := bytes.NewReader([]byte("hello"))

	got, err := BoundedBody(body, max)
	if err == nil {
		t.Fatal("expected ErrBodyTooLarge, got nil")
	}
	if !errors.Is(err, ErrBodyTooLarge) {
		t.Fatalf("err = %v, want ErrBodyTooLarge", err)
	}
	if got != nil {
		t.Fatalf("got %v, want nil on overflow", got)
	}
}

func TestBoundedBodyEmpty(t *testing.T) {
	got, err := BoundedBody(bytes.NewReader(nil), 1024)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("len = %d, want 0", len(got))
	}
}

func TestBoundedBodyZeroMaxPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for max == 0")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("recover() = %T, want string", r)
		}
		if !strings.Contains(msg, "max") {
			t.Fatalf("panic message %q must mention max", msg)
		}
	}()
	_, _ = BoundedBody(bytes.NewReader([]byte("x")), 0)
}

func TestBoundedBodyNegativeMaxPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for max < 0")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("recover() = %T, want string", r)
		}
		if !strings.Contains(msg, "max") {
			t.Fatalf("panic message %q must mention max", msg)
		}
	}()
	_, _ = BoundedBody(bytes.NewReader([]byte("x")), -1)
}

func TestBoundedBodyNilReaderPanics(t *testing.T) {
	defer func() {
		r := recover()
		if r == nil {
			t.Fatal("expected panic for nil reader")
		}
		msg, ok := r.(string)
		if !ok {
			t.Fatalf("recover() = %T, want string", r)
		}
		if !strings.Contains(msg, "reader") {
			t.Fatalf("panic message %q must mention reader", msg)
		}
	}()
	_, _ = BoundedBody(nil, 1024)
}
