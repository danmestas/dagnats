// worker/typed_test.go
// Tests for Typed handler wrapper: JSON unmarshal input, invoke typed
// function, JSON marshal output, and Complete call. Pure unit tests
// using a mock TaskContext -- no NATS required.
package worker

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
)

// mockTaskContext implements TaskContext for pure unit tests.
type mockTaskContext struct {
	input     []byte
	completed []byte
	failErr   error
}

func (m *mockTaskContext) Input() []byte           { return m.input }
func (m *mockTaskContext) RunID() string           { return "mock-run" }
func (m *mockTaskContext) StepID() string          { return "mock-step" }
func (m *mockTaskContext) RetryCount() int         { return 0 }
func (m *mockTaskContext) Heartbeat() error        { return nil }
func (m *mockTaskContext) PutStream([]byte) error  { return nil }
func (m *mockTaskContext) Checkpoint([]byte) error { return nil }
func (m *mockTaskContext) Continue([]byte) error   { return nil }

func (m *mockTaskContext) LoadCheckpoint() ([]byte, error) {
	return nil, nil
}

func (m *mockTaskContext) Pause(
	_ string, _ time.Duration,
) error {
	return nil
}

func (m *mockTaskContext) WaitForSignal(
	_ string, _ time.Duration,
) ([]byte, error) {
	return nil, nil
}

func (m *mockTaskContext) SendSignal(
	_, _ string, _ []byte,
) error {
	return nil
}

func (m *mockTaskContext) Complete(output []byte) error {
	m.completed = output
	return nil
}

func (m *mockTaskContext) Fail(err error) error {
	m.failErr = err
	return nil
}

func (m *mockTaskContext) FailPermanent(err error) error {
	m.failErr = err
	return nil
}

func (m *mockTaskContext) FailRetryAfter(
	err error, _ time.Duration,
) error {
	m.failErr = err
	return nil
}

type addInput struct {
	A int `json:"a"`
	B int `json:"b"`
}

type addOutput struct {
	Sum int `json:"sum"`
}

func TestTypedHandlerSuccess(t *testing.T) {
	handler := Typed(
		func(
			ctx TaskContext, in addInput,
		) (addOutput, error) {
			return addOutput{Sum: in.A + in.B}, nil
		},
	)
	mock := &mockTaskContext{
		input: []byte(`{"a":3,"b":4}`),
	}
	err := handler(mock)
	// Positive: handler returns no error
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	var out addOutput
	if err := json.Unmarshal(mock.completed, &out); err != nil {
		t.Fatalf("unmarshal completed: %v", err)
	}
	// Positive: output contains correct sum
	if out.Sum != 7 {
		t.Fatalf("Sum = %d, want 7", out.Sum)
	}
}

func TestTypedHandlerBadInput(t *testing.T) {
	handler := Typed(
		func(
			ctx TaskContext, in addInput,
		) (addOutput, error) {
			t.Fatal("should not be called with bad input")
			return addOutput{}, nil
		},
	)
	mock := &mockTaskContext{
		input: []byte(`not json`),
	}
	err := handler(mock)
	// Positive: returns an error for bad JSON
	if err == nil {
		t.Fatal("expected error for invalid JSON input")
	}
	// Negative: error is NonRetryableError (no point retrying)
	var nre *NonRetryableError
	if !isNonRetryable(err, &nre) {
		t.Fatal("expected NonRetryableError for bad input")
	}
}

func TestTypedHandlerNilInput(t *testing.T) {
	handler := Typed(
		func(
			ctx TaskContext, in addInput,
		) (addOutput, error) {
			return addOutput{Sum: 0}, nil
		},
	)
	mock := &mockTaskContext{input: nil}
	err := handler(mock)
	// Positive: nil input does not error
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	// Negative: completed output is valid JSON
	if mock.completed == nil {
		t.Fatal("expected non-nil completed output")
	}
}

func TestTypedHandlerFnError(t *testing.T) {
	handler := Typed(
		func(
			ctx TaskContext, in addInput,
		) (addOutput, error) {
			return addOutput{}, fmt.Errorf("boom")
		},
	)
	mock := &mockTaskContext{
		input: []byte(`{"a":1,"b":2}`),
	}
	err := handler(mock)
	// Positive: handler error is propagated
	if err == nil {
		t.Fatal("expected error from handler fn")
	}
	// Negative: Complete was not called
	if mock.completed != nil {
		t.Fatal("Complete should not be called on error")
	}
}

func TestTypedPanicsOnNilFn(t *testing.T) {
	defer func() {
		r := recover()
		// Positive: panics on nil fn
		if r == nil {
			t.Fatal("expected panic for nil fn")
		}
		msg := fmt.Sprintf("%v", r)
		// Negative: message is specific
		if msg != "Typed: fn must not be nil" {
			t.Fatalf("panic = %q, want fn message", msg)
		}
	}()
	Typed[addInput, addOutput](nil)
}

func TestHandleTypedRegistersHandler(t *testing.T) {
	mock := &mockTaskContext{
		input: []byte(`{"a":10,"b":20}`),
	}
	// Use HandleTyped on a real Worker to verify it wires
	// through to the handlers map correctly.
	_, nc := natsutil.StartTestServer(t)
	w := NewWorker(nc, nil)
	HandleTyped(w, "adder",
		func(
			ctx TaskContext, in addInput,
		) (addOutput, error) {
			return addOutput{Sum: in.A + in.B}, nil
		},
	)
	// Positive: handler registered in map
	handler, ok := w.handlers["adder"]
	if !ok {
		t.Fatal("handler not found after HandleTyped")
	}
	// Positive: handler works with typed JSON
	err := handler(mock)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	var out addOutput
	if err := json.Unmarshal(mock.completed, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Sum != 30 {
		t.Fatalf("Sum = %d, want 30", out.Sum)
	}
}

func TestHandleTypedPanicsOnNilFn(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	w := NewWorker(nc, nil)
	defer func() {
		r := recover()
		// Positive: panics on nil fn
		if r == nil {
			t.Fatal("expected panic for nil fn")
		}
	}()
	HandleTyped(w, "bad", (TypedHandlerFunc[addInput, addOutput])(nil))
}

func TestHandleTypedPanicsOnEmptyTaskType(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	w := NewWorker(nc, nil)
	defer func() {
		r := recover()
		// Positive: panics on empty taskType
		if r == nil {
			t.Fatal("expected panic for empty taskType")
		}
	}()
	HandleTyped(w, "",
		func(
			ctx TaskContext, in addInput,
		) (addOutput, error) {
			return addOutput{}, nil
		},
	)
}

// isNonRetryable is a helper using errors.As for NonRetryableError.
func isNonRetryable(
	err error, target **NonRetryableError,
) bool {
	for err != nil {
		if nre, ok := err.(*NonRetryableError); ok {
			*target = nre
			return true
		}
		unwrapper, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = unwrapper.Unwrap()
	}
	return false
}
