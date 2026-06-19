// Methodology: Unit tests for built-in handler factories.
// Uses real os/exec commands (echo, false, cat, env) and a
// local HTTP test server. No NATS required.
package cli

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/server"
	"github.com/danmestas/dagnats/worker"
)

// fakeTaskContext is a minimal TaskContext for testing
// built-in handlers. Only Complete and Fail are used.
type fakeTaskContext struct {
	input      []byte
	output     []byte
	completed  bool
	failed     bool
	failErr    error
	runID      string
	stepID     string
	retryCount int
}

func (f *fakeTaskContext) Context() context.Context { return context.Background() }
func (f *fakeTaskContext) Input() []byte            { return f.input }
func (f *fakeTaskContext) RunID() string            { return f.runID }
func (f *fakeTaskContext) StepID() string           { return f.stepID }
func (f *fakeTaskContext) RetryCount() int {
	return f.retryCount
}

func (f *fakeTaskContext) Metadata() map[string]string { return nil }

func (f *fakeTaskContext) Complete(
	output []byte,
) error {
	f.completed = true
	f.output = output
	return nil
}

func (f *fakeTaskContext) Fail(err error) error {
	f.failed = true
	f.failErr = err
	return nil
}

func (f *fakeTaskContext) FailPermanent(err error) error {
	f.failed = true
	f.failErr = err
	return nil
}

func (f *fakeTaskContext) FailRetryAfter(
	err error, _ time.Duration,
) error {
	f.failed = true
	f.failErr = err
	return nil
}

func (f *fakeTaskContext) Continue(
	_ []byte,
) error {
	return fmt.Errorf("Continue not expected")
}

func (f *fakeTaskContext) PutStream(_ []byte) error {
	return nil
}

func (f *fakeTaskContext) Heartbeat() error { return nil }

func (f *fakeTaskContext) Checkpoint(
	_ []byte,
) error {
	return nil
}

func (f *fakeTaskContext) LoadCheckpoint() (
	[]byte, error,
) {
	return nil, nil
}

func (f *fakeTaskContext) Pause(
	_ string, _ time.Duration,
) error {
	return fmt.Errorf("Pause not expected")
}

func (f *fakeTaskContext) WaitForSignal(
	_ string, _ time.Duration,
) ([]byte, error) {
	return nil, fmt.Errorf("not implemented")
}

func (f *fakeTaskContext) SendSignal(
	_, _ string, _ []byte,
) error {
	return nil
}

// Verify fakeTaskContext implements all worker interfaces.
var _ worker.TaskContext = (*fakeTaskContext)(nil)

// --- Exec handler tests ---

func TestBuildHandler_Exec_Success(t *testing.T) {
	cfg := server.WorkerConfig{
		Task: "test",
		Exec: "echo hello",
	}

	handler := buildHandler(cfg)
	if handler == nil {
		t.Fatal("buildHandler returned nil")
	}

	tc := &fakeTaskContext{input: []byte("ignored")}
	err := handler(tc)

	// Positive: no error
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	// Positive: Complete called with stdout
	if !tc.completed {
		t.Fatal("Complete was not called")
	}
	output := strings.TrimSpace(string(tc.output))
	if output != "hello" {
		t.Errorf("output = %q, want %q",
			output, "hello")
	}
	// Negative: Fail was not called
	if tc.failed {
		t.Error("Fail was called unexpectedly")
	}
}

func TestBuildHandler_Exec_NonZeroExit(t *testing.T) {
	cfg := server.WorkerConfig{
		Task: "test",
		Exec: "false",
	}

	handler := buildHandler(cfg)
	tc := &fakeTaskContext{input: []byte("")}
	err := handler(tc)

	// Positive: handler returns nil (calls Fail internally)
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	// Positive: Fail was called
	if !tc.failed {
		t.Fatal("Fail was not called")
	}
	// Negative: Complete was not called
	if tc.completed {
		t.Error("Complete was called unexpectedly")
	}
}

func TestBuildHandler_Exec_StdinReceivesInput(t *testing.T) {
	cfg := server.WorkerConfig{
		Task: "test",
		Exec: "cat",
	}

	handler := buildHandler(cfg)
	tc := &fakeTaskContext{input: []byte("input data")}
	err := handler(tc)

	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	// Positive: output matches input
	if !tc.completed {
		t.Fatal("Complete was not called")
	}
	if string(tc.output) != "input data" {
		t.Errorf("output = %q, want %q",
			string(tc.output), "input data")
	}
}

func TestBuildHandler_Exec_SetsEnvVars(t *testing.T) {
	cfg := server.WorkerConfig{
		Task: "test",
		Exec: "env",
	}

	handler := buildHandler(cfg)
	tc := &fakeTaskContext{
		input:      []byte(""),
		runID:      "run-123",
		stepID:     "step-abc",
		retryCount: 2,
	}
	err := handler(tc)

	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	output := string(tc.output)
	// Positive: env vars present
	if !strings.Contains(
		output, "DAGNATS_RUN_ID=run-123",
	) {
		t.Error("missing DAGNATS_RUN_ID in env")
	}
	if !strings.Contains(
		output, "DAGNATS_STEP_ID=step-abc",
	) {
		t.Error("missing DAGNATS_STEP_ID in env")
	}
	// Negative: retry count also present
	if !strings.Contains(
		output, "DAGNATS_RETRY_COUNT=2",
	) {
		t.Error("missing DAGNATS_RETRY_COUNT in env")
	}
}

// --- HTTP handler tests ---

func TestBuildHandler_HTTP_Success(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			if string(body) != "test input" {
				t.Errorf("body = %q, want %q",
					string(body), "test input")
			}
			if r.Method != "POST" {
				t.Errorf("method = %q, want POST",
					r.Method)
			}
			w.WriteHeader(200)
			w.Write([]byte("response"))
		},
	))
	defer ts.Close()

	cfg := server.WorkerConfig{
		Task: "test",
		HTTP: ts.URL,
	}
	handler := buildHandler(cfg)
	tc := &fakeTaskContext{input: []byte("test input")}
	err := handler(tc)

	// Positive: no error, Complete called
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !tc.completed {
		t.Fatal("Complete was not called")
	}
	if string(tc.output) != "response" {
		t.Errorf("output = %q, want %q",
			string(tc.output), "response")
	}
}

func TestBuildHandler_HTTP_NonSuccess(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(500)
			w.Write([]byte("internal error"))
		},
	))
	defer ts.Close()

	cfg := server.WorkerConfig{
		Task: "test",
		HTTP: ts.URL,
	}
	handler := buildHandler(cfg)
	tc := &fakeTaskContext{input: []byte("")}
	err := handler(tc)

	// Positive: Fail was called for 500
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !tc.failed {
		t.Fatal("Fail was not called for 500")
	}
	// Negative: Complete should not be called
	if tc.completed {
		t.Error("Complete called on 500")
	}
}

func TestBuildHandler_HTTP_CustomMethod(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.Method != "PUT" {
				t.Errorf("method = %q, want PUT",
					r.Method)
			}
			w.WriteHeader(200)
			w.Write([]byte("ok"))
		},
	))
	defer ts.Close()

	cfg := server.WorkerConfig{
		Task:       "test",
		HTTP:       ts.URL,
		HTTPMethod: "PUT",
	}
	handler := buildHandler(cfg)
	tc := &fakeTaskContext{input: []byte("")}
	err := handler(tc)

	// Positive: no error, Complete called
	if err != nil {
		t.Fatalf("handler error: %v", err)
	}
	if !tc.completed {
		t.Fatal("Complete was not called")
	}
}
