// Methodology: Pure unit tests for WorkerShim. No NATS required.
// Tests verify registration recording, panic guards, and bounds.
package server

import (
	"fmt"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/worker"
)

func TestWorkerShim_Handle_RecordsRegistration(t *testing.T) {
	shim := &WorkerShim{}
	handler := func(ctx worker.TaskContext) error {
		return nil
	}

	shim.Handle("test-task", handler)

	// Positive: registration recorded
	if len(shim.registrations) != 1 {
		t.Fatalf("registrations = %d, want 1", len(shim.registrations))
	}
	if shim.registrations[0].taskType != "test-task" {
		t.Errorf("taskType = %q, want %q",
			shim.registrations[0].taskType, "test-task")
	}

	// Negative: handler must not be nil
	if shim.registrations[0].handler == nil {
		t.Fatal("handler is nil")
	}
}

func TestWorkerShim_Handle_PanicsOnEmptyTaskType(t *testing.T) {
	shim := &WorkerShim{}
	defer func() {
		r := recover()
		// Positive: panic occurred
		if r == nil {
			t.Fatal("expected panic on empty taskType")
		}
		// Positive: message identifies the cause
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "taskType") {
			t.Errorf("panic = %q, want 'taskType'", msg)
		}
	}()
	shim.Handle("", func(ctx worker.TaskContext) error {
		return nil
	})
}

func TestWorkerShim_Handle_PanicsOnNilHandler(t *testing.T) {
	shim := &WorkerShim{}
	defer func() {
		r := recover()
		// Positive: panic occurred
		if r == nil {
			t.Fatal("expected panic on nil handler")
		}
		// Positive: message identifies the cause
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "handler") {
			t.Errorf("panic = %q, want 'handler'", msg)
		}
	}()
	shim.Handle("test-task", nil)
}

func TestWorkerShim_Handle_PanicsAfterStarted(t *testing.T) {
	shim := &WorkerShim{started: true}
	defer func() {
		r := recover()
		// Positive: panic occurred
		if r == nil {
			t.Fatal("expected panic after started")
		}
		// Positive: message identifies the cause
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "after Run") {
			t.Errorf("panic = %q, want 'after Run'", msg)
		}
	}()
	shim.Handle("test-task", func(ctx worker.TaskContext) error {
		return nil
	})
}

func TestWorkerShim_WithGroups_Records(t *testing.T) {
	shim := &WorkerShim{}
	shim.WithGroups("gpu", "cpu")

	// Positive: groups recorded
	if len(shim.groups) != 2 {
		t.Fatalf("groups = %d, want 2", len(shim.groups))
	}
	if shim.groups[0] != "gpu" {
		t.Errorf("groups[0] = %q, want %q", shim.groups[0], "gpu")
	}

	// Negative: registrations should be empty
	if len(shim.registrations) != 0 {
		t.Errorf("registrations = %d, want 0",
			len(shim.registrations))
	}
}

func TestWorkerShim_WithGroups_PanicsOnEmpty(t *testing.T) {
	shim := &WorkerShim{}
	defer func() {
		r := recover()
		// Positive: panic occurred
		if r == nil {
			t.Fatal("expected panic on empty groups")
		}
		// Positive: message identifies the cause
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "empty") {
			t.Errorf("panic = %q, want 'empty'", msg)
		}
	}()
	shim.WithGroups()
}

func TestEmbeddedWorker_PanicsOnNilServer(t *testing.T) {
	defer func() {
		r := recover()
		// Positive: panic occurred
		if r == nil {
			t.Fatal("expected panic on nil server")
		}
		// Positive: message identifies the cause
		msg := fmt.Sprintf("%v", r)
		if !strings.Contains(msg, "srv is nil") {
			t.Errorf("panic = %q, want 'srv is nil'", msg)
		}
	}()
	EmbeddedWorker(nil)
}

func TestEmbeddedWorker_TracksShimOnServer(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	srv := New(cfg)

	shim := EmbeddedWorker(srv)

	// Positive: shim is tracked
	if len(srv.workerShims) != 1 {
		t.Fatalf("workerShims = %d, want 1",
			len(srv.workerShims))
	}
	if srv.workerShims[0] != shim {
		t.Error("tracked shim does not match returned shim")
	}

	// Negative: workers not yet created
	if len(srv.workers) != 0 {
		t.Errorf("workers = %d, want 0 before Run()",
			len(srv.workers))
	}
}

func TestEmbeddedWorker_MultipleShims(t *testing.T) {
	cfg := DefaultConfig()
	cfg.DataDir = t.TempDir()
	srv := New(cfg)

	EmbeddedWorker(srv)
	EmbeddedWorker(srv)

	if len(srv.workerShims) != 2 {
		t.Fatalf("workerShims = %d, want 2",
			len(srv.workerShims))
	}
}
