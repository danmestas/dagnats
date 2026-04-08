package dagnatstest

import (
	"bytes"
	"context"
	"testing"

	"github.com/danmestas/dagnats/worker"
)

func TestMockTaskContext_SatisfiesInterfaces(t *testing.T) {
	var m MockTaskContext
	// Runtime assertions that the mock satisfies all role interfaces.
	var _ worker.TaskContext = &m
	var _ worker.SimpleTask = &m
	var _ worker.CheckpointTask = &m
	var _ worker.LoopTask = &m
	var _ worker.StreamTask = &m
	var _ worker.SignalTask = &m
}

func TestMockTaskContext_CompleteRecords(t *testing.T) {
	m := &MockTaskContext{}
	out := []byte(`{"ok":true}`)
	if err := m.Complete(out); err != nil {
		t.Fatalf("Complete returned error: %v", err)
	}
	if !m.Completed {
		t.Fatal("expected Completed to be true")
	}
	if !bytes.Equal(m.CompletedOutput, out) {
		t.Fatalf("CompletedOutput = %q, want %q", m.CompletedOutput, out)
	}
}

func TestMockTaskContext_ContinueRecords(t *testing.T) {
	m := &MockTaskContext{}
	out := []byte(`{"next":1}`)
	if err := m.Continue(out); err != nil {
		t.Fatalf("Continue returned error: %v", err)
	}
	if !m.Continued {
		t.Fatal("expected Continued to be true")
	}
	if !bytes.Equal(m.ContinuedOutput, out) {
		t.Fatalf("ContinuedOutput = %q, want %q", m.ContinuedOutput, out)
	}
}

func TestMockTaskContext_CheckpointRoundTrip(t *testing.T) {
	m := &MockTaskContext{}
	state := []byte(`{"step":5}`)
	if err := m.Checkpoint(state); err != nil {
		t.Fatalf("Checkpoint returned error: %v", err)
	}
	got, err := m.LoadCheckpoint()
	if err != nil {
		t.Fatalf("LoadCheckpoint returned error: %v", err)
	}
	if !bytes.Equal(got, state) {
		t.Fatalf("LoadCheckpoint = %q, want %q", got, state)
	}
	if len(m.Checkpoints) != 1 {
		t.Fatalf("len(Checkpoints) = %d, want 1", len(m.Checkpoints))
	}
}

func TestMockTaskContext_HeartbeatCounts(t *testing.T) {
	m := &MockTaskContext{}
	for i := 0; i < 3; i++ {
		if err := m.Heartbeat(); err != nil {
			t.Fatalf("Heartbeat returned error: %v", err)
		}
	}
	if m.HeartbeatCount != 3 {
		t.Fatalf("HeartbeatCount = %d, want 3", m.HeartbeatCount)
	}
}

func TestMockTaskContext_StreamRecords(t *testing.T) {
	m := &MockTaskContext{}
	chunks := [][]byte{[]byte("chunk1"), []byte("chunk2")}
	for _, c := range chunks {
		if err := m.PutStream(c); err != nil {
			t.Fatalf("PutStream returned error: %v", err)
		}
	}
	if len(m.Streams) != 2 {
		t.Fatalf("len(Streams) = %d, want 2", len(m.Streams))
	}
	for i, c := range chunks {
		if !bytes.Equal(m.Streams[i], c) {
			t.Fatalf("Streams[%d] = %q, want %q", i, m.Streams[i], c)
		}
	}
}

func TestMockTaskContext_DefaultContext(t *testing.T) {
	m := &MockTaskContext{}
	ctx := m.Context()
	if ctx == nil {
		t.Fatal("Context() returned nil when CtxValue is unset")
	}
	if ctx != context.Background() {
		t.Fatal("Context() should return context.Background() when CtxValue is unset")
	}

	custom, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.CtxValue = custom
	if m.Context() != custom {
		t.Fatal("Context() should return CtxValue when set")
	}
}
