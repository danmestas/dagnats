package dagnatstest

import (
	"context"
	"sync"
	"time"

	"github.com/danmestas/dagnats/worker"
)

// Verify MockTaskContext satisfies all role interfaces at compile time.
var _ worker.TaskContext = (*MockTaskContext)(nil)
var _ worker.LoopTask = (*MockTaskContext)(nil)
var _ worker.StreamTask = (*MockTaskContext)(nil)
var _ worker.SignalTask = (*MockTaskContext)(nil)

// MockTaskContext is a test double for worker.TaskContext that records
// all method calls. Safe for concurrent use. Satisfies TaskContext
// and all role interfaces (SimpleTask, CheckpointTask, LoopTask,
// StreamTask, SignalTask).
type MockTaskContext struct {
	mu sync.Mutex

	// Configuration -- set before use
	InputData      []byte
	RunIDValue     string
	StepIDValue    string
	RetryCountVal  int
	CtxValue       context.Context
	CheckpointData []byte // returned by LoadCheckpoint
	SignalData     []byte // returned by WaitForSignal
	FailErr        error  // if set, Complete/Fail/Continue return this

	// Recorded calls
	Completed           bool
	CompletedOutput     []byte
	Failed              bool
	FailedErr           error
	FailedPermanent     bool
	Continued           bool
	ContinuedOutput     []byte
	Checkpoints         [][]byte
	Streams             [][]byte
	HeartbeatCount      int
	SignalsSent         []SentSignal
	FailRetryAfterCalls []FailRetryAfterCall
}

// SentSignal records a SendSignal call.
type SentSignal struct {
	RunID string
	Name  string
	Data  []byte
}

// FailRetryAfterCall records a FailRetryAfter call.
type FailRetryAfterCall struct {
	Err   error
	After time.Duration
}

func (m *MockTaskContext) Context() context.Context {
	if m.CtxValue != nil {
		return m.CtxValue
	}
	return context.Background()
}

func (m *MockTaskContext) Input() []byte               { return m.InputData }
func (m *MockTaskContext) RunID() string               { return m.RunIDValue }
func (m *MockTaskContext) StepID() string              { return m.StepIDValue }
func (m *MockTaskContext) RetryCount() int             { return m.RetryCountVal }
func (m *MockTaskContext) Metadata() map[string]string { return nil }

func (m *MockTaskContext) Complete(output []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Completed = true
	m.CompletedOutput = output
	return m.FailErr
}

func (m *MockTaskContext) Fail(err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Failed = true
	m.FailedErr = err
	return m.FailErr
}

func (m *MockTaskContext) FailPermanent(err error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FailedPermanent = true
	m.FailedErr = err
	return m.FailErr
}

func (m *MockTaskContext) FailRetryAfter(err error, after time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.FailRetryAfterCalls = append(m.FailRetryAfterCalls, FailRetryAfterCall{Err: err, After: after})
	return m.FailErr
}

func (m *MockTaskContext) Continue(output []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Continued = true
	m.ContinuedOutput = output
	return m.FailErr
}

func (m *MockTaskContext) Checkpoint(state []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Checkpoints = append(m.Checkpoints, state)
	m.CheckpointData = state // latest checkpoint is loadable
	return m.FailErr
}

func (m *MockTaskContext) LoadCheckpoint() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.CheckpointData, nil
}

func (m *MockTaskContext) PutStream(data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Streams = append(m.Streams, data)
	return m.FailErr
}

func (m *MockTaskContext) Heartbeat() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.HeartbeatCount++
	return m.FailErr
}

func (m *MockTaskContext) Pause(name string, duration time.Duration) error {
	return nil
}

func (m *MockTaskContext) WaitForSignal(name string, timeout time.Duration) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.SignalData, nil
}

func (m *MockTaskContext) SendSignal(runID, name string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SignalsSent = append(m.SignalsSent, SentSignal{RunID: runID, Name: name, Data: data})
	return m.FailErr
}
