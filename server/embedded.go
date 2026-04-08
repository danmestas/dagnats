package server

import (
	"github.com/danmestas/dagnats/worker"
)

const maxEmbeddedWorkers = 50

// roleType distinguishes how a handler is registered with the worker.
type roleType int

const (
	roleDefault   roleType = iota
	roleLoop
	roleStream
	roleSignal
	roleSingleton
)

// registration pairs a task type with its handler function and role.
type registration struct {
	taskType string
	handler  worker.HandlerFunc
	role     roleType
}

// WorkerShim collects handler registrations before the server
// starts. Returned by EmbeddedWorker(). The shim is materialized
// to a real *worker.Worker during startComponents().
type WorkerShim struct {
	registrations []registration
	groups        []string
	started       bool
}

// Handle registers a handler for a task type. Panics if called
// after Run(), if taskType is empty, or if handler is nil.
func (s *WorkerShim) Handle(
	taskType string, handler worker.HandlerFunc,
) {
	if s == nil {
		panic("WorkerShim.Handle: s is nil")
	}
	if s.started {
		panic("WorkerShim.Handle: called after Run()")
	}
	if taskType == "" {
		panic("WorkerShim.Handle: taskType is empty")
	}
	if handler == nil {
		panic("WorkerShim.Handle: handler is nil")
	}
	s.registrations = append(s.registrations, registration{
		taskType: taskType,
		handler:  handler,
	})
}

// HandleLoop registers an agent-loop handler. During materialization,
// the handler is wrapped as HandlerFunc and dispatched via w.Handle.
func (s *WorkerShim) HandleLoop(
	taskType string, fn func(worker.LoopTask) error,
) {
	if s == nil {
		panic("WorkerShim.HandleLoop: s is nil")
	}
	if s.started {
		panic("WorkerShim.HandleLoop: called after Run()")
	}
	if taskType == "" {
		panic("WorkerShim.HandleLoop: taskType is empty")
	}
	if fn == nil {
		panic("WorkerShim.HandleLoop: fn is nil")
	}
	s.registrations = append(s.registrations, registration{
		taskType: taskType,
		handler:  func(ctx worker.TaskContext) error { return fn(ctx) },
		role:     roleLoop,
	})
}

// HandleStream registers a streaming-output handler. During
// materialization, the handler is wrapped as HandlerFunc and
// dispatched via w.Handle.
func (s *WorkerShim) HandleStream(
	taskType string, fn func(worker.StreamTask) error,
) {
	if s == nil {
		panic("WorkerShim.HandleStream: s is nil")
	}
	if s.started {
		panic("WorkerShim.HandleStream: called after Run()")
	}
	if taskType == "" {
		panic("WorkerShim.HandleStream: taskType is empty")
	}
	if fn == nil {
		panic("WorkerShim.HandleStream: fn is nil")
	}
	s.registrations = append(s.registrations, registration{
		taskType: taskType,
		handler:  func(ctx worker.TaskContext) error { return fn(ctx) },
		role:     roleStream,
	})
}

// HandleSignal registers an inter-step signal handler. During
// materialization, the handler is wrapped as HandlerFunc and
// dispatched via w.Handle.
func (s *WorkerShim) HandleSignal(
	taskType string, fn func(worker.SignalTask) error,
) {
	if s == nil {
		panic("WorkerShim.HandleSignal: s is nil")
	}
	if s.started {
		panic("WorkerShim.HandleSignal: called after Run()")
	}
	if taskType == "" {
		panic("WorkerShim.HandleSignal: taskType is empty")
	}
	if fn == nil {
		panic("WorkerShim.HandleSignal: fn is nil")
	}
	s.registrations = append(s.registrations, registration{
		taskType: taskType,
		handler:  func(ctx worker.TaskContext) error { return fn(ctx) },
		role:     roleSignal,
	})
}

// HandleSingleton registers a handler that runs as a single-
// partition consumer. During materialization, translated to
// worker.HandleSingleton.
func (s *WorkerShim) HandleSingleton(
	taskType string, handler worker.HandlerFunc,
) {
	if s == nil {
		panic("WorkerShim.HandleSingleton: s is nil")
	}
	if s.started {
		panic("WorkerShim.HandleSingleton: called after Run()")
	}
	if taskType == "" {
		panic("WorkerShim.HandleSingleton: taskType is empty")
	}
	if handler == nil {
		panic("WorkerShim.HandleSingleton: handler is nil")
	}
	s.registrations = append(s.registrations, registration{
		taskType: taskType,
		handler:  handler,
		role:     roleSingleton,
	})
}

// WithGroups configures this embedded worker for specific worker
// groups. During materialization, translated to
// worker.WithGroups(groups...). Panics after Run().
func (s *WorkerShim) WithGroups(groups ...string) {
	if s == nil {
		panic("WorkerShim.WithGroups: s is nil")
	}
	if s.started {
		panic("WorkerShim.WithGroups: called after Run()")
	}
	if len(groups) == 0 {
		panic("WorkerShim.WithGroups: groups is empty")
	}
	for _, g := range groups {
		if g == "" {
			panic(
				"WorkerShim.WithGroups: group name is empty",
			)
		}
	}
	s.groups = groups
}

// EmbeddedWorker creates a WorkerShim bound to srv's lifecycle.
// Must be called before Run(). Panics if called after Run(), if
// srv is nil, or if the max embedded worker limit is exceeded.
func EmbeddedWorker(srv *Server) *WorkerShim {
	if srv == nil {
		panic("EmbeddedWorker: srv is nil")
	}
	if srv.running.Load() {
		panic("EmbeddedWorker: called after Run()")
	}
	if len(srv.workerShims) >= maxEmbeddedWorkers {
		panic("EmbeddedWorker: max embedded workers exceeded")
	}
	shim := &WorkerShim{}
	srv.workerShims = append(srv.workerShims, shim)
	return shim
}
