# Test Harness Helper

**Status:** Design
**Date:** 2026-04-06
**Depends on:** Nothing (additive to existing dagnatstest package)

## Problem

Every integration test outside of `e2e/` repeats ~15 lines of boilerplate
to wire up the engine, service, and worker:

```go
nc := dagnatstest.Server(t)
orch := engine.NewOrchestrator(nc)
orch.Start()
t.Cleanup(func() { orch.Stop() })
svc := api.NewService(nc)
w := worker.NewWorker(nc)
// ... register handlers ...
w.Start()
t.Cleanup(func() { w.Stop() })
```

The `e2e/harness` package has helpers (`SubscribeWorker`,
`RegisterAndStart`, `WaitForRunStatus`) but they live in `e2e/harness`
which imports the topology system — overkill for simple integration tests.

## Design

### 1. New Function: `dagnatstest.Harness`

Add to `dagnatstest/dagnatstest.go`:

```go
// Harness holds all components needed for a workflow integration test.
type Harness struct {
    NC     *nats.Conn
    Engine *engine.Orchestrator
    Svc    *api.Service
    Worker *worker.Worker
}

// Harness starts an embedded NATS server, orchestrator, API service,
// and worker. All components are cleaned up when the test ends.
// Register handlers on h.Worker before calling h.Worker.Start() —
// or use h.Handle/h.HandleTyped for auto-start convenience.
func Harness(t *testing.T) *Harness {
    t.Helper()
    nc := Server(t)

    orch := engine.NewOrchestrator(nc)
    orch.Start()
    t.Cleanup(func() { orch.Stop() })

    svc := api.NewService(nc)
    w := worker.NewWorker(nc)

    return &Harness{
        NC:     nc,
        Engine: orch,
        Svc:    svc,
        Worker: w,
    }
}
```

### 2. Convenience Methods on Harness

```go
// Handle registers a raw handler and ensures the worker is started.
func (h *Harness) Handle(
    t *testing.T, taskType string, fn worker.HandlerFunc,
) {
    t.Helper()
    h.Worker.Handle(taskType, fn)
}

// HandleTyped registers a typed handler.
func HandleTypedOn[I, O any](
    h *Harness, t *testing.T,
    taskType string, fn worker.TypedHandlerFunc[I, O],
) {
    t.Helper()
    worker.HandleTyped(h.Worker, taskType, fn)
}

// Start starts the worker. Call after registering all handlers.
func (h *Harness) Start(t *testing.T) {
    t.Helper()
    h.Worker.Start()
    t.Cleanup(func() { h.Worker.Stop() })
}

// RunAndWait registers a workflow, starts a run, and waits for
// terminal status. Convenience wrapper over existing helpers.
func (h *Harness) RunAndWait(
    t *testing.T, def dag.WorkflowDef,
    input []byte, timeout time.Duration,
) dag.WorkflowRun {
    t.Helper()
    return RunAndWait(t, h.Svc, def.Name, input, timeout)
}
```

Wait — `RunAndWait` in helpers.go takes workflow name string, not def.
We need to register first:

```go
// RegisterAndRun registers a workflow, starts a run, and blocks
// until it reaches a terminal status. Returns the final snapshot.
func (h *Harness) RegisterAndRun(
    t *testing.T, def dag.WorkflowDef,
    input []byte, timeout time.Duration,
) dag.WorkflowRun {
    t.Helper()
    ctx := context.Background()
    if err := h.Svc.RegisterWorkflow(ctx, def); err != nil {
        t.Fatalf("RegisterWorkflow %q: %v", def.Name, err)
    }
    return RunAndWait(t, h.Svc, def.Name, input, timeout)
}
```

### 3. Usage Example

Before:

```go
func TestMyWorkflow(t *testing.T) {
    nc := dagnatstest.Server(t)
    orch := engine.NewOrchestrator(nc)
    orch.Start()
    t.Cleanup(func() { orch.Stop() })
    svc := api.NewService(nc)
    w := worker.NewWorker(nc)
    worker.HandleTyped(w, "greet", func(ctx worker.TaskContext, name string) (string, error) {
        return "Hello, " + name, nil
    })
    w.Start()
    t.Cleanup(func() { w.Stop() })
    // ... register workflow, start run, poll status ...
}
```

After:

```go
func TestMyWorkflow(t *testing.T) {
    h := dagnatstest.Harness(t)
    dagnatstest.HandleTypedOn(h, t, "greet",
        func(ctx worker.TaskContext, name string) (string, error) {
            return "Hello, " + name, nil
        },
    )
    h.Start(t)

    wb := dag.NewWorkflow("test-greet")
    wb.Task("greet", "greet")
    def, err := wb.Build()
    if err != nil {
        t.Fatalf("Build: %v", err)
    }
    run := h.RegisterAndRun(t, def, []byte(`"World"`), 10*time.Second)
    if run.Status != dag.RunStatusCompleted {
        t.Fatalf("expected completed, got %s", run.Status)
    }
}
```

### 4. Files Changed

| File | Change |
|------|--------|
| `dagnatstest/dagnatstest.go` | Add `Harness` struct and constructor |
| `dagnatstest/harness.go` (new) | Convenience methods on `Harness` |

### 5. Notes

- Does NOT replace the `e2e/harness` package — that handles topologies.
- `dagnatstest.Server(t)` remains available for tests that only need a
  NATS connection.
- The `Harness` struct fields are public so tests can reach into them
  for assertions (e.g., `h.Svc.GetRun()`).
- `h.Start(t)` must be called explicitly after handler registration,
  not in the constructor, because handlers must be registered before
  subscription creation.
