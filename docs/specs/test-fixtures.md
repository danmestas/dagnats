# Workflow Test Fixtures

**Status:** Design
**Date:** 2026-04-06
**Depends on:** Nothing (additive to dagnatstest package)

## Problem

Every test that needs a workflow definition builds it from scratch:

```go
wb := dag.NewWorkflow(harness.UniqueName(t, "linear"))
a := wb.Task("a", "task-a")
wb.Task("b", "task-b").After(a)
def, err := wb.Build()
if err != nil {
    t.Fatalf("Build: %v", err)
}
```

Common patterns (linear chain, fan-out, fan-in, diamond) are rebuilt in
every test file. This adds noise and makes tests harder to scan.

## Design

### 1. New File: `dagnatstest/fixtures.go`

Pre-built workflow builders for common DAG shapes. Each returns a
`dag.WorkflowDef` (already built and validated) and a map of task names
so the caller can register handlers.

```go
// LinearDef returns a workflow with n steps in sequence:
// step-0 → step-1 → ... → step-(n-1).
// Task names are "task-0", "task-1", etc.
// Panics if n < 1 or n > 100.
func LinearDef(t *testing.T, n int) dag.WorkflowDef

// FanOutDef returns a workflow with 1 root step fanning out to n
// parallel steps: root → {branch-0, branch-1, ..., branch-(n-1)}.
// Task names: "task-root", "task-branch-0", etc.
// Panics if n < 1 or n > 100.
func FanOutDef(t *testing.T, n int) dag.WorkflowDef

// FanInDef returns a fan-out followed by a single join step:
// root → {branch-0..n-1} → join.
// Task names: "task-root", "task-branch-0", ..., "task-join".
// Panics if n < 1 or n > 100.
func FanInDef(t *testing.T, n int) dag.WorkflowDef

// DiamondDef returns the classic diamond: A → {B, C} → D.
// Task names: "task-a", "task-b", "task-c", "task-d".
func DiamondDef(t *testing.T) dag.WorkflowDef
```

Each function:
- Generates a unique workflow name via `t.Name()` + counter to avoid
  KV collisions
- Calls `wb.Build()` internally and fatals on error
- Returns only the `WorkflowDef` — callers register their own handlers

### 2. Passthrough Handler

Most fixture-based tests just need steps that pass input to output:

```go
// PassHandler returns a HandlerFunc that passes input through as
// output unchanged. Useful for testing DAG structure without
// business logic.
func PassHandler() worker.HandlerFunc {
    return func(ctx worker.TaskContext) error {
        return ctx.Complete(ctx.Input())
    }
}

// FailHandler returns a HandlerFunc that always fails permanently.
func FailHandler(msg string) worker.HandlerFunc {
    return func(ctx worker.TaskContext) error {
        return ctx.FailPermanent(fmt.Errorf("%s", msg))
    }
}
```

### 3. Usage Example

Before:

```go
func TestDiamondCompletion(t *testing.T) {
    h := dagnatstest.Harness(t)
    name := "diamond-" + t.Name()
    wb := dag.NewWorkflow(name)
    a := wb.Task("a", "task-a")
    b := wb.Task("b", "task-b").After(a)
    c := wb.Task("c", "task-c").After(a)
    wb.Task("d", "task-d").After(b, c)
    def, err := wb.Build()
    if err != nil {
        t.Fatalf("Build: %v", err)
    }
    for _, task := range []string{"task-a", "task-b", "task-c", "task-d"} {
        h.Handle(t, task, func(ctx worker.TaskContext) error {
            return ctx.Complete(ctx.Input())
        })
    }
    h.Start(t)
    // ...register and run...
}
```

After:

```go
func TestDiamondCompletion(t *testing.T) {
    h := dagnatstest.Harness(t)
    def := dagnatstest.DiamondDef(t)
    for _, task := range []string{"task-a", "task-b", "task-c", "task-d"} {
        h.Handle(t, task, dagnatstest.PassHandler())
    }
    h.Start(t)
    run := h.RegisterAndRun(t, def, []byte(`"input"`), 10*time.Second)
    // ...assertions...
}
```

### 4. Files Changed

| File | Change |
|------|--------|
| `dagnatstest/fixtures.go` (new) | LinearDef, FanOutDef, FanInDef, DiamondDef, PassHandler, FailHandler |
