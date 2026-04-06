// dagnatstest/fixtures.go
// Pre-built workflow definitions for common DAG topologies.
// Eliminates boilerplate in tests that rebuild linear, fan-out,
// fan-in, and diamond patterns from scratch every time.
package dagnatstest

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/worker"
)

// fixtureCounter ensures unique workflow names across parallel tests.
var fixtureCounter atomic.Int64

// fixtureName generates a unique workflow name from the test name
// and an atomic counter. Deterministic prefix aids log correlation.
func fixtureName(t *testing.T, prefix string) string {
	if t == nil {
		panic("fixtureName: t must not be nil")
	}
	t.Helper()
	if prefix == "" {
		panic("fixtureName: prefix must not be empty")
	}
	seq := fixtureCounter.Add(1)
	return fmt.Sprintf("%s-%s-%d", prefix, t.Name(), seq)
}

// LinearDef builds a workflow with n steps in sequence:
// task-0 -> task-1 -> ... -> task-(n-1).
// Each step uses task type "task-{i}".
func LinearDef(t *testing.T, n int) dag.WorkflowDef {
	t.Helper()
	if n < 1 {
		panic("LinearDef: n must be >= 1")
	}
	if n > 100 {
		panic("LinearDef: n must be <= 100")
	}

	name := fixtureName(t, "linear")
	wb := dag.NewWorkflow(name)

	prev := wb.Task("task-0", "task-0")
	for i := 1; i < n; i++ {
		id := fmt.Sprintf("task-%d", i)
		prev = wb.Task(id, id).After(prev)
	}

	def, err := wb.Build()
	if err != nil {
		t.Fatalf("LinearDef: Build failed: %v", err)
	}
	return def
}

// FanOutDef builds a workflow with 1 root and n parallel branches:
// root -> {branch-0, branch-1, ..., branch-(n-1)}.
func FanOutDef(t *testing.T, n int) dag.WorkflowDef {
	t.Helper()
	if n < 1 {
		panic("FanOutDef: n must be >= 1")
	}
	if n > 100 {
		panic("FanOutDef: n must be <= 100")
	}

	name := fixtureName(t, "fanout")
	wb := dag.NewWorkflow(name)

	root := wb.Task("root", "task-root")
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("branch-%d", i)
		task := fmt.Sprintf("task-branch-%d", i)
		wb.Task(id, task).After(root)
	}

	def, err := wb.Build()
	if err != nil {
		t.Fatalf("FanOutDef: Build failed: %v", err)
	}
	return def
}

// FanInDef builds a workflow with root -> n branches -> join:
// root -> {branch-0, ..., branch-(n-1)} -> join.
func FanInDef(t *testing.T, n int) dag.WorkflowDef {
	t.Helper()
	if n < 1 {
		panic("FanInDef: n must be >= 1")
	}
	if n > 100 {
		panic("FanInDef: n must be <= 100")
	}

	name := fixtureName(t, "fanin")
	wb := dag.NewWorkflow(name)

	root := wb.Task("root", "task-root")
	branches := make([]dag.StepRef, n)
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("branch-%d", i)
		task := fmt.Sprintf("task-branch-%d", i)
		branches[i] = wb.Task(id, task).After(root)
	}
	wb.Task("join", "task-join").After(branches...)

	def, err := wb.Build()
	if err != nil {
		t.Fatalf("FanInDef: Build failed: %v", err)
	}
	return def
}

// DiamondDef builds the classic diamond: A -> {B, C} -> D.
func DiamondDef(t *testing.T) dag.WorkflowDef {
	if t == nil {
		panic("DiamondDef: t must not be nil")
	}
	t.Helper()

	name := fixtureName(t, "diamond")
	wb := dag.NewWorkflow(name)

	a := wb.Task("a", "task-a")
	b := wb.Task("b", "task-b").After(a)
	c := wb.Task("c", "task-c").After(a)
	wb.Task("d", "task-d").After(b, c)

	def, err := wb.Build()
	if err != nil {
		t.Fatalf("DiamondDef: Build failed: %v", err)
	}
	return def
}

// PassHandler returns a HandlerFunc that completes immediately,
// passing the input through as output.
func PassHandler() worker.HandlerFunc {
	return func(tc worker.TaskContext) error {
		if tc == nil {
			panic("PassHandler: tc must not be nil")
		}
		return tc.Complete(tc.Input())
	}
}

// FailHandler returns a HandlerFunc that always fails permanently
// with the given message. Panics if msg is empty.
func FailHandler(msg string) worker.HandlerFunc {
	if msg == "" {
		panic("FailHandler: msg must not be empty")
	}
	return func(tc worker.TaskContext) error {
		if tc == nil {
			panic("FailHandler: tc must not be nil")
		}
		return worker.NewNonRetryableError(fmt.Errorf("%s", msg))
	}
}
