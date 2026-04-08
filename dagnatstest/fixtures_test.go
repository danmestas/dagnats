// dagnatstest/fixtures_test.go
// Methodology: pure unit tests verifying fixture structure — step
// counts, dependency wiring, and handler behavior. No NATS server
// needed since we only inspect the built WorkflowDef and call
// handlers against mocks.
package dagnatstest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/worker"
)

// findStep returns the StepDef with the given ID, or nil if absent.
func findStep(def dag.WorkflowDef, id string) *dag.StepDef {
	for i := range def.Steps {
		if def.Steps[i].ID == id {
			return &def.Steps[i]
		}
	}
	return nil
}

// hasDep returns true if step depends on depID.
func hasDep(step *dag.StepDef, depID string) bool {
	if step == nil {
		return false
	}
	for _, d := range step.DependsOn {
		if d == depID {
			return true
		}
	}
	return false
}

func TestLinearDefStepCount(t *testing.T) {
	def := LinearDef(t, 3)

	// Positive: correct number of steps.
	if len(def.Steps) != 3 {
		t.Fatalf("expected 3 steps, got %d", len(def.Steps))
	}

	// Positive: sequential dependencies.
	step1 := findStep(def, "task-1")
	if !hasDep(step1, "task-0") {
		t.Fatal("task-1 should depend on task-0")
	}
	step2 := findStep(def, "task-2")
	if !hasDep(step2, "task-1") {
		t.Fatal("task-2 should depend on task-1")
	}

	// Negative: first step has no dependencies.
	step0 := findStep(def, "task-0")
	if len(step0.DependsOn) != 0 {
		t.Fatalf(
			"task-0 should have no deps, got %v",
			step0.DependsOn,
		)
	}
}

func TestLinearDefSingleStep(t *testing.T) {
	def := LinearDef(t, 1)

	// Positive: single step exists.
	if len(def.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(def.Steps))
	}

	// Positive: step ID is correct.
	if def.Steps[0].ID != "task-0" {
		t.Fatalf("expected task-0, got %s", def.Steps[0].ID)
	}

	// Negative: no dependencies on single step.
	if len(def.Steps[0].DependsOn) != 0 {
		t.Fatal("single step should have no deps")
	}
}

func TestLinearDefPanicsOnInvalidN(t *testing.T) {
	// Negative: n < 1 panics.
	assertPanics(t, "n=0", func() { LinearDef(t, 0) })

	// Negative: n > 100 panics.
	assertPanics(t, "n=101", func() { LinearDef(t, 101) })
}

func TestFanOutDefStructure(t *testing.T) {
	def := FanOutDef(t, 3)

	// Positive: 1 root + 3 branches = 4 steps.
	if len(def.Steps) != 4 {
		t.Fatalf("expected 4 steps, got %d", len(def.Steps))
	}

	// Positive: all branches depend on root.
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("branch-%d", i)
		step := findStep(def, id)
		if step == nil {
			t.Fatalf("missing step %s", id)
		}
		if !hasDep(step, "root") {
			t.Fatalf("%s should depend on root", id)
		}
	}

	// Negative: root has no dependencies.
	root := findStep(def, "root")
	if len(root.DependsOn) != 0 {
		t.Fatalf(
			"root should have no deps, got %v",
			root.DependsOn,
		)
	}
}

func TestFanOutDefPanicsOnInvalidN(t *testing.T) {
	assertPanics(t, "n=0", func() { FanOutDef(t, 0) })
	assertPanics(t, "n=101", func() { FanOutDef(t, 101) })
}

func TestFanInDefStructure(t *testing.T) {
	def := FanInDef(t, 3)

	// Positive: root + 3 branches + join = 5 steps.
	if len(def.Steps) != 5 {
		t.Fatalf("expected 5 steps, got %d", len(def.Steps))
	}

	// Positive: join depends on all branches.
	join := findStep(def, "join")
	if join == nil {
		t.Fatal("missing join step")
	}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("branch-%d", i)
		if !hasDep(join, id) {
			t.Fatalf("join should depend on %s", id)
		}
	}

	// Negative: branches do not depend on join.
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("branch-%d", i)
		step := findStep(def, id)
		if hasDep(step, "join") {
			t.Fatalf("%s should not depend on join", id)
		}
	}
}

func TestFanInDefPanicsOnInvalidN(t *testing.T) {
	assertPanics(t, "n=0", func() { FanInDef(t, 0) })
	assertPanics(t, "n=101", func() { FanInDef(t, 101) })
}

func TestDiamondDefStructure(t *testing.T) {
	def := DiamondDef(t)

	// Positive: exactly 4 steps.
	if len(def.Steps) != 4 {
		t.Fatalf("expected 4 steps, got %d", len(def.Steps))
	}

	// Positive: b and c depend on a.
	b := findStep(def, "b")
	c := findStep(def, "c")
	if !hasDep(b, "a") {
		t.Fatal("b should depend on a")
	}
	if !hasDep(c, "a") {
		t.Fatal("c should depend on a")
	}

	// Positive: d depends on both b and c.
	d := findStep(def, "d")
	if !hasDep(d, "b") || !hasDep(d, "c") {
		t.Fatalf(
			"d should depend on b and c, got %v",
			d.DependsOn,
		)
	}

	// Negative: a has no dependencies.
	a := findStep(def, "a")
	if len(a.DependsOn) != 0 {
		t.Fatalf("a should have no deps, got %v", a.DependsOn)
	}
}

func TestPassHandlerCompletes(t *testing.T) {
	handler := PassHandler()

	// Positive: handler is not nil.
	if handler == nil {
		t.Fatal("PassHandler returned nil")
	}

	// Positive: handler calls Complete with input.
	tc := &stubTaskContext{input: []byte(`"hello"`)}
	err := handler(tc)
	if err != nil {
		t.Fatalf("PassHandler returned error: %v", err)
	}
	if !tc.completed {
		t.Fatal("PassHandler should call Complete")
	}
	if string(tc.output) != `"hello"` {
		t.Fatalf(
			"expected output %q, got %q",
			`"hello"`, string(tc.output),
		)
	}
}

func TestFailHandlerFails(t *testing.T) {
	handler := FailHandler("boom")

	// Positive: handler is not nil.
	if handler == nil {
		t.Fatal("FailHandler returned nil")
	}

	// Positive: handler returns a non-nil error.
	tc := &stubTaskContext{input: []byte(`"x"`)}
	err := handler(tc)
	if err == nil {
		t.Fatal("FailHandler should return an error")
	}

	// Positive: error is a NonRetryableError.
	var nre *worker.NonRetryableError
	if !errors.As(err, &nre) {
		t.Fatalf(
			"expected NonRetryableError, got %T", err,
		)
	}

	// Positive: error message contains the provided message.
	if err.Error() != "boom" {
		t.Fatalf(
			"expected error %q, got %q",
			"boom", err.Error(),
		)
	}
}

func TestFailHandlerPanicsOnEmptyMsg(t *testing.T) {
	// Negative: empty msg panics.
	assertPanics(t, "empty msg", func() { FailHandler("") })

	// Positive: non-empty msg does not panic.
	handler := FailHandler("ok")
	if handler == nil {
		t.Fatal("FailHandler with valid msg returned nil")
	}
}

func TestUniqueWorkflowNames(t *testing.T) {
	def1 := LinearDef(t, 1)
	def2 := LinearDef(t, 1)

	// Positive: names are different.
	if def1.Name == def2.Name {
		t.Fatalf(
			"expected unique names, both are %q",
			def1.Name,
		)
	}

	// Positive: names contain the test name for traceability.
	if def1.Name == "" || def2.Name == "" {
		t.Fatal("workflow names should not be empty")
	}
}

// assertPanics verifies that fn panics. Fails the test if not.
func assertPanics(t *testing.T, label string, fn func()) {
	t.Helper()
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf(
				"%s: expected panic, got none", label,
			)
		}
	}()
	fn()
}

// stubTaskContext is a minimal stub implementing
// worker.TaskContext for testing handlers without NATS.
type stubTaskContext struct {
	input     []byte
	output    []byte
	completed bool
	failed    bool
}

func (s *stubTaskContext) Context() context.Context { return context.Background() }
func (s *stubTaskContext) Input() []byte            { return s.input }
func (s *stubTaskContext) RunID() string            { return "stub-run" }
func (s *stubTaskContext) StepID() string           { return "stub-step" }
func (s *stubTaskContext) RetryCount() int          { return 0 }

func (s *stubTaskContext) Complete(out []byte) error {
	s.completed = true
	s.output = out
	return nil
}

func (s *stubTaskContext) Fail(err error) error { return nil }

func (s *stubTaskContext) FailPermanent(err error) error {
	s.failed = true
	return nil
}

func (s *stubTaskContext) FailRetryAfter(
	err error, after time.Duration,
) error {
	return nil
}

func (s *stubTaskContext) Continue(
	out []byte,
) error {
	return nil
}

func (s *stubTaskContext) PutStream(
	data []byte,
) error {
	return nil
}

func (s *stubTaskContext) Heartbeat() error { return nil }

func (s *stubTaskContext) Checkpoint(
	state []byte,
) error {
	return nil
}

func (s *stubTaskContext) LoadCheckpoint() ([]byte, error) {
	return nil, nil
}

func (s *stubTaskContext) Pause(
	name string, duration time.Duration,
) error {
	return nil
}

func (s *stubTaskContext) WaitForSignal(
	name string, timeout time.Duration,
) ([]byte, error) {
	return nil, nil
}

func (s *stubTaskContext) SendSignal(
	runID, name string, data []byte,
) error {
	return nil
}
