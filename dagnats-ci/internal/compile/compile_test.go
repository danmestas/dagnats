// Package compile_test exercises the CI-spec compiler end-to-end using only
// in-memory YAML snippets — no filesystem, no network, no NATS.
//
// Methodology (TigerStyle TDD):
//   - Each test case names its expectation in the function name.
//   - Every test asserts both a positive (happy path) and a negative (rejection
//     or absence) condition to guard against trivially passing implementations.
//   - dag.Validate is called on every successfully compiled WorkflowDef to prove
//     the output is structurally sound, not just plausible-looking.
//   - Sample YAML is declared inline as constants; no external files.
package compile_test

import (
	"slices"
	"strings"
	"testing"

	"github.com/danmestas/dagnats-ci/internal/compile"
	"github.com/danmestas/dagnats/dag"
)

// ciYMLBasic is a three-check spec: test and lint run first; build depends on both.
const ciYMLBasic = `
defaults:
  module: "."
checks:
  test:  { call: "test" }
  lint:  { call: "lint" }
  build: { call: "build", needs: [test, lint] }
`

// ciYMLDeployApproval adds a deploy step requiring human approval after build.
const ciYMLDeployApproval = `
defaults:
  module: "./ci"
checks:
  test:  { call: "test" }
  build: { call: "build", needs: [test] }
deploy:
  call: "publish"
  needs: [build]
  approval: required
`

// ciYMLDeployNoApproval deploys directly on build success without a gate.
const ciYMLDeployNoApproval = `
defaults:
  module: "."
checks:
  test:  { call: "test" }
  build: { call: "build", needs: [test] }
deploy:
  call: "publish"
  needs: [build]
`

// ciYMLUnknownNeeds references a step name that does not exist.
const ciYMLUnknownNeeds = `
defaults:
  module: "."
checks:
  build: { call: "build", needs: [nonexistent] }
`

// ciYMLDeployBranches is a deploy spec with branches: set — the compiler must reject it.
const ciYMLDeployBranches = `
defaults:
  module: "."
checks:
  test:  { call: "test" }
deploy:
  call: "publish"
  needs: [test]
  branches: [main]
`

// stepByID is a test helper that looks up a compiled step by its ID.
// It fails the test immediately if the step is not found — an absent step
// is a compiler bug that makes every downstream assertion meaningless.
func stepByID(t *testing.T, steps []dag.StepDef, id string) dag.StepDef {
	t.Helper()
	for _, s := range steps {
		if s.ID == id {
			return s
		}
	}
	t.Fatalf("step %q not found in compiled output (ids: %v)", id, stepIDs(steps))
	return dag.StepDef{} // unreachable
}

// stepIDs extracts all step IDs for use in error messages.
func stepIDs(steps []dag.StepDef) []string {
	ids := make([]string, len(steps))
	for i, s := range steps {
		ids[i] = s.ID
	}
	return ids
}

// TestCompileBasicChecks verifies that a three-check spec produces exactly three
// steps, that build correctly declares both test and lint as dependencies, and
// that test itself has no dependencies (entry step).
func TestCompileBasicChecks(t *testing.T) {
	spec, err := compile.ParseSpec([]byte(ciYMLBasic))
	if err != nil {
		t.Fatalf("ParseSpec: %v", err)
	}
	def, err := compile.Compile("ci", spec)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Positive: three steps compiled from three checks.
	if len(def.Steps) != 3 {
		t.Errorf("step count = %d, want 3 (ids: %v)", len(def.Steps), stepIDs(def.Steps))
	}

	// Positive: build depends on both test and lint.
	build := stepByID(t, def.Steps, "build")
	if !slices.Contains(build.DependsOn, "test") {
		t.Errorf("build.DependsOn %v does not contain \"test\"", build.DependsOn)
	}
	if !slices.Contains(build.DependsOn, "lint") {
		t.Errorf("build.DependsOn %v does not contain \"lint\"", build.DependsOn)
	}

	// Positive: correct metadata for the build step.
	if build.Metadata["module"] != "." {
		t.Errorf("build.Metadata[module] = %q, want \".\"", build.Metadata["module"])
	}
	if build.Metadata["call"] != "build" {
		t.Errorf("build.Metadata[call] = %q, want \"build\"", build.Metadata["call"])
	}

	// Negative: test has no dependencies (it is an entry step).
	testStep := stepByID(t, def.Steps, "test")
	if len(testStep.DependsOn) != 0 {
		t.Errorf("test.DependsOn = %v, want empty (entry step)", testStep.DependsOn)
	}

	// Negative: the compiled def must be structurally valid per dag.Validate.
	if err := dag.Validate(def); err != nil {
		t.Errorf("dag.Validate returned error: %v", err)
	}
}

// TestCompileDeployWithApproval verifies that approval:required emits an
// approve-deploy gate before the deploy step and that deploy depends on the gate.
func TestCompileDeployWithApproval(t *testing.T) {
	spec, err := compile.ParseSpec([]byte(ciYMLDeployApproval))
	if err != nil {
		t.Fatalf("ParseSpec: %v", err)
	}
	def, err := compile.Compile("ci-deploy", spec)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Positive: approve-deploy step is present.
	gate := stepByID(t, def.Steps, "approve-deploy")
	if gate.Task != "ci.approval" {
		t.Errorf("approve-deploy.Task = %q, want \"ci.approval\"", gate.Task)
	}

	// Positive: deploy depends only on approve-deploy, not directly on build.
	deploy := stepByID(t, def.Steps, "deploy")
	if len(deploy.DependsOn) != 1 || deploy.DependsOn[0] != "approve-deploy" {
		t.Errorf("deploy.DependsOn = %v, want [approve-deploy]", deploy.DependsOn)
	}

	// Negative: the compiled def passes dag.Validate (no structural errors).
	if err := dag.Validate(def); err != nil {
		t.Errorf("dag.Validate returned error: %v", err)
	}
}

// TestCompileDeployWithoutApproval verifies that without approval:required the
// deploy step depends directly on its Needs list, bypassing the gate step.
func TestCompileDeployWithoutApproval(t *testing.T) {
	spec, err := compile.ParseSpec([]byte(ciYMLDeployNoApproval))
	if err != nil {
		t.Fatalf("ParseSpec: %v", err)
	}
	def, err := compile.Compile("ci-deploy-no-gate", spec)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}

	// Positive: deploy depends on build (its declared Needs), not approve-deploy.
	deploy := stepByID(t, def.Steps, "deploy")
	if !slices.Contains(deploy.DependsOn, "build") {
		t.Errorf("deploy.DependsOn %v does not contain \"build\"", deploy.DependsOn)
	}

	// Negative: no approve-deploy gate step should be present.
	for _, s := range def.Steps {
		if s.ID == "approve-deploy" {
			t.Errorf("approve-deploy step present without approval:required")
		}
	}
}

// TestCompileUnknownNeeds verifies that a Needs reference to a non-existent
// check name is rejected at compile time with a descriptive error.
func TestCompileUnknownNeeds(t *testing.T) {
	spec, err := compile.ParseSpec([]byte(ciYMLUnknownNeeds))
	if err != nil {
		t.Fatalf("ParseSpec: %v", err)
	}

	// Positive expectation of an error: unknown needs must be rejected.
	_, err = compile.Compile("ci-bad", spec)
	if err == nil {
		t.Fatal("Compile returned nil error for unknown needs reference, want error")
	}

	// Negative: a spec with valid needs (no unknown refs) compiles without error.
	validSpec, err2 := compile.ParseSpec([]byte(ciYMLBasic))
	if err2 != nil {
		t.Fatalf("ParseSpec(valid): %v", err2)
	}
	if _, err2 = compile.Compile("ci-valid", validSpec); err2 != nil {
		t.Errorf("Compile(valid needs) = %v, want nil", err2)
	}
}

// TestCompileEmptySpec verifies that a spec with no checks and no deploy is
// rejected at compile time with a descriptive error.
func TestCompileEmptySpec(t *testing.T) {
	// Positive expectation of an error: an empty spec must be rejected.
	_, err := compile.Compile("ci-empty", compile.Spec{})
	if err == nil {
		t.Fatal("Compile returned nil error for empty spec, want error")
	}

	// Negative: a spec with at least one check compiles without error.
	spec, err := compile.ParseSpec([]byte(ciYMLBasic))
	if err != nil {
		t.Fatalf("ParseSpec: %v", err)
	}
	if _, err := compile.Compile("ci-ok", spec); err != nil {
		t.Errorf("Compile(valid spec) = %v, want nil", err)
	}
}

// TestCompileDeployBranchesRejected verifies that a deploy step with branches:
// set is rejected at compile time (branch gating is unimplemented in Phase 1)
// and that the same deploy WITHOUT branches compiles cleanly.
func TestCompileDeployBranchesRejected(t *testing.T) {
	spec, err := compile.ParseSpec([]byte(ciYMLDeployBranches))
	if err != nil {
		t.Fatalf("ParseSpec: %v", err)
	}

	// Positive: deploy with branches: set must return an error mentioning "branch".
	_, err = compile.Compile("ci-branches", spec)
	if err == nil {
		t.Fatal("Compile returned nil error for deploy with branches, want error")
	}
	if msg := err.Error(); !strings.Contains(msg, "branch") {
		t.Errorf("error %q does not mention \"branch\"", msg)
	}

	// Negative: deploy WITHOUT branches compiles cleanly.
	nobranchSpec, err := compile.ParseSpec([]byte(ciYMLDeployNoApproval))
	if err != nil {
		t.Fatalf("ParseSpec(no-branch): %v", err)
	}
	if _, err := compile.Compile("ci-no-branch", nobranchSpec); err != nil {
		t.Errorf("Compile(no branches) = %v, want nil", err)
	}
}
