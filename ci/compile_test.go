// Package ci_test exercises the CI-spec compiler end-to-end using only
// in-memory YAML snippets — no filesystem, no network, no NATS.
//
// Methodology (TigerStyle TDD):
//   - Each test case names its expectation in the function name.
//   - Every test asserts both a positive (happy path) and a negative (rejection
//     or absence) condition to guard against trivially passing implementations.
//   - dag.Validate is called on every successfully compiled WorkflowDef to prove
//     the output is structurally sound, not just plausible-looking.
//   - Sample YAML is declared inline as constants; no external files.
package ci_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/ci"
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

// ciYMLTwoIndependentErrors has an unknown needs reference on one check and
// an invalid timeout on another — two unrelated problems in one spec.
const ciYMLTwoIndependentErrors = `
checks:
  a: { call: "a", needs: [missing] }
  b: { call: "b", timeout: "not-a-duration" }
`

// ciYMLBadDefaultsField has "defaults" set to a scalar instead of a mapping,
// which fails to decode into the Defaults struct.
const ciYMLBadDefaultsField = `
defaults: "not-a-map"
checks:
  test: { call: "test" }
`

// ciYMLBadMiddleCheck has three checks where the middle one ("b") has a
// bad value (a scalar instead of a mapping) -- Parse must report exactly
// that entry, at its own line, and keep "a" and "c" intact.
const ciYMLBadMiddleCheck = `
checks:
  a: { call: "a" }
  b: "not-a-map"
  c: { call: "c" }
`

// ciYMLTaskCheck has a single check using task: instead of call: — the
// runner-neutral path (#671) that must compile to a plain Task with no
// Dagger-shaped Metadata.
const ciYMLTaskCheck = `
checks:
  test: { task: "go-test" }
`

// ciYMLTaskAndCallBothSet sets both call: and task: on the same check,
// which is mutually exclusive and must produce exactly one diagnostic.
const ciYMLTaskAndCallBothSet = `
checks:
  test: { call: "test", task: "go-test" }
`

// ciYMLNeitherTaskNorCall sets neither call: nor task:, which is also
// mutually-exclusive-violating (exactly one must be set).
const ciYMLNeitherTaskNorCall = `
checks:
  test: { timeout: "5m" }
`

// ciYMLDeployTask is a deploy block using task: instead of call:.
const ciYMLDeployTask = `
checks:
  test: { call: "test" }
deploy:
  task: "deploy-worker"
  needs: [test]
`

// ciYMLDeployTaskAndCall sets both call: and task: on deploy.
const ciYMLDeployTaskAndCall = `
checks:
  test: { call: "test" }
deploy:
  call: "publish"
  task: "deploy-worker"
  needs: [test]
`

// ciYMLMixedTaskAndCall has one task: check and one call: check, with the
// task: check depending on the call: check -- proving needs edges compile
// correctly across the two runner kinds.
const ciYMLMixedTaskAndCall = `
checks:
  build: { call: "build" }
  test:  { task: "go-test", needs: [build] }
`

// ciYMLInvalidTaskValue has a task: value containing characters that are
// unsafe as a NATS subject token (a space), which dag.Validate would not
// catch (it only checks non-empty) and the engine does not sanitize (it
// builds the subject from step.Task verbatim).
const ciYMLInvalidTaskValue = `
checks:
  test: { task: "go test" }
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

// mustParse fails the test immediately on any Parse diagnostic — most tests
// only care about Compile behavior and should not silently swallow a parse
// regression.
func mustParse(t *testing.T, yml string) ci.Spec {
	t.Helper()
	spec, diags := ci.Parse([]byte(yml))
	if len(diags) != 0 {
		t.Fatalf("Parse: unexpected diagnostics: %+v", diags)
	}
	return spec
}

// TestCompileBasicChecks verifies that a three-check spec produces exactly three
// steps, that build correctly declares both test and lint as dependencies, and
// that test itself has no dependencies (entry step).
func TestCompileBasicChecks(t *testing.T) {
	spec := mustParse(t, ciYMLBasic)
	def, diags := ci.Compile("ci", spec)
	if len(diags) != 0 {
		t.Fatalf("Compile: unexpected diagnostics: %+v", diags)
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
	spec := mustParse(t, ciYMLDeployApproval)
	def, diags := ci.Compile("ci-deploy", spec)
	if len(diags) != 0 {
		t.Fatalf("Compile: unexpected diagnostics: %+v", diags)
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
	spec := mustParse(t, ciYMLDeployNoApproval)
	def, diags := ci.Compile("ci-deploy-no-gate", spec)
	if len(diags) != 0 {
		t.Fatalf("Compile: unexpected diagnostics: %+v", diags)
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
// check name is reported as a diagnostic rather than compiled.
func TestCompileUnknownNeeds(t *testing.T) {
	spec := mustParse(t, ciYMLUnknownNeeds)

	// Positive expectation of a diagnostic: unknown needs must be reported.
	_, diags := ci.Compile("ci-bad", spec)
	if len(diags) == 0 {
		t.Fatal("Compile returned no diagnostics for unknown needs reference, want >=1")
	}
	if diags[0].Field != "build" {
		t.Errorf("diags[0].Field = %q, want \"build\"", diags[0].Field)
	}

	// Negative: a spec with valid needs (no unknown refs) compiles without diagnostics.
	validSpec := mustParse(t, ciYMLBasic)
	if _, diags2 := ci.Compile("ci-valid", validSpec); len(diags2) != 0 {
		t.Errorf("Compile(valid needs) diagnostics = %+v, want none", diags2)
	}
}

// TestCompileEmptySpec verifies that a spec with no checks and no deploy is
// reported as a diagnostic rather than compiled.
func TestCompileEmptySpec(t *testing.T) {
	// Positive expectation of a diagnostic: an empty spec must be reported.
	_, diags := ci.Compile("ci-empty", ci.Spec{})
	if len(diags) == 0 {
		t.Fatal("Compile returned no diagnostics for empty spec, want >=1")
	}

	// Negative: a spec with at least one check compiles without diagnostics.
	spec := mustParse(t, ciYMLBasic)
	if _, diags := ci.Compile("ci-ok", spec); len(diags) != 0 {
		t.Errorf("Compile(valid spec) diagnostics = %+v, want none", diags)
	}
}

// TestCompileDeployBranchesRejected verifies that a deploy step with branches:
// set is reported as a diagnostic mentioning "branch" (and that the same
// deploy WITHOUT branches compiles cleanly).
func TestCompileDeployBranchesRejected(t *testing.T) {
	spec := mustParse(t, ciYMLDeployBranches)

	// Positive: deploy with branches: set must produce a diagnostic mentioning "branch".
	_, diags := ci.Compile("ci-branches", spec)
	if len(diags) == 0 {
		t.Fatal("Compile returned no diagnostics for deploy with branches, want >=1")
	}
	if !strings.Contains(diags[0].Message, "branch") {
		t.Errorf("diagnostic message %q does not mention \"branch\"", diags[0].Message)
	}

	// Negative: deploy WITHOUT branches compiles cleanly.
	nobranchSpec := mustParse(t, ciYMLDeployNoApproval)
	if _, diags := ci.Compile("ci-no-branch", nobranchSpec); len(diags) != 0 {
		t.Errorf("Compile(no branches) diagnostics = %+v, want none", diags)
	}
}

// TestCompileAccumulatesIndependentDiagnostics verifies that Compile does
// not stop at the first problem: two unrelated errors on two different
// checks both appear in the returned diagnostics.
func TestCompileAccumulatesIndependentDiagnostics(t *testing.T) {
	spec := mustParse(t, ciYMLTwoIndependentErrors)

	// Positive: both the unknown-needs and bad-timeout problems are reported.
	_, diags := ci.Compile("ci-two-errors", spec)
	if len(diags) < 2 {
		t.Fatalf("Compile diagnostics = %+v, want >=2 (one per independent error)", diags)
	}
	var sawA, sawB bool
	for _, d := range diags {
		if d.Field == "a" {
			sawA = true
		}
		if d.Field == "b" {
			sawB = true
		}
	}
	if !sawA || !sawB {
		t.Errorf("diags = %+v, want diagnostics for both check \"a\" and check \"b\"", diags)
	}

	// Negative: a spec with neither problem compiles without diagnostics.
	valid := mustParse(t, ciYMLBasic)
	if _, diags := ci.Compile("ci-two-errors-fixed", valid); len(diags) != 0 {
		t.Errorf("Compile(valid) diagnostics = %+v, want none", diags)
	}
}

// TestCompileDiagnosticsMaxCap verifies that Compile never accumulates more
// than DiagnosticsMax diagnostics and appends a terminal "too many" sentinel
// instead of growing without bound.
func TestCompileDiagnosticsMaxCap(t *testing.T) {
	var b strings.Builder
	b.WriteString("checks:\n")
	checkCount := ci.DiagnosticsMax + 50
	for i := 0; i < checkCount; i++ {
		fmt.Fprintf(&b, "  c%d: { call: \"c\", needs: [missing] }\n", i)
	}
	spec := mustParse(t, b.String())

	// Positive: diagnostics are capped at DiagnosticsMax plus one sentinel.
	_, diags := ci.Compile("ci-overflow", spec)
	if len(diags) != ci.DiagnosticsMax+1 {
		t.Fatalf("len(diags) = %d, want %d (cap + sentinel)", len(diags), ci.DiagnosticsMax+1)
	}
	if !strings.Contains(diags[len(diags)-1].Message, "too many") {
		t.Errorf("final diagnostic = %+v, want a \"too many\" sentinel", diags[len(diags)-1])
	}

	// Negative: a spec producing fewer than DiagnosticsMax problems never
	// hits the sentinel.
	spec2 := mustParse(t, ciYMLTwoIndependentErrors)
	if _, diags2 := ci.Compile("ci-few-errors", spec2); len(diags2) >= ci.DiagnosticsMax {
		t.Errorf("len(diags2) = %d, want < DiagnosticsMax", len(diags2))
	}
}

// TestParseKeepsLineAndColumnForBadField verifies that Parse reports the
// YAML source position (Line/Column) of a field that fails to decode, and
// names the offending field.
func TestParseKeepsLineAndColumnForBadField(t *testing.T) {
	spec, diags := ci.Parse([]byte(ciYMLBadDefaultsField))

	// Positive: exactly one diagnostic, positioned at the bad "defaults" value.
	if len(diags) != 1 {
		t.Fatalf("diags = %+v, want exactly 1", diags)
	}
	if diags[0].Field != "defaults" {
		t.Errorf("diags[0].Field = %q, want \"defaults\"", diags[0].Field)
	}
	if diags[0].Line <= 0 || diags[0].Column <= 0 {
		t.Errorf("diags[0] = %+v, want positive Line and Column", diags[0])
	}

	// Negative: the Checks field, which decoded fine, is still populated —
	// one bad field must not blank out the rest of the spec.
	if _, ok := spec.Checks["test"]; !ok {
		t.Errorf("spec.Checks = %+v, want \"test\" present despite bad defaults field", spec.Checks)
	}
}

// TestParseChecksReportsPerEntryDiagnostics verifies that a bad entry
// inside "checks" is reported as its own Diagnostic (Field "checks.<name>",
// positioned at that entry, not the whole checks: block) and that the
// other, valid entries survive in the returned Spec.
func TestParseChecksReportsPerEntryDiagnostics(t *testing.T) {
	spec, diags := ci.Parse([]byte(ciYMLBadMiddleCheck))

	// Positive: exactly one diagnostic, naming checks.b at its own position.
	if len(diags) != 1 {
		t.Fatalf("diags = %+v, want exactly 1", diags)
	}
	if diags[0].Field != "checks.b" {
		t.Errorf("diags[0].Field = %q, want \"checks.b\"", diags[0].Field)
	}
	if diags[0].Line <= 0 || diags[0].Column <= 0 {
		t.Errorf("diags[0] = %+v, want positive Line and Column", diags[0])
	}

	// Negative: the other two checks ("a" and "c") are intact, and the
	// bad one ("b") is absent rather than present with zero-value fields.
	if _, ok := spec.Checks["a"]; !ok {
		t.Errorf("spec.Checks = %+v, want \"a\" present", spec.Checks)
	}
	if _, ok := spec.Checks["c"]; !ok {
		t.Errorf("spec.Checks = %+v, want \"c\" present", spec.Checks)
	}
	if _, ok := spec.Checks["b"]; ok {
		t.Errorf("spec.Checks = %+v, want \"b\" absent (it failed to decode)", spec.Checks)
	}
}

// TestParseValidSpecHasNoDiagnostics verifies the negative space for Parse:
// a well-formed spec produces zero diagnostics.
func TestParseValidSpecHasNoDiagnostics(t *testing.T) {
	_, diags := ci.Parse([]byte(ciYMLBasic))
	if len(diags) != 0 {
		t.Errorf("diags = %+v, want none for a valid spec", diags)
	}

	// Negative-of-negative: an empty document also produces no diagnostics
	// (Compile, not Parse, is where an empty spec is rejected).
	_, diags2 := ci.Parse([]byte(""))
	if len(diags2) != 0 {
		t.Errorf("diags2 = %+v, want none for an empty document", diags2)
	}
}

// TestCompileYAMLValidSpecYieldsValidatedDef verifies that CompileYAML, the
// Parse+Compile composition callers use when they hold raw bytes, produces
// a def that itself passes dag.Validate.
func TestCompileYAMLValidSpecYieldsValidatedDef(t *testing.T) {
	def, diags := ci.CompileYAML("ci-yaml", []byte(ciYMLBasic))
	if len(diags) != 0 {
		t.Fatalf("CompileYAML: unexpected diagnostics: %+v", diags)
	}

	// Positive: the resulting def passes dag.Validate.
	if err := dag.Validate(def); err != nil {
		t.Errorf("dag.Validate returned error: %v", err)
	}

	// Negative: a spec with a parse-level problem short-circuits before
	// Compile runs and yields diagnostics, not a panic or a bogus def.
	_, diags2 := ci.CompileYAML("ci-yaml-bad", []byte(ciYMLBadDefaultsField))
	if len(diags2) == 0 {
		t.Fatal("CompileYAML(bad defaults) diagnostics = none, want >=1")
	}
}

// TestCompileTaskCheckCompilesVerbatimWithNoDaggerInput verifies that a
// check using task: compiles to a StepDef whose Task is the value verbatim
// and whose Metadata carries no Dagger-shaped module/call keys.
func TestCompileTaskCheckCompilesVerbatimWithNoDaggerInput(t *testing.T) {
	spec := mustParse(t, ciYMLTaskCheck)
	def, diags := ci.Compile("ci-task", spec)
	if len(diags) != 0 {
		t.Fatalf("Compile: unexpected diagnostics: %+v", diags)
	}

	// Positive: the step's Task is the task: value verbatim.
	step := stepByID(t, def.Steps, "test")
	if step.Task != "go-test" {
		t.Errorf("step.Task = %q, want \"go-test\"", step.Task)
	}

	// Negative: no Dagger-shaped input (module/call) leaks into Metadata.
	if _, ok := step.Metadata["module"]; ok {
		t.Errorf("step.Metadata = %+v, want no \"module\" key for a task: check", step.Metadata)
	}
	if _, ok := step.Metadata["call"]; ok {
		t.Errorf("step.Metadata = %+v, want no \"call\" key for a task: check", step.Metadata)
	}

	if err := dag.Validate(def); err != nil {
		t.Errorf("dag.Validate returned error: %v", err)
	}
}

// TestCompileTaskAndCallBothSetIsRejected verifies that setting both call:
// and task: on the same check produces exactly one diagnostic naming that
// check, and that fixing it (setting only one) compiles cleanly.
func TestCompileTaskAndCallBothSetIsRejected(t *testing.T) {
	spec := mustParse(t, ciYMLTaskAndCallBothSet)

	// Positive: exactly one diagnostic naming the "test" check.
	_, diags := ci.Compile("ci-both", spec)
	if len(diags) != 1 {
		t.Fatalf("diags = %+v, want exactly 1", diags)
	}
	if diags[0].Field != "test" {
		t.Errorf("diags[0].Field = %q, want \"test\"", diags[0].Field)
	}

	// Negative: a spec with only one of call/task set compiles cleanly.
	okSpec := mustParse(t, ciYMLTaskCheck)
	if _, diags2 := ci.Compile("ci-both-fixed", okSpec); len(diags2) != 0 {
		t.Errorf("Compile(task only) diagnostics = %+v, want none", diags2)
	}
}

// TestCompileNeitherTaskNorCallIsRejected verifies that a check with
// neither call: nor task: set produces exactly one diagnostic.
func TestCompileNeitherTaskNorCallIsRejected(t *testing.T) {
	spec := mustParse(t, ciYMLNeitherTaskNorCall)

	// Positive: exactly one diagnostic naming the "test" check.
	_, diags := ci.Compile("ci-neither", spec)
	if len(diags) != 1 {
		t.Fatalf("diags = %+v, want exactly 1", diags)
	}
	if diags[0].Field != "test" {
		t.Errorf("diags[0].Field = %q, want \"test\"", diags[0].Field)
	}

	// Negative: the same spec with call: added compiles cleanly.
	okSpec := mustParse(t, ciYMLBasic)
	if _, diags2 := ci.Compile("ci-neither-fixed", okSpec); len(diags2) != 0 {
		t.Errorf("Compile(call set) diagnostics = %+v, want none", diags2)
	}
}

// TestCompileDeployWithTask verifies that a deploy block using task:
// compiles to a StepDef whose Task is the value verbatim, with no
// Dagger-shaped Metadata, same as a task: check.
func TestCompileDeployWithTask(t *testing.T) {
	spec := mustParse(t, ciYMLDeployTask)
	def, diags := ci.Compile("ci-deploy-task", spec)
	if len(diags) != 0 {
		t.Fatalf("Compile: unexpected diagnostics: %+v", diags)
	}

	// Positive: deploy step's Task is the task: value verbatim.
	deploy := stepByID(t, def.Steps, "deploy")
	if deploy.Task != "deploy-worker" {
		t.Errorf("deploy.Task = %q, want \"deploy-worker\"", deploy.Task)
	}

	// Negative: no Dagger-shaped Metadata for a task: deploy.
	if len(deploy.Metadata) != 0 {
		t.Errorf("deploy.Metadata = %+v, want empty for a task: deploy", deploy.Metadata)
	}

	if err := dag.Validate(def); err != nil {
		t.Errorf("dag.Validate returned error: %v", err)
	}
}

// TestCompileDeployTaskAndCallBothSetIsRejected verifies deploy's
// exclusivity diagnostic mirrors the per-check one, naming "deploy".
func TestCompileDeployTaskAndCallBothSetIsRejected(t *testing.T) {
	spec := mustParse(t, ciYMLDeployTaskAndCall)

	// Positive: exactly one diagnostic naming "deploy".
	_, diags := ci.Compile("ci-deploy-both", spec)
	if len(diags) != 1 {
		t.Fatalf("diags = %+v, want exactly 1", diags)
	}
	if diags[0].Field != "deploy" {
		t.Errorf("diags[0].Field = %q, want \"deploy\"", diags[0].Field)
	}

	// Negative: the task-only deploy variant compiles cleanly.
	okSpec := mustParse(t, ciYMLDeployTask)
	if _, diags2 := ci.Compile("ci-deploy-both-fixed", okSpec); len(diags2) != 0 {
		t.Errorf("Compile(deploy task only) diagnostics = %+v, want none", diags2)
	}
}

// TestCompileMixedTaskAndCallChecksCompile verifies a spec with one call:
// check and one task: check compiles both, with the needs edge between
// them intact regardless of which runner kind is on each side.
func TestCompileMixedTaskAndCallChecksCompile(t *testing.T) {
	spec := mustParse(t, ciYMLMixedTaskAndCall)
	def, diags := ci.Compile("ci-mixed", spec)
	if len(diags) != 0 {
		t.Fatalf("Compile: unexpected diagnostics: %+v", diags)
	}

	// Positive: both steps compiled with their respective runner shapes.
	build := stepByID(t, def.Steps, "build")
	if build.Task != "dagger.call" || build.Metadata["call"] != "build" {
		t.Errorf("build = %+v, want dagger.call with call metadata", build)
	}
	test := stepByID(t, def.Steps, "test")
	if test.Task != "go-test" {
		t.Errorf("test.Task = %q, want \"go-test\"", test.Task)
	}

	// Negative: the needs edge from test -> build survives across runner kinds.
	if !slices.Contains(test.DependsOn, "build") {
		t.Errorf("test.DependsOn %v does not contain \"build\"", test.DependsOn)
	}

	if err := dag.Validate(def); err != nil {
		t.Errorf("dag.Validate returned error: %v", err)
	}
}

// TestCompileInvalidTaskValueIsRejected verifies that a task: value
// containing NATS-subject-unsafe characters (here, a space) is rejected at
// compile time with a diagnostic, rather than silently minting a malformed
// subject at dispatch time — dag.Validate only checks Task is non-empty,
// and the engine's StepSubject builds "task.{Task}.{runID}" verbatim with
// no sanitization.
func TestCompileInvalidTaskValueIsRejected(t *testing.T) {
	spec := mustParse(t, ciYMLInvalidTaskValue)

	// Positive: exactly one diagnostic naming the "test" check.
	_, diags := ci.Compile("ci-bad-task", spec)
	if len(diags) != 1 {
		t.Fatalf("diags = %+v, want exactly 1", diags)
	}
	if diags[0].Field != "test" {
		t.Errorf("diags[0].Field = %q, want \"test\"", diags[0].Field)
	}

	// Negative: a task: value with only safe characters compiles cleanly.
	okSpec := mustParse(t, ciYMLTaskCheck)
	if _, diags2 := ci.Compile("ci-bad-task-fixed", okSpec); len(diags2) != 0 {
		t.Errorf("Compile(valid task) diagnostics = %+v, want none", diags2)
	}
}

// ciCallSpecHashes pins the DefHash of every call:-only example spec as
// compiled by the pre-#671 compiler, captured before task: existed. A
// changed hash here means the call: compile path stopped being
// byte-identical -- the one thing #671 must never do.
var ciCallSpecHashes = map[string]string{
	"golden-basic":  "e9f5043068315e1cc55a60de4295ed91d5d56721a968b94affd6e5409bdf5a56",
	"golden-deploy": "93d81e4872504eb1f9c3166a609ba28258a453f34a146fde654a67aa354f26a1",
}

// TestCompileCallSpecsAreByteIdenticalToPreTaskCompiler verifies that
// adding task: support did not change one bit of the call: compile path:
// the DefHash of ciYMLBasic and ciYMLDeployApproval, compiled through
// today's compiler, must equal the hashes captured from the compiler
// before #671 touched compile.go/spec.go.
func TestCompileCallSpecsAreByteIdenticalToPreTaskCompiler(t *testing.T) {
	cases := []struct {
		hashKey string
		defName string
		yml     string
	}{
		{"golden-basic", "golden-basic", ciYMLBasic},
		{"golden-deploy", "golden-deploy", ciYMLDeployApproval},
	}
	for _, c := range cases {
		def, diags := ci.CompileYAML(c.defName, []byte(c.yml))
		if len(diags) != 0 {
			t.Fatalf("CompileYAML(%s): unexpected diagnostics: %+v", c.defName, diags)
		}

		// Positive: hash matches the pinned pre-#671 golden value.
		got := dag.DefHash(def)
		want := ciCallSpecHashes[c.hashKey]
		if got != want {
			t.Errorf("DefHash(%s) = %q, want %q (call: path is not byte-identical)",
				c.defName, got, want)
		}
	}

	// Negative: a task: spec's hash does NOT collide with either golden
	// call: hash -- proving the two runner kinds produce distinguishable
	// output rather than both degenerating to the same shape.
	taskDef, diags := ci.CompileYAML("golden-basic", []byte(ciYMLTaskCheck))
	if len(diags) != 0 {
		t.Fatalf("CompileYAML(task check): unexpected diagnostics: %+v", diags)
	}
	taskHash := dag.DefHash(taskDef)
	if taskHash == ciCallSpecHashes["golden-basic"] {
		t.Errorf("task: spec hash collides with the call: golden hash %q", taskHash)
	}
}
