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
	"time"

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

// ciYMLRetriesShorthand uses the retries: N shorthand on a check (#681).
const ciYMLRetriesShorthand = `
checks:
  test: { call: "test", retries: 3 }
`

// ciYMLRetryBlock uses the full retry: mapping with every field set.
const ciYMLRetryBlock = `
checks:
  test:
    call: "test"
    retry:
      max_attempts: 5
      strategy: exponential
      initial_delay: 30s
      max_delay: 2m
      multiplier: 2
`

// ciYMLRetriesAndRetryBothSet sets both retries: and retry:, which is
// mutually exclusive.
const ciYMLRetriesAndRetryBothSet = `
checks:
  test:
    call: "test"
    retries: 3
    retry:
      max_attempts: 5
`

// ciYMLCheckUnknownField has an unrecognized "run:" key on a check, plus a
// typo'd "retres:" on another -- the issue's exact repro (#681).
const ciYMLCheckUnknownField = `
checks:
  test:
    call: "test"
    run: "echo hi"
  build:
    call: "build"
    retres: 3
`

// ciYMLCheckRetryUnknownField has an unrecognized key nested inside the
// retry: block.
const ciYMLCheckRetryUnknownField = `
checks:
  test:
    call: "test"
    retry:
      max_attempts: 3
      backoff: "slow"
`

// ciYMLDeployUnknownField has an unrecognized key on the deploy block.
const ciYMLDeployUnknownField = `
checks:
  test: { call: "test" }
deploy:
  call: "publish"
  needs: [test]
  run: "echo deploying"
`

// ciYMLMergeKeyCheck reproduces the merge-key regression (#681 BLOCKER 1):
// a checks entry ("base") holds an anchor, and a sibling ("derived") merges
// it in via "<<: *b". Before the merge-key fix, "<<" itself was reported as
// an unknown field -- a spec that compiled clean at the merge base would
// fail to register after the unknown-field scan landed.
const ciYMLMergeKeyCheck = `
checks:
  base: &b
    call: "base"
  derived:
    <<: *b
    needs: [base]
`

// ciYMLTopLevelUnknownField has a typo'd top-level key ("defalts" instead
// of "defaults") that must be diagnosed, not silently ignored (#681 MAJOR 3).
const ciYMLTopLevelUnknownField = `
defalts:
  module: "."
checks:
  test: { call: "test" }
`

// ciYMLExtensionTopLevelKey parks an anchor under an "x-"-prefixed
// top-level key with an arbitrary, otherwise-unrecognized body. The
// extension key itself must not be diagnosed, and its body must not be
// scanned.
const ciYMLExtensionTopLevelKey = `
x-defaults: &shared
  totally_made_up_field: 1
  another_one: [1, 2, 3]
checks:
  test: { call: "test" }
`

// ciYMLExtensionAnchorAliasedIntoCheck aliases an "x-"-prefixed anchor
// (whose body has an unrecognized field) into a check via a merge key.
// This pins the actual, verified behavior (#681 MAJOR 3): the merge key
// itself is skipped by the scan, and the merged-in field is not visible in
// the check entry's own Content, so it is not diagnosed -- but the merge
// still decodes correctly (the check's known fields come through).
const ciYMLExtensionAnchorAliasedIntoCheck = `
x-shared: &shared
  call: "shared"
  bogus_field: 1
checks:
  test:
    <<: *shared
`

// ciYMLDefaultsUnknownField has an unrecognized key nested inside
// defaults:.
const ciYMLDefaultsUnknownField = `
defaults:
  module: "."
  bogus: "nope"
checks:
  test: { call: "test" }
`

// ciYMLRetriesNegative has a negative retries: shorthand value.
const ciYMLRetriesNegative = `
checks:
  test: { call: "test", retries: -1 }
`

// ciYMLRetryMaxAttemptsNegative has a negative retry.max_attempts.
const ciYMLRetryMaxAttemptsNegative = `
checks:
  test:
    call: "test"
    retry:
      max_attempts: -5
`

// ciYMLRetryMultiplierNegative has a negative retry.multiplier.
const ciYMLRetryMultiplierNegative = `
checks:
  test:
    call: "test"
    retry:
      max_attempts: 3
      multiplier: -2
`

// ciYMLRetryInitialDelayNegative has a negative retry.initial_delay.
const ciYMLRetryInitialDelayNegative = `
checks:
  test:
    call: "test"
    retry:
      max_attempts: 3
      initial_delay: "-5s"
`

// ciYMLRetryStrategyBogus has an unrecognized retry.strategy value.
const ciYMLRetryStrategyBogus = `
checks:
  test:
    call: "test"
    retry:
      max_attempts: 3
      strategy: "bogus"
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

// TestCompileRetriesShorthandMapsToFixedPolicy verifies that retries: N
// compiles to the same fixed-delay policy dag.ResolveRetryPolicy's legacy
// Retries defaulting produces (#681).
func TestCompileRetriesShorthandMapsToFixedPolicy(t *testing.T) {
	def, diags := ci.CompileYAML("ci", []byte(ciYMLRetriesShorthand))
	if len(diags) != 0 {
		t.Fatalf("CompileYAML: unexpected diagnostics: %+v", diags)
	}

	// Positive: the step carries the expected fixed-delay policy.
	step := stepByID(t, def.Steps, "test")
	if step.Retry == nil {
		t.Fatal("step.Retry = nil, want a fixed-delay policy")
	}
	want := dag.RetryPolicy{
		MaxAttempts: 3, Strategy: dag.RetryFixed,
		InitialDelay: 5 * time.Second, MaxDelay: 5 * time.Second,
	}
	if *step.Retry != want {
		t.Errorf("step.Retry = %+v, want %+v", *step.Retry, want)
	}

	// Negative: a check with no retries: set has no retry policy.
	basic, diags := ci.CompileYAML("ci-basic", []byte(ciYMLBasic))
	if len(diags) != 0 {
		t.Fatalf("CompileYAML(basic): unexpected diagnostics: %+v", diags)
	}
	if noRetry := stepByID(t, basic.Steps, "test"); noRetry.Retry != nil {
		t.Errorf("step.Retry = %+v, want nil (no retries: set)", noRetry.Retry)
	}
}

// TestCompileRetryBlockMapsExactly verifies that a full retry: mapping
// compiles onto dag.RetryPolicy with every field carried through verbatim.
func TestCompileRetryBlockMapsExactly(t *testing.T) {
	def, diags := ci.CompileYAML("ci", []byte(ciYMLRetryBlock))
	if len(diags) != 0 {
		t.Fatalf("CompileYAML: unexpected diagnostics: %+v", diags)
	}

	step := stepByID(t, def.Steps, "test")
	if step.Retry == nil {
		t.Fatal("step.Retry = nil, want a policy")
	}
	want := dag.RetryPolicy{
		MaxAttempts: 5, Strategy: dag.RetryExponential,
		InitialDelay: 30 * time.Second, MaxDelay: 2 * time.Minute,
		Multiplier: 2,
	}
	if *step.Retry != want {
		t.Errorf("step.Retry = %+v, want %+v", *step.Retry, want)
	}
}

// TestCompileRetriesAndRetryBothSetIsRejected verifies that setting both
// retries: and retry: on the same check is a diagnostic.
func TestCompileRetriesAndRetryBothSetIsRejected(t *testing.T) {
	_, diags := ci.CompileYAML("ci", []byte(ciYMLRetriesAndRetryBothSet))
	if len(diags) == 0 {
		t.Fatal("CompileYAML: no diagnostics for retries+retry both set, want >=1")
	}
	found := false
	for _, d := range diags {
		if strings.Contains(d.Message, "exactly one of retries or retry") {
			found = true
		}
	}
	if !found {
		t.Errorf("diags = %+v, want one mentioning \"exactly one of retries or retry\"", diags)
	}
}

// TestParseCheckUnknownFieldIsDiagnosed reproduces the issue's exact repro:
// a check with `run:` (not a real field) and a sibling with a typo'd
// `retres:` must both be diagnosed instead of silently dropped (#681).
func TestParseCheckUnknownFieldIsDiagnosed(t *testing.T) {
	spec, diags := ci.Parse([]byte(ciYMLCheckUnknownField))

	// Positive: both unknown keys are reported.
	if len(diags) != 2 {
		t.Fatalf("diags = %+v, want exactly 2", diags)
	}
	var messages []string
	for _, d := range diags {
		messages = append(messages, d.Message)
	}
	if !strings.Contains(strings.Join(messages, "\n"), `"run"`) {
		t.Errorf("diags = %+v, want one mentioning \"run\"", diags)
	}
	if !strings.Contains(strings.Join(messages, "\n"), `"retres"`) {
		t.Errorf("diags = %+v, want one mentioning \"retres\"", diags)
	}

	// Negative: the checks still decode their known fields despite the
	// unrecognized key sitting alongside them.
	if spec.Checks["test"].Call != "test" {
		t.Errorf("spec.Checks[test].Call = %q, want \"test\"", spec.Checks["test"].Call)
	}
}

// TestParseCheckRetryUnknownFieldIsDiagnosed verifies that an unrecognized
// key nested inside a check's retry: block is also diagnosed.
func TestParseCheckRetryUnknownFieldIsDiagnosed(t *testing.T) {
	_, diags := ci.Parse([]byte(ciYMLCheckRetryUnknownField))
	if len(diags) != 1 {
		t.Fatalf("diags = %+v, want exactly 1", diags)
	}
	if diags[0].Field != "checks.test.retry" {
		t.Errorf("diags[0].Field = %q, want \"checks.test.retry\"", diags[0].Field)
	}
	if !strings.Contains(diags[0].Message, `"backoff"`) {
		t.Errorf("diags[0].Message = %q, want it to mention \"backoff\"", diags[0].Message)
	}
}

// TestParseDeployUnknownFieldIsDiagnosed verifies that deploy: gets the
// same unknown-field treatment as checks (#681).
func TestParseDeployUnknownFieldIsDiagnosed(t *testing.T) {
	_, diags := ci.Parse([]byte(ciYMLDeployUnknownField))
	if len(diags) != 1 {
		t.Fatalf("diags = %+v, want exactly 1", diags)
	}
	if diags[0].Field != "deploy" {
		t.Errorf("diags[0].Field = %q, want \"deploy\"", diags[0].Field)
	}
	if !strings.Contains(diags[0].Message, `"run"`) {
		t.Errorf("diags[0].Message = %q, want it to mention \"run\"", diags[0].Message)
	}
}

// TestParseValidSpecsHaveNoUnknownFieldFalsePositives verifies that every
// existing valid spec constant in this file still compiles clean -- the
// unknown-field scan must not misfire on known fields.
func TestParseValidSpecsHaveNoUnknownFieldFalsePositives(t *testing.T) {
	specs := []string{
		ciYMLBasic, ciYMLDeployApproval, ciYMLDeployNoApproval,
		ciYMLTaskCheck, ciYMLDeployTask, ciYMLMixedTaskAndCall,
		ciYMLRetriesShorthand, ciYMLRetryBlock,
		ciYMLMergeKeyCheck, ciYMLExtensionTopLevelKey,
	}
	for _, yml := range specs {
		if _, diags := ci.Parse([]byte(yml)); len(diags) != 0 {
			t.Errorf("Parse(%q): unexpected diagnostics: %+v", yml, diags)
		}
	}
}

// TestParseMergeKeyIsNotAnUnknownField is the regression test for #681
// BLOCKER 1: a checks entry using a YAML merge key ("<<: *anchor") must not
// be diagnosed as having an unknown "<<" field, and the merge must still
// decode -- the derived check picks up the anchor's fields.
func TestParseMergeKeyIsNotAnUnknownField(t *testing.T) {
	spec, diags := ci.Parse([]byte(ciYMLMergeKeyCheck))

	// Positive: zero diagnostics -- the merge key is not an unknown field.
	if len(diags) != 0 {
		t.Fatalf("diags = %+v, want none", diags)
	}

	// Negative: the merge actually applied -- derived picked up base's call.
	derived, ok := spec.Checks["derived"]
	if !ok {
		t.Fatal("spec.Checks[derived] missing")
	}
	if derived.Call != "base" {
		t.Errorf("derived.Call = %q, want \"base\" (merged from &b)", derived.Call)
	}
}

// TestParseTopLevelUnknownFieldIsDiagnosed verifies that a typo'd
// top-level key is diagnosed rather than silently ignored (#681 MAJOR 3).
func TestParseTopLevelUnknownFieldIsDiagnosed(t *testing.T) {
	spec, diags := ci.Parse([]byte(ciYMLTopLevelUnknownField))

	// Positive: the typo is reported.
	if len(diags) != 1 {
		t.Fatalf("diags = %+v, want exactly 1", diags)
	}
	if !strings.Contains(diags[0].Message, `"defalts"`) {
		t.Errorf("diags[0].Message = %q, want it to mention \"defalts\"", diags[0].Message)
	}

	// Negative: the valid "checks" field still decoded despite the typo.
	if _, ok := spec.Checks["test"]; !ok {
		t.Errorf("spec.Checks = %+v, want \"test\" present", spec.Checks)
	}
}

// TestParseExtensionTopLevelKeyIsIgnored verifies that an "x-"-prefixed
// top-level key is exempt from the unknown-field scan entirely -- neither
// the key itself nor its arbitrary body is diagnosed (#681 MAJOR 3).
func TestParseExtensionTopLevelKeyIsIgnored(t *testing.T) {
	_, diags := ci.Parse([]byte(ciYMLExtensionTopLevelKey))
	if len(diags) != 0 {
		t.Fatalf("diags = %+v, want none (x- keys are free-form)", diags)
	}
}

// TestParseExtensionAnchorAliasedIntoCheckPinsMergeBehavior pins the
// verified behavior of a merge key whose anchor lives under an "x-"
// extension key with an unrecognized field: the alias resolves and the
// check's known fields decode correctly, but the anchor's unrecognized
// field is not visible in the check entry's own Content and so is not
// diagnosed (#681 MAJOR 3) -- documented as a known gap rather than
// silently assumed.
func TestParseExtensionAnchorAliasedIntoCheckPinsMergeBehavior(t *testing.T) {
	spec, diags := ci.Parse([]byte(ciYMLExtensionAnchorAliasedIntoCheck))

	if len(diags) != 0 {
		t.Fatalf("diags = %+v, want none (merged-in fields are not scanned)", diags)
	}
	test, ok := spec.Checks["test"]
	if !ok {
		t.Fatal("spec.Checks[test] missing")
	}
	if test.Call != "shared" {
		t.Errorf("test.Call = %q, want \"shared\" (merged from x-shared)", test.Call)
	}
}

// TestParseDefaultsUnknownFieldIsDiagnosed verifies that defaults: gets
// the same nested unknown-field treatment as checks and deploy (#681 MAJOR 3).
func TestParseDefaultsUnknownFieldIsDiagnosed(t *testing.T) {
	_, diags := ci.Parse([]byte(ciYMLDefaultsUnknownField))
	if len(diags) != 1 {
		t.Fatalf("diags = %+v, want exactly 1", diags)
	}
	if diags[0].Field != "defaults" {
		t.Errorf("diags[0].Field = %q, want \"defaults\"", diags[0].Field)
	}
	if !strings.Contains(diags[0].Message, `"bogus"`) {
		t.Errorf("diags[0].Message = %q, want it to mention \"bogus\"", diags[0].Message)
	}
}

// TestCompileRetriesNegativeIsRejected verifies that a negative retries:
// shorthand value is a diagnostic, not silently ignored (#681 MAJOR 4).
func TestCompileRetriesNegativeIsRejected(t *testing.T) {
	_, diags := ci.CompileYAML("ci", []byte(ciYMLRetriesNegative))
	if len(diags) == 0 {
		t.Fatal("CompileYAML: no diagnostics for negative retries, want >=1")
	}
	found := false
	for _, d := range diags {
		if strings.Contains(d.Message, "retries must not be negative") {
			found = true
		}
	}
	if !found {
		t.Errorf("diags = %+v, want one mentioning \"retries must not be negative\"", diags)
	}
}

// TestCompileRetryMaxAttemptsNegativeIsRejected verifies that a negative
// retry.max_attempts is a diagnostic (#681 BLOCKER 2).
func TestCompileRetryMaxAttemptsNegativeIsRejected(t *testing.T) {
	_, diags := ci.CompileYAML("ci", []byte(ciYMLRetryMaxAttemptsNegative))
	if len(diags) == 0 {
		t.Fatal("CompileYAML: no diagnostics for negative retry.max_attempts, want >=1")
	}
	found := false
	for _, d := range diags {
		if strings.Contains(d.Message, "retry.max_attempts must not be negative") {
			found = true
		}
	}
	if !found {
		t.Errorf(
			"diags = %+v, want one mentioning \"retry.max_attempts must not be negative\"",
			diags,
		)
	}
}

// TestCompileRetryMultiplierNegativeIsRejected verifies that a negative
// retry.multiplier is a diagnostic (#681 BLOCKER 2).
func TestCompileRetryMultiplierNegativeIsRejected(t *testing.T) {
	_, diags := ci.CompileYAML("ci", []byte(ciYMLRetryMultiplierNegative))
	if len(diags) == 0 {
		t.Fatal("CompileYAML: no diagnostics for negative retry.multiplier, want >=1")
	}
	found := false
	for _, d := range diags {
		if strings.Contains(d.Message, "retry.multiplier must not be negative") {
			found = true
		}
	}
	if !found {
		t.Errorf(
			"diags = %+v, want one mentioning \"retry.multiplier must not be negative\"",
			diags,
		)
	}
}

// TestCompileRetryInitialDelayNegativeIsRejected verifies that a negative
// retry.initial_delay duration is a diagnostic (#681 MAJOR 4).
func TestCompileRetryInitialDelayNegativeIsRejected(t *testing.T) {
	_, diags := ci.CompileYAML("ci", []byte(ciYMLRetryInitialDelayNegative))
	if len(diags) == 0 {
		t.Fatal("CompileYAML: no diagnostics for negative retry.initial_delay, want >=1")
	}
	found := false
	for _, d := range diags {
		if strings.Contains(d.Message, "invalid retry.initial_delay") {
			found = true
		}
	}
	if !found {
		t.Errorf(
			"diags = %+v, want one mentioning \"invalid retry.initial_delay\"", diags,
		)
	}
}

// TestCompileRetryStrategyBogusIsRejected verifies that an unrecognized
// retry.strategy value is a diagnostic (#681 MAJOR 4).
func TestCompileRetryStrategyBogusIsRejected(t *testing.T) {
	_, diags := ci.CompileYAML("ci", []byte(ciYMLRetryStrategyBogus))
	if len(diags) == 0 {
		t.Fatal("CompileYAML: no diagnostics for bogus retry.strategy, want >=1")
	}
	found := false
	for _, d := range diags {
		if strings.Contains(d.Message, "unknown retry strategy") {
			found = true
		}
	}
	if !found {
		t.Errorf("diags = %+v, want one mentioning \"unknown retry strategy\"", diags)
	}
}
