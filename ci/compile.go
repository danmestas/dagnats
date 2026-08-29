package ci

import (
	"fmt"
	"regexp"
	"sort"
	"time"

	"github.com/danmestas/dagnats/dag"
)

// Default timeouts mirror the spec's examples and give reasonable bounds for
// CI pipelines. The approval default is generous to allow for human review
// across time-zones. All are overridable per-step in ci.yml.
const (
	defaultCheckTimeout    = 15 * time.Minute
	defaultApprovalTimeout = 24 * time.Hour
	defaultDeployTimeout   = 15 * time.Minute
	workflowTimeout        = 45 * time.Minute
	workflowVersion        = "1.0.0"
	defaultModule          = "." // Dagger module in the repo root
)

// stepIDPattern extracts the quoted step ID from a dag.Validate error
// message (e.g. `step "build" depends on ...`). dag.Validate returns a
// single fmt.Errorf per call, not structured data, so this is the "best
// available" way to attribute the resulting Diagnostic to a Field — see
// Compile's doc comment.
var stepIDPattern = regexp.MustCompile(`step "([^"]+)"`)

// Compile converts a parsed Spec into a dag.WorkflowDef ready for the DagNats
// engine. name becomes WorkflowDef.Name. Unlike the pre-#633 fail-fast
// compiler, every problem found (unknown Needs references, invalid
// timeouts, unsupported deploy config, dag.Validate failures) is
// accumulated into the returned Diagnostic slice instead of stopping at the
// first one, so a spec author fixes every mistake in one pass. The returned
// WorkflowDef is valid (has passed dag.Validate) only when the Diagnostic
// slice is empty.
//
// Diagnostics raised here have no YAML source position (Line/Column are 0)
// because Compile receives an already-decoded Spec, not the original bytes.
// Callers that hold the raw ci.yml text should use CompileYAML instead,
// which threads Parse's positions through for the fields it can decode.
func Compile(name string, s Spec) (dag.WorkflowDef, []Diagnostic) {
	if name == "" {
		panic("Compile: name must not be empty")
	}
	var diags []Diagnostic
	if len(s.Checks) == 0 && s.Deploy == nil {
		diags = addDiagnostic(diags, Diagnostic{
			Message: "spec has no checks and no deploy step",
		})
	}
	module := s.Defaults.Module
	if module == "" {
		module = defaultModule
	}
	known := knownCheckIDs(s)
	var steps []dag.StepDef
	steps, diags = buildSteps(s, module, known, diags)
	if len(diags) > 0 {
		return dag.WorkflowDef{}, diags
	}
	def := dag.WorkflowDef{
		Name:    name,
		Version: workflowVersion,
		Timeout: workflowTimeout,
		Steps:   steps,
	}
	if err := dag.Validate(def); err != nil {
		diags = addDiagnostic(diags, Diagnostic{
			Field:   stepFieldFromError(err),
			Message: err.Error(),
		})
		return dag.WorkflowDef{}, diags
	}
	if def.Name != name {
		panic("Compile: internal invariant: compiled def.Name does not match name")
	}
	return def, nil
}

// CompileYAML is the thin composition of Parse and Compile for callers that
// hold ci.yml bytes directly (the CLI, the /v1/ci/compile handler). Parse
// diagnostics — which do carry source positions — short-circuit before
// Compile ever runs, since a Spec that failed to decode has no meaningful
// checks/deploy to compile.
func CompileYAML(name string, spec []byte) (dag.WorkflowDef, []Diagnostic) {
	if name == "" {
		panic("CompileYAML: name must not be empty")
	}
	if spec == nil {
		panic("CompileYAML: spec must not be nil")
	}
	s, diags := Parse(spec)
	if len(diags) > 0 {
		return dag.WorkflowDef{}, diags
	}
	return Compile(name, s)
}

// stepFieldFromError extracts the offending step ID from a dag.Validate
// error message, when present, for use as a Diagnostic's Field. Returns ""
// for workflow-level errors (e.g. "workflow %q has no steps") that name no
// single step.
func stepFieldFromError(err error) string {
	if err == nil {
		panic("stepFieldFromError: err must not be nil")
	}
	m := stepIDPattern.FindStringSubmatch(err.Error())
	if len(m) != 0 && len(m) != 2 {
		panic("stepFieldFromError: internal invariant: unexpected submatch count")
	}
	if len(m) == 2 {
		return m[1]
	}
	return ""
}

// knownCheckIDs returns a set of every check name declared in the spec.
// Only check names are valid targets in Needs lists — deploy steps are not
// referenceable as dependencies.
func knownCheckIDs(s Spec) map[string]bool {
	known := make(map[string]bool, len(s.Checks))
	for name := range s.Checks {
		known[name] = true
	}
	if len(known) != len(s.Checks) {
		panic("knownCheckIDs: internal invariant: known count does not match s.Checks")
	}
	for name := range known {
		if _, ok := s.Checks[name]; !ok {
			panic("knownCheckIDs: internal invariant: known key not present in s.Checks")
		}
	}
	return known
}

// sortedCheckNames returns check names in sorted order for deterministic
// compiled step ordering.
func sortedCheckNames(s Spec) []string {
	names := make([]string, 0, len(s.Checks))
	for n := range s.Checks {
		names = append(names, n)
	}
	sort.Strings(names)
	if len(names) != len(s.Checks) {
		panic("sortedCheckNames: internal invariant: names count does not match s.Checks")
	}
	if len(names) > 1 && names[0] > names[len(names)-1] {
		panic("sortedCheckNames: internal invariant: names not sorted")
	}
	return names
}

// buildSteps compiles every check (in sorted-name order, for deterministic
// output) and the deploy block, if present, accumulating diagnostics from
// each into diags rather than stopping at the first failure.
func buildSteps(
	s Spec, module string, known map[string]bool, diags []Diagnostic,
) ([]dag.StepDef, []Diagnostic) {
	if known == nil {
		panic("buildSteps: known must not be nil")
	}
	diagCountBefore := len(diags)
	var steps []dag.StepDef
	for _, n := range sortedCheckNames(s) {
		var step dag.StepDef
		var ok bool
		step, ok, diags = compileCheck(n, s.Checks[n], module, known, diags)
		if ok {
			steps = append(steps, step)
		}
	}
	if s.Deploy != nil {
		var deploySteps []dag.StepDef
		deploySteps, diags = compileDeploy(s.Deploy, module, known, diags)
		steps = append(steps, deploySteps...)
	}
	if len(diags) < diagCountBefore {
		panic("buildSteps: internal invariant: diagnostics count decreased")
	}
	return steps, diags
}

// compileCheck converts one ci.yml check entry into a dag.StepDef that
// executes a Dagger function via the "dagger.call" task type. module is the
// resolved Dagger module path (Defaults.Module or "."). Returns ok==false
// when the check produced one or more diagnostics, in which case the
// returned StepDef is not usable.
func compileCheck(
	name string, c Check, module string, known map[string]bool,
	diags []Diagnostic,
) (dag.StepDef, bool, []Diagnostic) {
	if known == nil {
		panic("compileCheck: known must not be nil")
	}
	before := len(diags)
	for _, need := range c.Needs {
		if !known[need] {
			diags = addDiagnostic(diags, Diagnostic{
				Field: name,
				Message: fmt.Sprintf(
					"check %q: unknown needs target %q", name, need,
				),
			})
		}
	}
	timeout, err := compileTimeout(c.Timeout, defaultCheckTimeout)
	if err != nil {
		diags = addDiagnostic(diags, Diagnostic{
			Field:   name,
			Message: fmt.Sprintf("check %q: %v", name, err),
		})
	}
	if len(diags) > before {
		return dag.StepDef{}, false, diags
	}
	if len(diags) != before {
		panic("compileCheck: internal invariant: diagnostics changed on the ok path")
	}
	deps := make([]string, len(c.Needs))
	copy(deps, c.Needs)
	return dag.StepDef{
		ID:        name,
		Task:      "dagger.call",
		Type:      dag.StepTypeNormal,
		Timeout:   timeout,
		DependsOn: deps,
		Metadata: map[string]string{
			"module": module,
			"call":   c.Call,
		},
	}, true, diags
}

// compileDeploy converts the ci.yml deploy block into one or two dag.StepDefs.
// When Approval is "required", an "approve-deploy" step is inserted first so
// the engine waits for a human signal before handing off to the deploy worker.
// This is the durable-gate feature that ephemeral CI runners cannot provide.
// Branch gating (branches: set) is rejected as unsupported — see the Phase 4
// note in the emitted diagnostic — but rejection is accumulated like every
// other deploy problem rather than returned immediately.
func compileDeploy(
	d *DeployStep, module string, known map[string]bool, diags []Diagnostic,
) ([]dag.StepDef, []Diagnostic) {
	if d == nil {
		panic("compileDeploy: deploy step must not be nil")
	}
	if known == nil {
		panic("compileDeploy: known must not be nil")
	}
	before := len(diags)
	if len(d.Branches) > 0 {
		diags = addDiagnostic(diags, Diagnostic{
			Field: "deploy",
			Message: fmt.Sprintf(
				"deploy: branch gating (branches: %v) is not yet supported — "+
					"requires the Phase 4 runner that emits a branch step "+
					"output; remove `branches:` from deploy until then",
				d.Branches,
			),
		})
	}
	for _, need := range d.Needs {
		if !known[need] {
			diags = addDiagnostic(diags, Diagnostic{
				Field:   "deploy",
				Message: fmt.Sprintf("deploy: unknown needs target %q", need),
			})
		}
	}
	deployTimeout, err := compileTimeout(d.Timeout, defaultDeployTimeout)
	if err != nil {
		diags = addDiagnostic(diags, Diagnostic{
			Field:   "deploy",
			Message: fmt.Sprintf("deploy: %v", err),
		})
	}
	if len(diags) > before {
		return nil, diags
	}
	return buildDeploySteps(d, module, deployTimeout), diags
}

// buildDeploySteps assembles the deploy step(s) once compileDeploy has
// confirmed the deploy block is free of diagnostics.
func buildDeploySteps(
	d *DeployStep, module string, deployTimeout time.Duration,
) []dag.StepDef {
	if d == nil {
		panic("buildDeploySteps: deploy step must not be nil")
	}
	if deployTimeout <= 0 {
		panic("buildDeploySteps: deployTimeout must be positive")
	}
	var steps []dag.StepDef
	deployDeps := make([]string, len(d.Needs))
	copy(deployDeps, d.Needs)
	if d.Approval == "required" {
		approvalDeps := make([]string, len(d.Needs))
		copy(approvalDeps, d.Needs)
		steps = append(steps, dag.StepDef{
			ID:        "approve-deploy",
			Task:      "ci.approval",
			Type:      dag.StepTypeNormal,
			Timeout:   defaultApprovalTimeout,
			DependsOn: approvalDeps,
		})
		deployDeps = []string{"approve-deploy"}
	}
	steps = append(steps, dag.StepDef{
		ID:        "deploy",
		Task:      "dagger.call",
		Type:      dag.StepTypeNormal,
		Timeout:   deployTimeout,
		DependsOn: deployDeps,
		Metadata: map[string]string{
			"module": module,
			"call":   d.Call,
		},
	})
	return steps
}

// compileTimeout parses a human-readable duration string into time.Duration.
// An empty string returns defaultTimeout without error — steps need not repeat
// the common case. A zero or negative duration is rejected.
func compileTimeout(s string, defaultTimeout time.Duration) (time.Duration, error) {
	if defaultTimeout <= 0 {
		panic("compileTimeout: defaultTimeout must be positive")
	}
	if s == "" {
		return defaultTimeout, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid timeout %q: %w", s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("timeout %q must be positive", s)
	}
	if d <= 0 {
		panic("compileTimeout: internal invariant: non-positive duration on success path")
	}
	return d, nil
}
