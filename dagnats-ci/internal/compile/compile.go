package compile

import (
	"fmt"
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

// Compile converts a parsed Spec into a dag.WorkflowDef ready for the DagNats
// engine. name becomes WorkflowDef.Name. Unknown Needs references and an empty
// spec (no checks, no deploy) are rejected before building the DAG.
// The returned WorkflowDef has already passed dag.Validate.
func Compile(name string, s Spec) (dag.WorkflowDef, error) {
	if len(s.Checks) == 0 && s.Deploy == nil {
		return dag.WorkflowDef{}, fmt.Errorf(
			"compile %q: spec has no checks and no deploy step", name,
		)
	}
	module := s.Defaults.Module
	if module == "" {
		module = defaultModule
	}
	known := knownCheckIDs(s)
	steps, err := buildSteps(s, module, known)
	if err != nil {
		return dag.WorkflowDef{}, fmt.Errorf("compile %q: %w", name, err)
	}
	def := dag.WorkflowDef{
		Name:    name,
		Version: workflowVersion,
		Timeout: workflowTimeout,
		Steps:   steps,
	}
	if err := dag.Validate(def); err != nil {
		return dag.WorkflowDef{}, fmt.Errorf("compile %q: validation: %w", name, err)
	}
	return def, nil
}

// knownCheckIDs returns a set of every check name declared in the spec.
// Only check names are valid targets in Needs lists — deploy steps are not
// referenceable as dependencies.
func knownCheckIDs(s Spec) map[string]bool {
	known := make(map[string]bool, len(s.Checks))
	for name := range s.Checks {
		known[name] = true
	}
	return known
}

// validateNeeds rejects any Needs reference in a check or deploy step that does
// not resolve to a known check name. Catching this before dag.Validate means
// the engine cycle-checker sees only coherent dependency graphs.
func validateNeeds(s Spec, known map[string]bool) error {
	for name, check := range s.Checks {
		for _, need := range check.Needs {
			if !known[need] {
				return fmt.Errorf(
					"check %q: unknown needs target %q", name, need,
				)
			}
		}
	}
	if s.Deploy == nil {
		return nil
	}
	for _, need := range s.Deploy.Needs {
		if !known[need] {
			return fmt.Errorf("deploy: unknown needs target %q", need)
		}
	}
	return nil
}

// buildSteps validates cross-references, then assembles steps in sorted
// check-name order (for deterministic output) followed by deploy steps.
func buildSteps(s Spec, module string, known map[string]bool) ([]dag.StepDef, error) {
	if err := validateNeeds(s, known); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(s.Checks))
	for n := range s.Checks {
		names = append(names, n)
	}
	sort.Strings(names)

	var steps []dag.StepDef
	for _, n := range names {
		step, err := compileCheck(n, s.Checks[n], module)
		if err != nil {
			return nil, err
		}
		steps = append(steps, step)
	}
	if s.Deploy != nil {
		deploySteps, err := compileDeploy(s.Deploy, module)
		if err != nil {
			return nil, err
		}
		steps = append(steps, deploySteps...)
	}
	return steps, nil
}

// compileCheck converts one ci.yml check entry into a dag.StepDef that
// executes a Dagger function via the "dagger.call" task type.
// module is the resolved Dagger module path (Defaults.Module or ".").
func compileCheck(name string, c Check, module string) (dag.StepDef, error) {
	timeout, err := compileTimeout(c.Timeout, defaultCheckTimeout)
	if err != nil {
		return dag.StepDef{}, fmt.Errorf("check %q: %w", name, err)
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
	}, nil
}

// compileDeploy converts the ci.yml deploy block into one or two dag.StepDefs.
// When Approval is "required", an "approve-deploy" step is inserted first so
// the engine waits for a human signal before handing off to the deploy worker.
// This is the durable-gate feature that ephemeral CI runners cannot provide.
func compileDeploy(d *DeployStep, module string) ([]dag.StepDef, error) {
	if d == nil {
		panic("compileDeploy: deploy step must not be nil")
	}
	if len(d.Branches) > 0 {
		return nil, fmt.Errorf(
			"deploy: branch gating (branches: %v) is not yet supported — requires the Phase 4"+
				" runner that emits a branch step output; remove `branches:` from deploy until then",
			d.Branches,
		)
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

	deployTimeout, err := compileTimeout(d.Timeout, defaultDeployTimeout)
	if err != nil {
		return nil, fmt.Errorf("deploy: %w", err)
	}
	deps := make([]string, len(deployDeps))
	copy(deps, deployDeps)
	steps = append(steps, dag.StepDef{
		ID:        "deploy",
		Task:      "dagger.call",
		Type:      dag.StepTypeNormal,
		Timeout:   deployTimeout,
		DependsOn: deps,
		Metadata: map[string]string{
			"module": module,
			"call":   d.Call,
		},
	})
	return steps, nil
}

// compileTimeout parses a human-readable duration string into time.Duration.
// An empty string returns defaultTimeout without error — steps need not repeat
// the common case. A zero or negative duration is rejected.
func compileTimeout(s string, defaultTimeout time.Duration) (time.Duration, error) {
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
	return d, nil
}
