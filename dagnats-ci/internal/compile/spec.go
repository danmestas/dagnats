// Package compile parses .dagnats/ci.yml CI specs and compiles them into
// dag.WorkflowDef instances ready for submission to a DagNats engine.
// Keeping YAML parsing separate from compilation keeps each concern small and
// independently testable without touching the DAG library.
package compile

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Spec is the parsed form of a .dagnats/ci.yml file. The On block records
// which GitHub events trigger CI; Checks and Deploy describe what to run.
type Spec struct {
	On       On               `yaml:"on"`
	Defaults Defaults         `yaml:"defaults"`
	Checks   map[string]Check `yaml:"checks"`
	Deploy   *DeployStep      `yaml:"deploy"`
}

// On describes which GitHub events trigger the CI run.
type On struct {
	PullRequest *PullRequest `yaml:"pull_request"`
	Push        *Push        `yaml:"push"`
	Schedule    *Schedule    `yaml:"schedule"`
}

// PullRequest restricts CI runs to the listed target branches.
type PullRequest struct {
	Branches []string `yaml:"branches"`
}

// Push restricts CI runs to the listed push target branches.
type Push struct {
	Branches []string `yaml:"branches"`
}

// Schedule triggers CI on a cron expression, routed through a DagNats cron trigger.
// This is a DagNats differentiator — ephemeral CI runners have no cron primitive.
type Schedule struct {
	Cron string `yaml:"cron"`
}

// Defaults carry workflow-wide settings inherited by every step. Module is the
// Dagger module path in the repository (usually "."). Engine is advisory only
// in Phase 1; workers provision Dagger themselves.
type Defaults struct {
	Module string `yaml:"module"`
	Engine string `yaml:"engine"`
}

// Check declares one CI check step backed by a Dagger function call.
// Call is the Dagger function name. Needs lists check names that must
// complete before this check runs. Timeout is a Go duration string (e.g. "15m").
type Check struct {
	Call    string   `yaml:"call"`
	Needs   []string `yaml:"needs"`
	Timeout string   `yaml:"timeout"`
}

// DeployStep declares an optional deploy stage that follows the CI checks.
// Approval=="required" inserts a durable human-gate step before execution.
// Branches limits deployment to specific push targets (never PR heads).
type DeployStep struct {
	Call     string   `yaml:"call"`
	Needs    []string `yaml:"needs"`
	Approval string   `yaml:"approval"`
	Branches []string `yaml:"branches"`
	Timeout  string   `yaml:"timeout"`
}

// ParseSpec decodes YAML bytes into a Spec. The returned error message is
// user-facing — it surfaces the yaml.v3 parse position so authors can locate
// the problem in their ci.yml without a stack trace.
func ParseSpec(data []byte) (Spec, error) {
	var s Spec
	if err := yaml.Unmarshal(data, &s); err != nil {
		return Spec{}, fmt.Errorf("parse ci.yml: %w", err)
	}
	return s, nil
}
