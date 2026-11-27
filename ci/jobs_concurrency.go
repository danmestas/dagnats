// ci/jobs_concurrency.go
// A jobs: spec's top-level concurrency (#1031), GitHub's
// concurrency: {group, cancel-in-progress: true}, compiles to the workflow's
// singleton in cancel mode: a newer run in a group cancels the run holding
// it, and the bridge never hands out a cancelled run's queued task, so a
// worker gets only the newest run per group.
//
// The spec has no expressions, so the group's one ${{ PATH }} is not
// evaluated: it names the run-input path the lock is keyed on, which the
// caller fills when it starts the run (input {"github":{"ref":...}} for
// ${{ github.ref }}). A ${{ github.workflow }}- prefix adds nothing, since
// every lock is already scoped to its workflow, and a group with no
// expression is one run at a time for the whole workflow.
package ci

import (
	"regexp"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"gopkg.in/yaml.v3"
)

// concurrencyGroupExpression matches a group that is one ${{ PATH }},
// optionally after ${{ github.workflow }}-.
var concurrencyGroupExpression = regexp.MustCompile(
	`^(?:\$\{\{\s*github\.workflow\s*\}\}-)?\$\{\{\s*([A-Za-z_][A-Za-z0-9_-]*(?:\.[A-Za-z_][A-Za-z0-9_-]*)*)\s*\}\}$`)

var concurrencyAllowed = []string{"group", "cancel-in-progress"}

// concurrencyGroupForms names the group forms a spec may use.
const concurrencyGroupForms = "a name, ${{ PATH }}, or ${{ github.workflow }}-${{ PATH }}"

// parseConcurrency reads the concurrency mapping into a cancel-mode
// singleton, or nil with a diagnostic for anything it cannot honour.
func parseConcurrency(node *yaml.Node, diags []Diagnostic) (*dag.SingletonConfig, []Diagnostic) {
	if node == nil {
		panic("parseConcurrency: node must not be nil")
	}
	const field = "concurrency"
	if node.Kind != yaml.MappingNode {
		return nil, addDiagnostic(diags, diagAt(node, field, "concurrency must be a mapping "+
			"with group and cancel-in-progress: true; only cancelling the older run is supported"))
	}
	diags = unknownFieldDiagnostics(node, jobsKnown(concurrencyAllowed), field, diags)
	cancel := mappingValue(node, "cancel-in-progress")
	if cancel == nil || cancel.Kind != yaml.ScalarNode || cancel.Value != "true" {
		return nil, addDiagnostic(diags, diagAt(node, field, "concurrency needs cancel-in-progress: "+
			"true; a newer run cancels the older one, and queueing it instead is not supported"))
	}
	group := mappingValue(node, "group")
	if group == nil || group.Kind != yaml.ScalarNode || strings.TrimSpace(group.Value) == "" {
		return nil, addDiagnostic(diags, diagAt(node, field,
			"concurrency needs a group: "+concurrencyGroupForms))
	}
	key, ok := concurrencyKey(group.Value)
	if !ok {
		return nil, addDiagnostic(diags, diagAt(group, field+".group",
			"concurrency group must be "+concurrencyGroupForms+"; the path names the run input it is keyed on"))
	}
	return &dag.SingletonConfig{Mode: dag.SingletonModeCancel, Key: key}, diags
}

// concurrencyKey is the singleton key for group: the input path of its
// expression, or "" (the whole workflow) for a plain name. False for any
// other expression, and for a secret, which must never key a lock.
func concurrencyKey(group string) (string, bool) {
	group = strings.TrimSpace(group)
	if !strings.Contains(group, "${{") {
		return "", true
	}
	m := concurrencyGroupExpression.FindStringSubmatch(group)
	if m == nil || m[1] == "secrets" || strings.HasPrefix(m[1], "secrets.") {
		return "", false
	}
	return m[1], true
}
