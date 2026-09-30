package ci

import (
	"encoding/json"

	"github.com/danmestas/dagnats/dag"
	"gopkg.in/yaml.v3"
)

// CompileOptions carries the caller-supplied inputs a ci.yml cannot hold.
// TaskNamespace is required by a jobs: spec: every job compiles to task
// "<TaskNamespace>.job" in worker group TaskNamespace. A checks: spec
// ignores it.
type CompileOptions struct {
	TaskNamespace string
}

// CompileResult is a successful compile: the workflow, plus, for a jobs:
// spec, the spec's on: block normalised to JSON (event name to filter
// object) and otherwise uninterpreted. On is nil for a checks: spec and
// whenever the compile reported diagnostics.
type CompileResult struct {
	Workflow dag.WorkflowDef
	On       json.RawMessage
}

// CompileYAMLWith compiles ci.yml bytes of either shape. A spec with a
// top-level jobs: key is the Actions-shaped spec; anything else compiles
// exactly as CompileYAML always has.
func CompileYAMLWith(
	name string, spec []byte, opts CompileOptions,
) (CompileResult, []Diagnostic) {
	if name == "" {
		panic("CompileYAMLWith: name must not be empty")
	}
	if spec == nil {
		panic("CompileYAMLWith: spec must not be nil")
	}
	var root yaml.Node
	if err := yaml.Unmarshal(spec, &root); err == nil && len(root.Content) > 0 {
		doc := root.Content[0]
		if doc.Kind == yaml.MappingNode && mappingKey(doc, "jobs") != nil {
			return compileJobsDoc(name, doc, opts.TaskNamespace)
		}
	}
	s, diags := Parse(spec)
	if len(diags) > 0 {
		return CompileResult{}, diags
	}
	def, diags := Compile(name, s)
	return CompileResult{Workflow: def}, diags
}
