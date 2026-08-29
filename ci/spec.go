// Package ci parses .dagnats/ci.yml CI specs and compiles them into
// dag.WorkflowDef instances ready for submission to a DagNats engine.
//
// This package lives at the module root (not under internal/) so the
// dagnats-ci add-on module — which cannot import github.com/danmestas/dagnats's
// internal/ tree — can depend on it directly, mirroring the openapi/
// package's promotion (#614). Core DagNats stays spec-agnostic: the
// mounted /v1/ci/* control-plane endpoints (internal/api/rest_ci.go)
// are the only place ci.yml awareness enters the control plane.
package ci

import (
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

// Parse decodes YAML bytes into a Spec, accumulating a Diagnostic (rather
// than failing fast) for every field that fails to decode. It parses via
// yaml.Node first so each diagnostic carries the offending field's Line and
// Column — authors can jump straight to the problem in their ci.yml instead
// of pattern-matching a stack-trace-flavored error string.
func Parse(spec []byte) (Spec, []Diagnostic) {
	if spec == nil {
		panic("Parse: spec must not be nil")
	}
	var root yaml.Node
	if err := yaml.Unmarshal(spec, &root); err != nil {
		return Spec{}, []Diagnostic{{Message: "parse ci.yml: " + err.Error()}}
	}
	// An empty document (root.Content empty) is not itself an error here —
	// Compile rejects the resulting empty Spec with its own diagnostic, so
	// the caller sees one consistent "no checks and no deploy" message
	// instead of two different empty-input errors depending on entry point.
	if len(root.Content) == 0 {
		return Spec{}, nil
	}
	doc := root.Content[0]
	if doc.Kind != yaml.MappingNode {
		return Spec{}, []Diagnostic{{
			Line: doc.Line, Column: doc.Column,
			Message: "spec must be a YAML mapping",
		}}
	}
	result, diags := decodeSpecFields(doc)
	if len(diags) > DiagnosticsMax+1 {
		panic("Parse: internal invariant: diagnostics exceeded the capped length")
	}
	return result, diags
}

// decodeSpecFields walks the top-level mapping node's key/value pairs and
// decodes each known field independently, so one bad field does not prevent
// diagnostics for the others. Unknown top-level keys are ignored, matching
// yaml.v3's default (non-strict) unmarshal behavior.
func decodeSpecFields(doc *yaml.Node) (Spec, []Diagnostic) {
	if doc == nil {
		panic("decodeSpecFields: doc must not be nil")
	}
	if doc.Kind != yaml.MappingNode {
		panic("decodeSpecFields: doc must be a mapping node")
	}
	var s Spec
	var diags []Diagnostic
	for i := 0; i+1 < len(doc.Content); i += 2 {
		diags = decodeOneField(&s, doc.Content[i], doc.Content[i+1], diags)
	}
	return s, diags
}

// decodeOneField decodes a single top-level ci.yml field (on, defaults,
// checks, deploy) into s. A decode failure becomes a Diagnostic positioned
// at the value node so the author sees exactly where the bad field is.
// checks gets its own per-entry treatment (decodeChecksField) since it is
// a mapping of independently-authored entries, not a single struct.
func decodeOneField(
	s *Spec, key, val *yaml.Node, diags []Diagnostic,
) []Diagnostic {
	if s == nil {
		panic("decodeOneField: s must not be nil")
	}
	if key == nil || val == nil {
		panic("decodeOneField: key and val must not be nil")
	}
	if key.Value == "checks" {
		s.Checks, diags = decodeChecksField(val, diags)
		return diags
	}
	var err error
	switch key.Value {
	case "on":
		err = val.Decode(&s.On)
	case "defaults":
		err = val.Decode(&s.Defaults)
	case "deploy":
		s.Deploy = &DeployStep{}
		err = val.Decode(s.Deploy)
	default:
		return diags
	}
	if err != nil {
		diags = addDiagnostic(diags, Diagnostic{
			Line: val.Line, Column: val.Column,
			Field: key.Value, Message: err.Error(),
		})
	}
	return diags
}

// decodeChecksField decodes the checks mapping entry-by-entry (a flat loop
// bounded by the node's own Content length, not recursion) so one bad
// check reports its own Diagnostic -- Field "checks.<name>", positioned at
// that entry -- and does not discard its valid siblings. deploy does not
// need this treatment: it is a single struct, not a mapping of named
// entries, so decodeOneField's whole-field decode already reports the
// most precise position available for it.
//
// A checks value that is not itself a YAML mapping (e.g. "checks: foo")
// has no per-entry position to report, so it falls back to one
// whole-field diagnostic instead.
func decodeChecksField(
	val *yaml.Node, diags []Diagnostic,
) (map[string]Check, []Diagnostic) {
	if val == nil {
		panic("decodeChecksField: val must not be nil")
	}
	if val.Kind != yaml.MappingNode {
		diags = addDiagnostic(diags, Diagnostic{
			Line: val.Line, Column: val.Column,
			Field: "checks", Message: "checks must be a YAML mapping",
		})
		return nil, diags
	}
	checks := make(map[string]Check, len(val.Content)/2)
	for i := 0; i+1 < len(val.Content); i += 2 {
		nameNode, entryNode := val.Content[i], val.Content[i+1]
		var c Check
		if err := entryNode.Decode(&c); err != nil {
			diags = addDiagnostic(diags, Diagnostic{
				Line: entryNode.Line, Column: entryNode.Column,
				Field:   "checks." + nameNode.Value,
				Message: err.Error(),
			})
			continue
		}
		checks[nameNode.Value] = c
	}
	if len(checks) > len(val.Content)/2 {
		panic("decodeChecksField: internal invariant: checks count exceeds entry count")
	}
	return checks, diags
}
