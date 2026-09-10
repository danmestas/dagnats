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
	"fmt"
	"strings"

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

// Check declares one CI check step backed by exactly one runner: Call (a
// Dagger function name, compiled to the "dagger.call" task type) or Task
// (a plain task type, compiled verbatim for any worker that speaks the
// ordinary worker protocol). Setting both, or neither, is a compile-time
// diagnostic (#671) — see compileCheck. Needs lists check names that must
// complete before this check runs. Timeout is a Go duration string (e.g.
// "15m"). Retries is shorthand for a fixed-delay retry policy; Retry is the
// full policy. Setting both is a compile-time diagnostic (#681) — see
// compileCheckRetry.
type Check struct {
	Call    string      `yaml:"call"`
	Task    string      `yaml:"task"`
	Needs   []string    `yaml:"needs"`
	Timeout string      `yaml:"timeout"`
	Retries int         `yaml:"retries"`
	Retry   *CheckRetry `yaml:"retry"`
}

// CheckRetry is the full retry policy for a check, mapped onto
// dag.RetryPolicy by compileCheckRetry. InitialDelay and MaxDelay are Go
// duration strings, parsed the same way Check.Timeout is. Strategy is one
// of "fixed", "linear", "exponential" ("" defaults to "fixed", matching
// dag.RetryPolicy's zero value).
type CheckRetry struct {
	MaxAttempts  int     `yaml:"max_attempts"`
	Strategy     string  `yaml:"strategy"`
	InitialDelay string  `yaml:"initial_delay"`
	MaxDelay     string  `yaml:"max_delay"`
	Multiplier   float64 `yaml:"multiplier"`
}

// DeployStep declares an optional deploy stage that follows the CI checks.
// Call and Task are mutually exclusive, same as Check (#671) — see
// compileDeploy. Approval=="required" inserts a durable human-gate step
// before execution. Branches limits deployment to specific push targets
// (never PR heads).
type DeployStep struct {
	Call     string   `yaml:"call"`
	Task     string   `yaml:"task"`
	Needs    []string `yaml:"needs"`
	Approval string   `yaml:"approval"`
	Branches []string `yaml:"branches"`
	Timeout  string   `yaml:"timeout"`
}

// specKnownFields, checkKnownFields, checkRetryKnownFields,
// deployKnownFields, and defaultsKnownFields list the YAML keys each
// struct's yaml.v3 default (non-strict) Decode silently drops when
// unrecognized. unknownFieldDiagnostics (and specTopLevelDiagnostics for
// the top level) use them to turn that silent drop into a Diagnostic
// instead (#681) — a typo like "retres:" or a stale key like "run:" must
// be caught, not compiled away.
var (
	specKnownFields = map[string]bool{
		"on": true, "defaults": true, "checks": true, "deploy": true,
	}
	checkKnownFields = map[string]bool{
		"call": true, "task": true, "needs": true, "timeout": true,
		"retries": true, "retry": true,
	}
	checkRetryKnownFields = map[string]bool{
		"max_attempts": true, "strategy": true, "initial_delay": true,
		"max_delay": true, "multiplier": true,
	}
	deployKnownFields = map[string]bool{
		"call": true, "task": true, "needs": true, "approval": true,
		"branches": true, "timeout": true,
	}
	defaultsKnownFields = map[string]bool{
		"module": true, "engine": true,
	}
)

// extensionKeyPrefix marks a top-level ci.yml key as free-form
// extension/anchor space — the docker-compose/OpenAPI "x-" convention.
// specTopLevelDiagnostics never scans or diagnoses a key with this prefix,
// so ci.yml authors have a documented place to park YAML anchors (for
// `<<: *name` merge keys) without it looking like an unrecognized real
// field. This is also what stops an anchor from smuggling unknown fields
// past the scan by hiding under some other unscanned top-level key (#681
// MAJOR 3): only "x-"-prefixed keys are exempt, everything else unknown is
// diagnosed.
const extensionKeyPrefix = "x-"

// unknownFieldDiagnostics scans node's mapping keys against known and
// returns one Diagnostic per unrecognized key, positioned at the key
// itself so the ci.yml author can jump straight to the typo. field is the
// Diagnostic Field prefix ("checks.<name>", "checks.<name>.retry",
// "defaults", or "deploy"). A YAML merge key (`<<: *anchor`, Tag
// "!!merge") is always skipped: it is not a field of the struct being
// decoded, and its merged-in content is not visible in node's own Content
// (see decodeChecksField's doc comment for how that content is decoded).
func unknownFieldDiagnostics(
	node *yaml.Node, known map[string]bool, field string, diags []Diagnostic,
) []Diagnostic {
	if node == nil {
		panic("unknownFieldDiagnostics: node must not be nil")
	}
	if node.Kind != yaml.MappingNode {
		panic("unknownFieldDiagnostics: node must be a mapping node")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i]
		if key.Tag == "!!merge" || known[key.Value] {
			continue
		}
		diags = addDiagnostic(diags, Diagnostic{
			Line: key.Line, Column: key.Column,
			Field:   field,
			Message: fmt.Sprintf("%s: unknown field %q", field, key.Value),
		})
	}
	return diags
}

// specTopLevelDiagnostics scans doc's top-level mapping keys and returns
// one Diagnostic per unrecognized key, exempting merge keys and
// "x"-prefixed extension keys the same way unknownFieldDiagnostics does
// (see extensionKeyPrefix). It is separate from unknownFieldDiagnostics
// (rather than a call to it) because the extension exemption is specific
// to the top level: nothing below it (a check, a retry block, deploy) gets
// free-form extension keys.
func specTopLevelDiagnostics(doc *yaml.Node, diags []Diagnostic) []Diagnostic {
	if doc == nil {
		panic("specTopLevelDiagnostics: doc must not be nil")
	}
	if doc.Kind != yaml.MappingNode {
		panic("specTopLevelDiagnostics: doc must be a mapping node")
	}
	for i := 0; i+1 < len(doc.Content); i += 2 {
		key := doc.Content[i]
		if key.Tag == "!!merge" || specKnownFields[key.Value] ||
			strings.HasPrefix(key.Value, extensionKeyPrefix) {
			continue
		}
		diags = addDiagnostic(diags, Diagnostic{
			Line: key.Line, Column: key.Column,
			Message: fmt.Sprintf("unknown field %q", key.Value),
		})
	}
	return diags
}

// mappingValue returns the value node for key within mapping node node, or
// nil when key is absent. node must be a mapping node.
func mappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil {
		panic("mappingValue: node must not be nil")
	}
	if node.Kind != yaml.MappingNode {
		panic("mappingValue: node must be a mapping node")
	}
	for i := 0; i+1 < len(node.Content); i += 2 {
		if node.Content[i].Value == key {
			return node.Content[i+1]
		}
	}
	return nil
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
// diagnostics for the others. Unknown top-level keys are diagnosed by
// specTopLevelDiagnostics rather than ignored (#681) -- except keys under
// the "x-" extension prefix, which stay free-form and unscanned.
func decodeSpecFields(doc *yaml.Node) (Spec, []Diagnostic) {
	if doc == nil {
		panic("decodeSpecFields: doc must not be nil")
	}
	if doc.Kind != yaml.MappingNode {
		panic("decodeSpecFields: doc must be a mapping node")
	}
	var s Spec
	var diags []Diagnostic
	diags = specTopLevelDiagnostics(doc, diags)
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
	if key.Value == "deploy" && val.Kind == yaml.MappingNode {
		diags = unknownFieldDiagnostics(val, deployKnownFields, "deploy", diags)
	}
	if key.Value == "defaults" && val.Kind == yaml.MappingNode {
		diags = unknownFieldDiagnostics(val, defaultsKnownFields, "defaults", diags)
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
		// Only scan for unknown fields when the entry is itself a mapping --
		// a non-mapping entry (e.g. "b": "not-a-map") has no keys to walk and
		// already gets its own decode-failure Diagnostic below.
		if entryNode.Kind == yaml.MappingNode {
			field := "checks." + nameNode.Value
			diags = unknownFieldDiagnostics(entryNode, checkKnownFields, field, diags)
			if retryNode := mappingValue(entryNode, "retry"); retryNode != nil &&
				retryNode.Kind == yaml.MappingNode {
				diags = unknownFieldDiagnostics(
					retryNode, checkRetryKnownFields, field+".retry", diags,
				)
			}
		}
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
