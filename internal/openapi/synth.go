// openapi/synth.go
//
// OpenAPI 3.1 spec synthesis. Pure transform: takes a list of
// trigger.TriggerDef plus a workflowID → dag.WorkflowDef map, returns
// the assembled openapi.Spec. No I/O, no allocations beyond what the
// spec itself needs.
//
// Mapping rules (ADR-013 + the OpenAPI brief) — every cell here must
// round-trip back through the smoke tests:
//
//	HTTP trigger Path + Method               → paths.<path>.<method>
//	HTTPConfig.MaxBodyBytes                  → requestBody body + x-* hint
//	HTTPConfig.TimeoutMs                     → operation-level x-* hint
//	HTTPConfig.Secret (non-empty)            → securitySchemes.hmacSignature
//	HTTPConfig.IdempotencyHeader             → header parameter (optional)
//	HTTPConfig.Authentication                → securitySchemes.<name>
//	WorkflowDef.InputSchema                  → requestBody schema
//	WorkflowDef.OutputSchema                 → 200 response schema
//	Respond step Status / ContentType        → 200 / 2xx content key
//	X-Dagnats-Run-Id always-on               → header on every response
//	500/503/504/499 failure modes (ADR-013)  → shared error schema
//
// Non-HTTP triggers are skipped: ADR-013 §scope locks v1 to HTTP.
package openapi

import (
	"encoding/json"
	"net/http"
	"sort"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
)

// Build assembles an OpenAPI 3.1 spec from the supplied triggers and
// workflow definitions. The result is deterministic for a given input
// (path keys sorted by string; methods iterated in canonical order)
// so the smoke-test outputs and golden files do not flap.
//
// title and version flow into Info — typical caller passes the
// running server's product name and binary version. Empty values are
// substituted with sensible defaults so a misconfigured caller never
// emits an OpenAPI document with required Info fields missing.
func Build(
	title, version string,
	triggers []trigger.TriggerDef,
	defs map[string]dag.WorkflowDef,
) Spec {
	if defs == nil {
		panic("openapi.Build: defs map must not be nil")
	}
	if len(triggers) > buildMaxTriggers {
		panic("openapi.Build: too many triggers")
	}
	out := Spec{
		OpenAPI: "3.1.0",
		Info: Info{
			Title:   defaultIfEmpty(title, "dagnats HTTP API"),
			Version: defaultIfEmpty(version, "0.0.0"),
			Description: "Auto-generated from registered HTTP-trigger " +
				"workflows. Synthesised on demand at request time.",
		},
		Paths: make(map[string]PathItem),
	}
	comps := newComponents()
	httpCount := buildPaths(&out, comps, triggers, defs)
	if httpCount == 0 {
		// No HTTP triggers — emit an empty paths object but still
		// include the shared error schema so a downstream client
		// generator can target it later without a re-fetch.
	}
	out.Components = comps.collapse()
	return out
}

// buildMaxTriggers caps the input slice length. Workflows beyond this
// cap are vanishingly rare and the trigger service refuses to load
// past it anyway — restating the cap here lets the synthesiser keep
// every loop bounded per TigerStyle.
const buildMaxTriggers = 1000

// defaultIfEmpty returns s unless s is empty, in which case fallback
// substitutes. Avoids zero-value Info fields in the rendered spec.
func defaultIfEmpty(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// buildState carries the in-progress Components block so each helper
// can register a security scheme or shared schema without threading
// the same struct through every signature.
type buildState struct {
	schemes map[string]SecurityScheme
	schemas map[string]json.RawMessage
}

func newComponents() *buildState {
	return &buildState{
		schemes: make(map[string]SecurityScheme),
		schemas: make(map[string]json.RawMessage),
	}
}

// collapse returns the Components struct or nil when nothing was
// registered (drops an empty `components: {}` key from the JSON).
func (s *buildState) collapse() *Components {
	if len(s.schemes) == 0 && len(s.schemas) == 0 {
		return nil
	}
	return &Components{
		Schemas:         s.schemas,
		SecuritySchemes: s.schemes,
	}
}

// buildPaths walks the HTTP triggers in deterministic order and
// adds one operation per (path, method). Returns the count of HTTP
// triggers actually folded in.
func buildPaths(
	spec *Spec, comps *buildState,
	triggers []trigger.TriggerDef,
	defs map[string]dag.WorkflowDef,
) int {
	if spec == nil {
		panic("buildPaths: spec must not be nil")
	}
	if comps == nil {
		panic("buildPaths: comps must not be nil")
	}
	httpDefs := filterHTTPTriggers(triggers)
	sortHTTPTriggers(httpDefs)
	registerErrorSchema(comps)
	for _, td := range httpDefs {
		def := defs[td.WorkflowID]
		op := buildOperation(td, def, comps)
		path := td.HTTP.Path
		item := spec.Paths[path]
		assignOperation(&item, td.HTTP.Method, op)
		spec.Paths[path] = item
	}
	return len(httpDefs)
}

// filterHTTPTriggers retains only triggers that:
//   - have a non-nil HTTPConfig (other trigger kinds are out of scope)
//   - are enabled (disabled triggers must not appear in the spec —
//     clients would call them and get 404s)
func filterHTTPTriggers(
	triggers []trigger.TriggerDef,
) []trigger.TriggerDef {
	out := make([]trigger.TriggerDef, 0, len(triggers))
	for _, t := range triggers {
		if t.HTTP == nil {
			continue
		}
		if !t.Enabled {
			continue
		}
		out = append(out, t)
	}
	return out
}

// sortHTTPTriggers orders triggers by (path, method) so spec output
// is byte-stable across rebuilds and golden tests do not flap.
func sortHTTPTriggers(in []trigger.TriggerDef) {
	if len(in) > buildMaxTriggers {
		panic("sortHTTPTriggers: count exceeds cap")
	}
	sort.SliceStable(in, func(i, j int) bool {
		a, b := in[i].HTTP, in[j].HTTP
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		return a.Method < b.Method
	})
}

// assignOperation places op on the path item at the right method
// slot. Panics on unsupported methods — those are caught earlier by
// HTTPConfig.Validate's allowed-method set.
func assignOperation(item *PathItem, method string, op *Operation) {
	if item == nil {
		panic("assignOperation: item must not be nil")
	}
	if op == nil {
		panic("assignOperation: op must not be nil")
	}
	switch method {
	case http.MethodGet:
		item.Get = op
	case http.MethodPost:
		item.Post = op
	case http.MethodPut:
		item.Put = op
	case http.MethodPatch:
		item.Patch = op
	case http.MethodDelete:
		item.Delete = op
	default:
		panic("assignOperation: unsupported method " + method)
	}
}
