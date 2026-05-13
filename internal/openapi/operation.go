// openapi/operation.go
//
// Per-operation assembly. Splits out of synth.go so each function
// stays inside the 70-line TigerStyle budget. The functions here are
// pure — they read from the trigger/workflow inputs, write into the
// shared *buildState for components, and return the operation node.
package openapi

import (
	"encoding/json"
	"net/http"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/trigger"
)

// buildOperation produces the OpenAPI operation for one HTTP trigger.
// Side effect: writes any required security schemes / shared error
// components into comps. The returned op is ready to slot into a
// PathItem.
func buildOperation(
	td trigger.TriggerDef,
	def dag.WorkflowDef,
	comps *buildState,
) *Operation {
	if td.HTTP == nil {
		panic("buildOperation: td.HTTP must not be nil")
	}
	if comps == nil {
		panic("buildOperation: comps must not be nil")
	}
	op := &Operation{
		OperationID: operationID(td),
		Summary:     summaryFor(td, def),
		Tags:        []string{td.WorkflowID},
		Responses:   buildResponses(def),
		Extensions:  buildOpExtensions(td.HTTP),
	}
	if rb := buildRequestBody(td.HTTP, def); rb != nil {
		op.RequestBody = rb
	}
	if params := buildHeaderParams(td.HTTP); len(params) > 0 {
		op.Parameters = params
	}
	applySecurity(op, td.HTTP, comps)
	return op
}

// operationID is a stable, machine-friendly handle for client
// generators. Workflow ID + method keeps it readable; lowercase keeps
// generators that camelCase it happy.
func operationID(td trigger.TriggerDef) string {
	return td.WorkflowID + "_" + strings.ToLower(td.HTTP.Method)
}

// summaryFor produces a short human label. Falls back to a default
// "<METHOD> <PATH>" string when the workflow has no name (impossible
// in practice — Validate enforces it — but harmless if it slipped in).
func summaryFor(td trigger.TriggerDef, def dag.WorkflowDef) string {
	name := def.Name
	if name == "" {
		name = td.WorkflowID
	}
	if name == "" {
		return td.HTTP.Method + " " + td.HTTP.Path
	}
	return name
}

// buildOpExtensions captures dagnats-specific HTTP trigger metadata as
// OpenAPI extension keys (x-dagnats-*) so clients can surface them
// without re-reading the engine config.
func buildOpExtensions(
	cfg *trigger.HTTPConfig,
) map[string]interface{} {
	if cfg == nil {
		panic("buildOpExtensions: cfg must not be nil")
	}
	return map[string]interface{}{
		"x-dagnats-max-body-bytes": cfg.MaxBodyBytes,
		"x-dagnats-timeout-ms":     cfg.TimeoutMs,
	}
}

// buildRequestBody renders the requestBody object. Returns nil for
// GET and DELETE — OpenAPI 3.1 technically permits bodies on those
// verbs, but most SDK generators (openapi-typescript,
// openapi-generator-cli, swagger-codegen) treat GET/DELETE-with-body
// as malformed and either fail codegen or produce broken clients.
// Omitting requestBody keeps the spec interoperable with the
// ecosystem; callers who genuinely need GET-with-body are off the
// happy path anyway.
//
// Schema resolution order (for non-GET/DELETE):
//  1. workflow.InputSchema if present
//  2. free-form {type: object, additionalProperties: true}
//
// The fallback matches the "missing_schemas" validator warning shape.
func buildRequestBody(
	cfg *trigger.HTTPConfig, def dag.WorkflowDef,
) *RequestBody {
	if cfg == nil {
		panic("buildRequestBody: cfg must not be nil")
	}
	if cfg.Method == http.MethodGet || cfg.Method == http.MethodDelete {
		return nil
	}
	schema := def.InputSchema
	if len(schema) == 0 {
		schema = freeFormObjectSchema()
	}
	return &RequestBody{
		Required:    true,
		Description: "JSON payload — see schema",
		Content: map[string]MediaType{
			"application/json": {Schema: schema},
		},
	}
}

// buildHeaderParams emits header-parameter entries for the optional
// idempotency header (when configured) and the HMAC signature header
// (when the trigger opts into HMAC). Mirrors the runtime contract
// from internal/trigger/http.go so the spec describes the same
// surface the engine accepts.
func buildHeaderParams(
	cfg *trigger.HTTPConfig,
) []Parameter {
	if cfg == nil {
		panic("buildHeaderParams: cfg must not be nil")
	}
	var out []Parameter
	if cfg.IdempotencyHeader != "" {
		out = append(out, Parameter{
			Name:        cfg.IdempotencyHeader,
			In:          "header",
			Required:    false,
			Description: "Idempotency key — replay-safe identifier",
			Schema:      json.RawMessage(`{"type":"string"}`),
		})
	}
	return out
}

// freeFormObjectSchema is the spec brief's locked fallback when the
// workflow has no input_schema / output_schema. additionalProperties
// must be true so clients accept extra fields the workflow didn't
// declare.
func freeFormObjectSchema() json.RawMessage {
	return json.RawMessage(
		`{"type":"object","additionalProperties":true}`,
	)
}

// buildResponses produces the four locked ADR-013 failure modes plus
// the success response. Each non-2xx references the shared error
// schema in components. Every response includes the X-Dagnats-Run-Id
// header.
func buildResponses(def dag.WorkflowDef) map[string]Response {
	successSchema := def.OutputSchema
	if len(successSchema) == 0 {
		successSchema = freeFormObjectSchema()
	}
	runIDHeader := Header{
		Description: "Echoed run identifier for engine correlation",
		Schema:      json.RawMessage(`{"type":"string"}`),
	}
	headers := map[string]Header{
		"X-Dagnats-Run-Id": runIDHeader,
	}
	return map[string]Response{
		"200": {
			Description: "Workflow responded successfully",
			Headers:     headers,
			Content: map[string]MediaType{
				"application/json": {Schema: successSchema},
			},
		},
		"499": errorResponse(
			"Client closed request before workflow responded",
			headers,
		),
		"500": errorResponse(
			"Workflow failed before producing a respond step",
			headers,
		),
		"503": errorResponse(
			"Workflow was cancelled before responding",
			headers,
		),
		"504": errorResponse(
			"Workflow exceeded its HTTP trigger timeout",
			headers,
		),
	}
}

// errorResponse is a tiny helper for the four failure-mode responses.
// All four share the structural shape — separated functions only for
// the description string.
func errorResponse(
	desc string, headers map[string]Header,
) Response {
	return Response{
		Description: desc,
		Headers:     headers,
		Content: map[string]MediaType{
			"application/json": {
				Schema: json.RawMessage(
					`{"$ref":"#/components/schemas/DagnatsError"}`,
				),
			},
		},
	}
}

// registerErrorSchema adds the shared DagnatsError schema to comps,
// covering the four ADR-013 failure mode bodies. Field set is
// stable; consumers may switch on `error`.
func registerErrorSchema(comps *buildState) {
	if comps == nil {
		panic("registerErrorSchema: comps must not be nil")
	}
	if _, ok := comps.schemas["DagnatsError"]; ok {
		return
	}
	comps.schemas["DagnatsError"] = json.RawMessage(
		`{` +
			`"type":"object",` +
			`"required":["error","run_id"],` +
			`"properties":{` +
			`"error":{"type":"string"},` +
			`"run_id":{"type":"string"},` +
			`"step_id":{"type":"string"},` +
			`"message":{"type":"string"}` +
			`}}`,
	)
}

// applySecurity registers any declared security schemes on comps and
// references them on the operation's security[] list. Order: declared
// authentication first, then HMAC if Secret is set. Empty when
// neither is configured.
func applySecurity(
	op *Operation,
	cfg *trigger.HTTPConfig,
	comps *buildState,
) {
	if op == nil {
		panic("applySecurity: op must not be nil")
	}
	if cfg == nil {
		panic("applySecurity: cfg must not be nil")
	}
	var requirements []map[string][]string
	if cfg.Authentication != nil {
		name := cfg.Authentication.Name
		comps.schemes[name] = toSecurityScheme(cfg.Authentication)
		requirements = append(requirements,
			map[string][]string{name: {}})
	}
	if cfg.Secret != "" {
		const hmacName = "hmacSignature"
		comps.schemes[hmacName] = SecurityScheme{
			Type:        "apiKey",
			Name:        "X-Signature-256",
			In:          "header",
			Description: "HMAC-SHA256 signature over the raw request body",
		}
		requirements = append(requirements,
			map[string][]string{hmacName: {}})
	}
	if len(requirements) > 0 {
		op.Security = requirements
	}
}

// toSecurityScheme converts a workflow-author-declared
// HTTPAuthentication into the OpenAPI SecurityScheme shape. The
// upstream validation in trigger.HTTPAuthentication.Validate guarantees
// each field set is internally consistent — this function maps 1:1.
func toSecurityScheme(
	a *trigger.HTTPAuthentication,
) SecurityScheme {
	if a == nil {
		panic("toSecurityScheme: a must not be nil")
	}
	out := SecurityScheme{
		Type:         a.Type,
		Description:  a.Description,
		Scheme:       a.Scheme,
		BearerFormat: a.BearerFormat,
		In:           a.In,
	}
	if a.Type == "apiKey" && a.HeaderName != "" {
		out.Name = a.HeaderName
	}
	return out
}
