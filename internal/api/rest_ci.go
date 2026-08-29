// api/rest_ci.go
// /v1/ci/{compile,validate} — mounted control-plane endpoints for the
// dagnats-ci add-on (#633, supersedes the API half of #631). Core DagNats
// stays spec-agnostic: POST /workflows (rest.go) only ever accepts a
// dag.WorkflowDef. These two routes are the sole place ci.yml awareness
// enters the control plane, and both are thin projections of one inner
// compileRequest helper so the register/no-register and always-200-vs-422
// differences are the only things that vary between them.
package api

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"

	"github.com/danmestas/dagnats/ci"
	"github.com/danmestas/dagnats/dag"
)

// ciSpecMaxBytes bounds how much ci.yml text a single compile/validate
// request may submit. 256 KiB is generous for a CI spec (which is a
// handful of check declarations) while keeping a malicious or buggy
// caller from streaming an unbounded body into the yaml.v3 decoder.
const ciSpecMaxBytes = 256 * 1024

// ciRequest is the shared JSON body shape for both /v1/ci routes.
// Register is only meaningful (and only read) on /v1/ci/compile.
type ciRequest struct {
	Name     string `json:"name"`
	Spec     string `json:"spec"`
	Register bool   `json:"register"`
}

// ciCompileResponse is the 200 body for POST /v1/ci/compile.
type ciCompileResponse struct {
	Workflow   dag.WorkflowDef `json:"workflow"`
	DefHash    string          `json:"def_hash"`
	Registered bool            `json:"registered"`
	Warnings   []dag.Warning   `json:"warnings,omitempty"`
}

// ciDiagnosticsResponse is the 422 body for POST /v1/ci/compile and the
// body for both branches of POST /v1/ci/validate (which is always 200).
type ciDiagnosticsResponse struct {
	Valid       bool            `json:"valid,omitempty"`
	Diagnostics []ci.Diagnostic `json:"diagnostics"`
}

// ciCompileResult is compileRequest's outcome: exactly one of Def (compiled
// successfully, Diagnostics empty) or Diagnostics (one or more problems, Def
// zero-valued) is meaningful. status is 0 when decoding/compiling ran to
// completion; otherwise it is the HTTP status the caller should write with
// no body (a malformed request, not a diagnosable spec problem).
type ciCompileResult struct {
	Def         dag.WorkflowDef
	Diagnostics []ci.Diagnostic
	Register    bool
	status      int
}

// compileRequest reads, decodes, and compiles the shared ci.yml request
// body. It never registers anything -- registration is handleCICompile's
// job, applied only after this returns a clean Def; handleCIValidate
// ignores Register entirely. Both handlers call this first and project
// its result into their own response shape.
func compileRequest(r *http.Request) ciCompileResult {
	if r == nil {
		panic("compileRequest: r must not be nil")
	}
	if r.Body == nil {
		panic("compileRequest: r.Body must not be nil")
	}
	req, status := decodeCIRequestBody(r)
	if status != 0 {
		return ciCompileResult{status: status}
	}
	def, diags := ci.CompileYAML(req.Name, []byte(req.Spec))
	return ciCompileResult{Def: def, Diagnostics: diags, Register: req.Register}
}

// decodeCIRequestBody reads the request body through a bounded
// io.LimitReader (so an oversized spec never reaches the yaml.v3 decoder)
// and decodes it into a ciRequest. Returns status == 0 on success.
func decodeCIRequestBody(r *http.Request) (ciRequest, int) {
	if r == nil {
		panic("decodeCIRequestBody: r must not be nil")
	}
	if r.Body == nil {
		panic("decodeCIRequestBody: r.Body must not be nil")
	}
	// Read one byte past the cap: if that succeeds, the real body exceeded
	// ciSpecMaxBytes and we reject it, rather than silently truncating a
	// spec (which would compile a corrupted CI pipeline).
	limited := io.LimitReader(r.Body, ciSpecMaxBytes+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return ciRequest{}, http.StatusBadRequest
	}
	if len(body) > ciSpecMaxBytes {
		return ciRequest{}, http.StatusRequestEntityTooLarge
	}
	var req ciRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return ciRequest{}, http.StatusBadRequest
	}
	if req.Name == "" {
		return ciRequest{}, http.StatusBadRequest
	}
	return req, 0
}

// handleCICompile serves POST /v1/ci/compile. On success it returns the
// compiled dag.WorkflowDef plus its DefHash; with register:true it also
// persists the workflow via RegisterWorkflowWithWarnings and returns that
// call's warnings. Diagnostics (parse or compile problems) return 422 and
// register nothing -- registration only runs after compileRequest reports
// zero diagnostics.
func handleCICompile(svc *Service, w http.ResponseWriter, r *http.Request) {
	if svc == nil {
		panic("handleCICompile: svc must not be nil")
	}
	if r == nil {
		panic("handleCICompile: r must not be nil")
	}
	result := compileRequest(r)
	if result.status != 0 {
		w.WriteHeader(result.status)
		return
	}
	if len(result.Diagnostics) > 0 {
		writeJSON(w, http.StatusUnprocessableEntity,
			ciDiagnosticsResponse{Diagnostics: result.Diagnostics})
		return
	}
	resp := ciCompileResponse{Workflow: result.Def, DefHash: dag.DefHash(result.Def)}
	if result.Register {
		warnings, err := svc.RegisterWorkflowWithWarnings(r.Context(), result.Def)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		resp.Registered = true
		resp.Warnings = warnings
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleCIValidate serves POST /v1/ci/validate. It never registers -- it
// exists purely to let an author check a ci.yml before committing it, so
// it always returns 200 with {"valid": bool, "diagnostics": [...]}.
func handleCIValidate(svc *Service, w http.ResponseWriter, r *http.Request) {
	if svc == nil {
		panic("handleCIValidate: svc must not be nil")
	}
	if r == nil {
		panic("handleCIValidate: r must not be nil")
	}
	result := compileRequest(r)
	if result.status != 0 {
		w.WriteHeader(result.status)
		return
	}
	writeJSON(w, http.StatusOK, ciDiagnosticsResponse{
		Valid:       len(result.Diagnostics) == 0,
		Diagnostics: result.Diagnostics,
	})
}

// writeJSON encodes v as the response body with the given status code.
// An encode failure is logged rather than surfaced: the status line is
// already written by the time json.Marshal could fail on a well-typed
// response struct, so there is nothing left to correct on this response.
func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	if w == nil {
		panic("writeJSON: w must not be nil")
	}
	if status < 200 || status >= 600 {
		panic("writeJSON: status out of HTTP range")
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("encode ci response", "error", err)
	}
}
