package ci

// Diagnostic reports one problem found while parsing or compiling a ci.yml
// spec. Line and Column are 1-based YAML source positions (0 when the
// diagnostic has no source position, e.g. one raised inside Compile after
// the original bytes are no longer available — see Compile's doc comment).
// Field names the offending ci.yml field or check/deploy step, when known.
type Diagnostic struct {
	Line    int    `json:"line,omitempty"`
	Column  int    `json:"column,omitempty"`
	Field   string `json:"field,omitempty"`
	Message string `json:"message"`
}

// DiagnosticsMax bounds how many diagnostics Parse/Compile will accumulate
// before giving up and reporting a single "too many errors" sentinel. A
// pathological or generated spec with thousands of independent errors must
// not turn a single compile request into an unbounded response body.
const DiagnosticsMax = 100

// addDiagnostic appends d to diags, respecting DiagnosticsMax. Once the cap
// is reached it appends one terminal sentinel diagnostic and then silently
// drops every further diagnostic — the caller already has enough signal to
// fix the worst offenders and re-run.
func addDiagnostic(diags []Diagnostic, d Diagnostic) []Diagnostic {
	if len(diags) > DiagnosticsMax+1 {
		panic("addDiagnostic: internal invariant: diags already exceeds the capped length")
	}
	if len(diags) >= DiagnosticsMax {
		return diags
	}
	diags = append(diags, d)
	if len(diags) == DiagnosticsMax {
		diags = append(diags, Diagnostic{
			Message: "too many errors: stopped accumulating diagnostics " +
				"after reaching the maximum",
		})
	}
	if len(diags) > DiagnosticsMax+1 {
		panic("addDiagnostic: internal invariant: diags grew past the capped length")
	}
	return diags
}
