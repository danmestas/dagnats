// dag/validate_respond_test.go
//
// Methodology: pure unit tests, no NATS. Each test builds a small
// WorkflowDef and asserts the warning kinds returned by
// ValidateRespondReachability. Two assertions per test: presence (or
// absence) of the expected warning plus a sanity check on the rest of
// the warning list.
package dag

import (
	"encoding/json"
	"testing"
)

// hasWarning is a small helper for kind-membership checks; lets the
// existing tests stay declarative now that the validator may emit
// missing_schemas alongside missing_respond / duplicate_respond.
func hasWarning(ws []Warning, kind string) bool {
	for _, w := range ws {
		if w.Kind == kind {
			return true
		}
	}
	return false
}

func mustMarshal(t *testing.T, v any) json.RawMessage {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return raw
}

func TestValidateRespondNonHTTPTriggerNoWarning(t *testing.T) {
	def := WorkflowDef{
		Name: "non-http", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
		},
	}
	got := ValidateRespondReachability(def, false)
	if len(got) != 0 {
		t.Fatalf("non-http trigger should produce no warnings: %v", got)
	}
}

func TestValidateRespondNonHTTPWithRespondStepNoWarning(t *testing.T) {
	def := WorkflowDef{
		Name: "non-http-respond", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID: "r", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
		},
	}
	got := ValidateRespondReachability(def, false)
	if len(got) != 0 {
		t.Fatalf("respond is legal everywhere: %v", got)
	}
}

func TestValidateRespondHTTPTriggerWithReachableRespond(t *testing.T) {
	def := WorkflowDef{
		Name: "http-ok", Version: "1",
		InputSchema:  json.RawMessage(`{"type":"object"}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID: "r", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if len(got) != 0 {
		t.Fatalf("single reachable respond — no warnings: %v", got)
	}
}

func TestValidateRespondHTTPTriggerMissingRespond(t *testing.T) {
	def := WorkflowDef{
		Name: "http-no-respond", Version: "1",
		InputSchema:  json.RawMessage(`{"type":"object"}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID: "b", Task: "task-b",
				DependsOn: []string{"a"},
				Type:      StepTypeNormal,
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if !hasWarning(got, WarnMissingRespond) {
		t.Fatalf("want WarnMissingRespond, got %v", got)
	}
	if hasWarning(got, WarnMissingSchemas) {
		t.Fatalf("schemas set — should not warn: %v", got)
	}
}

func TestValidateRespondHTTPTriggerDuplicateRespond(t *testing.T) {
	// Two respond steps with no mutually-exclusive gating: both will
	// fire on the same execution. ADR-013 names this duplicate_respond.
	def := WorkflowDef{
		Name: "dup", Version: "1",
		InputSchema:  json.RawMessage(`{"type":"object"}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID: "r1", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
			{
				ID: "r2", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if !hasWarning(got, WarnDuplicateRespond) {
		t.Fatalf("want WarnDuplicateRespond, got %v", got)
	}
	if hasWarning(got, WarnMissingSchemas) {
		t.Fatalf("schemas set — should not warn: %v", got)
	}
}

func TestValidateRespondHTTPTriggerBranchPerOutcomeNoWarning(t *testing.T) {
	// happy and error branches each have their own respond. The two
	// respond steps are mutually exclusive because each is gated on an
	// opposite SkipIf condition over the same upstream's output.
	def := WorkflowDef{
		Name: "branched", Version: "1",
		InputSchema:  json.RawMessage(`{"type":"object"}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID:        "happy",
				Task:      "task-happy",
				DependsOn: []string{"a"},
				Type:      StepTypeNormal,
				SkipIf: &ParentCond{
					StepID: "a",
					Field:  "ok",
					Op:     "==",
					Value:  false,
				},
			},
			{
				ID:        "err",
				Task:      "task-err",
				DependsOn: []string{"a"},
				Type:      StepTypeNormal,
				SkipIf: &ParentCond{
					StepID: "a",
					Field:  "ok",
					Op:     "==",
					Value:  true,
				},
			},
			{
				ID: "r-ok", Type: StepTypeRespond,
				DependsOn: []string{"happy"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
			{
				ID: "r-err", Type: StepTypeRespond,
				DependsOn: []string{"err"},
				Config:    mustMarshal(t, RespondConfig{Status: 500}),
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if len(got) != 0 {
		t.Fatalf(
			"branch-per-outcome should produce no warnings: %v", got,
		)
	}
}

func TestValidateRespondHTTPTriggerNoSteps(t *testing.T) {
	def := WorkflowDef{
		Name: "empty", Version: "1",
		InputSchema:  json.RawMessage(`{"type":"object"}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Steps:        []StepDef{},
	}
	got := ValidateRespondReachability(def, true)
	if !hasWarning(got, WarnMissingRespond) {
		t.Fatalf("want WarnMissingRespond, got %v", got)
	}
	if hasWarning(got, WarnMissingSchemas) {
		t.Fatalf("schemas set — should not warn: %v", got)
	}
}

func TestValidateRespondHTTPTriggerOneOfThreeRespondsBranched(t *testing.T) {
	// Two responds gated by mutually-exclusive SkipIf, plus a third
	// respond that's reachable on every path. The bare respond fires
	// simultaneously with whichever branch wins → duplicate_respond.
	def := WorkflowDef{
		Name: "mixed", Version: "1",
		InputSchema:  json.RawMessage(`{"type":"object"}`),
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID:        "happy",
				Task:      "task-happy",
				DependsOn: []string{"a"},
				Type:      StepTypeNormal,
				SkipIf: &ParentCond{
					StepID: "a",
					Field:  "ok",
					Op:     "==",
					Value:  false,
				},
			},
			{
				ID:        "err",
				Task:      "task-err",
				DependsOn: []string{"a"},
				Type:      StepTypeNormal,
				SkipIf: &ParentCond{
					StepID: "a",
					Field:  "ok",
					Op:     "==",
					Value:  true,
				},
			},
			{
				ID: "r-ok", Type: StepTypeRespond,
				DependsOn: []string{"happy"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
			{
				ID: "r-err", Type: StepTypeRespond,
				DependsOn: []string{"err"},
				Config:    mustMarshal(t, RespondConfig{Status: 500}),
			},
			{
				ID: "r-other", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 202}),
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if !hasWarning(got, WarnDuplicateRespond) {
		t.Fatalf("want WarnDuplicateRespond, got %v", got)
	}
	if hasWarning(got, WarnMissingSchemas) {
		t.Fatalf("schemas set — should not warn: %v", got)
	}
}

func TestValidateRespondHTTPMissingBothSchemas(t *testing.T) {
	def := WorkflowDef{
		Name: "no-schemas", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID: "r", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if !hasWarning(got, WarnMissingSchemas) {
		t.Fatalf("want WarnMissingSchemas, got %v", got)
	}
	if hasWarning(got, WarnMissingRespond) {
		t.Fatalf("respond is present — should not warn: %v", got)
	}
}

func TestValidateRespondHTTPMissingInputSchemaOnly(t *testing.T) {
	def := WorkflowDef{
		Name:         "no-input",
		Version:      "1",
		OutputSchema: json.RawMessage(`{"type":"object"}`),
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
			{
				ID: "r", Type: StepTypeRespond,
				DependsOn: []string{"a"},
				Config:    mustMarshal(t, RespondConfig{Status: 200}),
			},
		},
	}
	got := ValidateRespondReachability(def, true)
	if !hasWarning(got, WarnMissingSchemas) {
		t.Fatalf("want WarnMissingSchemas, got %v", got)
	}
	var msg string
	for _, w := range got {
		if w.Kind == WarnMissingSchemas {
			msg = w.Message
		}
	}
	if msg == "" || !respondTestContains(msg, "input_schema") {
		t.Fatalf("message must mention input_schema, got %q", msg)
	}
}

func TestValidateRespondNonHTTPNoSchemasNoWarning(t *testing.T) {
	def := WorkflowDef{
		Name: "non-http-noschemas", Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "task-a", Type: StepTypeNormal},
		},
	}
	got := ValidateRespondReachability(def, false)
	if hasWarning(got, WarnMissingSchemas) {
		t.Fatalf(
			"non-http workflow has no OpenAPI surface; should not warn: %v",
			got,
		)
	}
	if len(got) != 0 {
		t.Fatalf("expected no warnings, got %v", got)
	}
}

// respondTestContains is a tiny helper to avoid importing "strings"
// just for the substring check above.
func respondTestContains(s, sub string) bool {
	if len(sub) > len(s) {
		return false
	}
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
