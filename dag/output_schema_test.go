// dag/output_schema_test.go
// Methodology: pure unit tests (no NATS) for workflow OutputSchema
// enforcement helpers (#784 item 5): register-time rejection of an
// unparseable schema in Validate, and ValidateRunOutput's choice of
// which step outputs count as the run's final output.
package dag

import (
	"encoding/json"
	"strings"
	"testing"
)

func outputSchemaDef(schema string) WorkflowDef {
	return WorkflowDef{
		Name:         "out-wf",
		Version:      "1",
		OutputSchema: json.RawMessage(schema),
		Steps: []StepDef{
			{ID: "a", Task: "t", Type: StepTypeNormal},
			{ID: "b", Task: "t", Type: StepTypeNormal, DependsOn: []string{"a"}},
		},
	}
}

func TestValidateRejectsInvalidOutputSchema(t *testing.T) {
	err := Validate(outputSchemaDef(`{"type": 5}`))
	if err == nil || !strings.Contains(err.Error(), "output_schema") {
		t.Fatalf("want output_schema error, got %v", err)
	}
	if err := Validate(outputSchemaDef(`{"type":"object"}`)); err != nil {
		t.Fatalf("valid schema rejected: %v", err)
	}
	if err := Validate(outputSchemaDef(``)); err != nil {
		t.Fatalf("absent schema rejected: %v", err)
	}
}

func TestValidateRunOutputChecksTerminalStepOnly(t *testing.T) {
	def := outputSchemaDef(`{"type":"object","required":["ok"]}`)
	steps := map[string]StepState{
		"a": {Status: StepStatusCompleted, Output: []byte(`{"x":1}`)},
		"b": {Status: StepStatusCompleted, Output: []byte(`{"ok":true}`)},
	}
	if id, err := ValidateRunOutput(def, steps); err != nil {
		t.Fatalf("matching output rejected at %q: %v", id, err)
	}
	steps["b"] = StepState{
		Status: StepStatusCompleted, Output: []byte(`{"no":1}`),
	}
	id, err := ValidateRunOutput(def, steps)
	if err == nil || id != "b" {
		t.Fatalf("want violation at b, got id=%q err=%v", id, err)
	}
	if !strings.Contains(err.Error(), "ok") {
		t.Fatalf("error should name the missing field: %v", err)
	}
}

func TestValidateRunOutputNoSchemaOrNoOutput(t *testing.T) {
	steps := map[string]StepState{
		"b": {Status: StepStatusCompleted, Output: []byte(`"anything"`)},
	}
	if _, err := ValidateRunOutput(outputSchemaDef(``), steps); err != nil {
		t.Fatalf("no schema must not validate: %v", err)
	}
	skipped := map[string]StepState{"b": {Status: StepStatusSkipped}}
	def := outputSchemaDef(`{"type":"object"}`)
	if _, err := ValidateRunOutput(def, skipped); err != nil {
		t.Fatalf("skipped terminal step must be ignored: %v", err)
	}
}
