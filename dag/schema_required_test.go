// dag/schema_required_test.go
// Methodology: pure unit test pinning docs/workflow-schema.json to what
// Validate actually enforces for top-level fields. version is a free-form
// label (registry keys use the content hash), so Validate accepts a def
// without it and the schema must not list it as required (#784).
package dag

import (
	"encoding/json"
	"os"
	"testing"
)

func TestSchemaRequiredMatchesValidate(t *testing.T) {
	raw, err := os.ReadFile("../docs/workflow-schema.json")
	if err != nil {
		t.Fatalf("read schema: %v", err)
	}
	var schema struct {
		Required []string `json:"required"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("parse schema: %v", err)
	}
	for _, field := range schema.Required {
		if field == "version" {
			t.Error("schema requires version but Validate does not enforce it")
		}
	}
	if len(schema.Required) == 0 {
		t.Fatal("schema has no required fields; test is not reading it")
	}
	def := WorkflowDef{
		Name:  "no-version",
		Steps: []StepDef{{ID: "a", Task: "t"}},
	}
	if err := Validate(def); err != nil {
		t.Fatalf("def without version must validate: %v", err)
	}
	def.Name = ""
	if err := Validate(def); err == nil {
		t.Fatal("def without name must still be rejected")
	}
}
