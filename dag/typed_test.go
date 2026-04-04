// dag/typed_test.go
// Tests for typed workflow generics: schema generation from Go types
// and the WithSchemas convenience function. Methodology: generate
// schemas, verify JSON structure and type mappings.
package dag

import (
	"encoding/json"
	"testing"
)

type orderRequest struct {
	CustomerID string `json:"customer_id"`
	Amount     int    `json:"amount"`
	Currency   string `json:"currency"`
}

type orderResult struct {
	OrderID string `json:"order_id"`
	Status  string `json:"status"`
}

type optionalFields struct {
	Name  string `json:"name"`
	Email string `json:"email,omitempty"`
}

func TestSchemaFromTypeStruct(t *testing.T) {
	schema := schemaFromType[orderRequest]()
	if schema == nil {
		t.Fatal("schema is nil")
	}

	var parsed map[string]any
	if err := json.Unmarshal(schema, &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Positive: type is object
	if parsed["type"] != "object" {
		t.Fatalf("type = %v, want object", parsed["type"])
	}

	// Positive: has properties
	props, ok := parsed["properties"].(map[string]any)
	if !ok {
		t.Fatalf("properties missing or wrong type")
	}

	// Positive: customer_id is string
	cid, ok := props["customer_id"].(map[string]any)
	if !ok {
		t.Fatal("customer_id property missing")
	}
	if cid["type"] != "string" {
		t.Fatalf("customer_id type = %v, want string", cid["type"])
	}

	// Positive: amount is number (JSON has no integer type)
	amt, ok := props["amount"].(map[string]any)
	if !ok {
		t.Fatal("amount property missing")
	}
	if amt["type"] != "number" {
		t.Fatalf("amount type = %v, want number", amt["type"])
	}
}

func TestSchemaFromTypeRequiredFields(t *testing.T) {
	schema := schemaFromType[optionalFields]()
	var parsed map[string]any
	json.Unmarshal(schema, &parsed)

	req, ok := parsed["required"].([]any)
	if !ok {
		t.Fatal("required field missing")
	}
	// Positive: name is required (no omitempty)
	found := false
	for _, r := range req {
		if r == "name" {
			found = true
		}
		// Negative: email should not be required (omitempty)
		if r == "email" {
			t.Fatal("email should not be required (omitempty)")
		}
	}
	if !found {
		t.Fatal("name should be in required list")
	}
}

func TestSchemaFromTypePrimitive(t *testing.T) {
	schema := schemaFromType[string]()
	var parsed map[string]any
	json.Unmarshal(schema, &parsed)
	// Positive: string type
	if parsed["type"] != "string" {
		t.Fatalf("type = %v, want string", parsed["type"])
	}
}

func TestSchemaFromTypeSlice(t *testing.T) {
	schema := schemaFromType[[]string]()
	var parsed map[string]any
	json.Unmarshal(schema, &parsed)
	// Positive: array type
	if parsed["type"] != "array" {
		t.Fatalf("type = %v, want array", parsed["type"])
	}
	items, ok := parsed["items"].(map[string]any)
	if !ok {
		t.Fatal("items missing")
	}
	if items["type"] != "string" {
		t.Fatalf("items type = %v, want string", items["type"])
	}
}

func TestSchemaFromTypeBool(t *testing.T) {
	schema := schemaFromType[bool]()
	var parsed map[string]any
	json.Unmarshal(schema, &parsed)
	if parsed["type"] != "boolean" {
		t.Fatalf("type = %v, want boolean", parsed["type"])
	}
}

func TestSchemaFromTypeFloat(t *testing.T) {
	schema := schemaFromType[float64]()
	var parsed map[string]any
	json.Unmarshal(schema, &parsed)
	if parsed["type"] != "number" {
		t.Fatalf("type = %v, want number", parsed["type"])
	}
}

func TestSchemaFromTypeMap(t *testing.T) {
	schema := schemaFromType[map[string]int]()
	var parsed map[string]any
	json.Unmarshal(schema, &parsed)
	// Positive: object type for maps
	if parsed["type"] != "object" {
		t.Fatalf("type = %v, want object", parsed["type"])
	}
}

func TestWithSchemas(t *testing.T) {
	wb := NewWorkflow("typed-test")
	wb.Task("process", "process-task")
	def, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	// Negative: no schemas before WithSchemas
	if def.InputSchema != nil {
		t.Fatal("InputSchema should be nil before WithSchemas")
	}

	def = WithSchemas[orderRequest, orderResult](def)

	// Positive: InputSchema populated
	if def.InputSchema == nil {
		t.Fatal("InputSchema is nil after WithSchemas")
	}
	// Positive: OutputSchema populated
	if def.OutputSchema == nil {
		t.Fatal("OutputSchema is nil after WithSchemas")
	}

	// Positive: InputSchema has customer_id property
	var inSchema map[string]any
	json.Unmarshal(def.InputSchema, &inSchema)
	props := inSchema["properties"].(map[string]any)
	if _, ok := props["customer_id"]; !ok {
		t.Fatal("InputSchema missing customer_id property")
	}

	// Positive: OutputSchema has order_id property
	var outSchema map[string]any
	json.Unmarshal(def.OutputSchema, &outSchema)
	outProps := outSchema["properties"].(map[string]any)
	if _, ok := outProps["order_id"]; !ok {
		t.Fatal("OutputSchema missing order_id property")
	}
}

func TestWithSchemasValidatesAgainstExistingValidator(t *testing.T) {
	// Verify generated schemas work with ValidateSchema
	def := WithSchemas[orderRequest, orderResult](WorkflowDef{})

	// Positive: valid input passes
	validInput := json.RawMessage(
		`{"customer_id":"c1","amount":100,"currency":"USD"}`,
	)
	if err := ValidateSchema(def.InputSchema, validInput); err != nil {
		t.Fatalf("valid input failed: %v", err)
	}

	// Negative: wrong type fails
	badInput := json.RawMessage(
		`{"customer_id":123,"amount":100,"currency":"USD"}`,
	)
	if err := ValidateSchema(def.InputSchema, badInput); err == nil {
		t.Fatal("expected error for wrong type")
	}
}
