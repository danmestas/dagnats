package dag

// Methodology: unit tests for the JSON Schema subset validator.
// Pure — no NATS.

import (
	"encoding/json"
	"testing"
)

func TestValidateSchemaTypeString(t *testing.T) {
	schema := json.RawMessage(`{"type":"string"}`)

	// Positive: string passes
	if err := ValidateSchema(schema,
		json.RawMessage(`"hello"`)); err != nil {
		t.Fatalf("string should pass: %v", err)
	}

	// Negative: number fails
	if err := ValidateSchema(schema,
		json.RawMessage(`42`)); err == nil {
		t.Fatalf("number should fail string schema")
	}
}

func TestValidateSchemaTypeObject(t *testing.T) {
	schema := json.RawMessage(`{
		"type": "object",
		"required": ["name"],
		"properties": {
			"name": {"type": "string"},
			"age": {"type": "number"}
		}
	}`)

	// Positive: valid object
	if err := ValidateSchema(schema,
		json.RawMessage(`{"name":"alice","age":30}`)); err != nil {
		t.Fatalf("valid object: %v", err)
	}

	// Negative: missing required field
	if err := ValidateSchema(schema,
		json.RawMessage(`{"age":30}`)); err == nil {
		t.Fatalf("missing name should fail")
	}

	// Negative: wrong type for field
	if err := ValidateSchema(schema,
		json.RawMessage(`{"name":123}`)); err == nil {
		t.Fatalf("name as number should fail")
	}
}

func TestValidateSchemaTypeArray(t *testing.T) {
	schema := json.RawMessage(`{"type":"array"}`)

	// Positive: array passes
	if err := ValidateSchema(schema,
		json.RawMessage(`[1,2,3]`)); err != nil {
		t.Fatalf("array should pass: %v", err)
	}

	// Negative: object fails
	if err := ValidateSchema(schema,
		json.RawMessage(`{}`)); err == nil {
		t.Fatalf("object should fail array schema")
	}
}

func TestValidateSchemaNilSchemaPassesAll(t *testing.T) {
	// Positive: nil schema accepts anything
	if err := ValidateSchema(nil,
		json.RawMessage(`"anything"`)); err != nil {
		t.Fatalf("nil schema should accept: %v", err)
	}
}
