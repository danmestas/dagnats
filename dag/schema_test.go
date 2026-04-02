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

func TestValidateSchemaTypeBoolean(t *testing.T) {
	schema := json.RawMessage(`{"type":"boolean"}`)

	// Positive: boolean passes
	if err := ValidateSchema(schema,
		json.RawMessage(`true`)); err != nil {
		t.Fatalf("boolean should pass: %v", err)
	}

	// Negative: string fails
	if err := ValidateSchema(schema,
		json.RawMessage(`"yes"`)); err == nil {
		t.Fatal("string should fail boolean schema")
	}
}

func TestValidateSchemaTypeNumber(t *testing.T) {
	schema := json.RawMessage(`{"type":"number"}`)

	// Positive: number passes
	if err := ValidateSchema(schema,
		json.RawMessage(`3.14`)); err != nil {
		t.Fatalf("number should pass: %v", err)
	}

	// Negative: boolean fails
	if err := ValidateSchema(schema,
		json.RawMessage(`false`)); err == nil {
		t.Fatal("boolean should fail number schema")
	}
}

func TestValidateSchemaTypeNull(t *testing.T) {
	schema := json.RawMessage(`{"type":"null"}`)

	// Positive: null passes
	if err := ValidateSchema(schema,
		json.RawMessage(`null`)); err != nil {
		t.Fatalf("null should pass: %v", err)
	}

	// Negative: number fails
	if err := ValidateSchema(schema,
		json.RawMessage(`0`)); err == nil {
		t.Fatal("number should fail null schema")
	}
}

func TestValidateSchemaInvalidSchema(t *testing.T) {
	// Positive: invalid schema JSON returns error
	err := ValidateSchema(
		json.RawMessage(`{not json`),
		json.RawMessage(`"data"`),
	)
	if err == nil {
		t.Fatal("expected error for invalid schema JSON")
	}
	// Negative: valid schema does not error on valid data
	err = ValidateSchema(
		json.RawMessage(`{"type":"string"}`),
		json.RawMessage(`"ok"`),
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSchemaInvalidData(t *testing.T) {
	schema := json.RawMessage(`{"type":"string"}`)

	// Positive: invalid data JSON returns error
	err := ValidateSchema(schema, json.RawMessage(`{broken`))
	if err == nil {
		t.Fatal("expected error for invalid data JSON")
	}
	// Negative: valid data does not error
	err = ValidateSchema(schema, json.RawMessage(`"valid"`))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSchemaNestedObject(t *testing.T) {
	schema := json.RawMessage(`{
		"type":"object",
		"properties":{
			"inner":{"type":"object","properties":{
				"val":{"type":"number"}
			}}
		}
	}`)
	// Positive: nested object with correct types
	err := ValidateSchema(schema,
		json.RawMessage(`{"inner":{"val":42}}`))
	if err != nil {
		t.Fatalf("nested valid: %v", err)
	}
	// Negative: nested type mismatch
	err = ValidateSchema(schema,
		json.RawMessage(`{"inner":{"val":"nope"}}`))
	if err == nil {
		t.Fatal("expected error for nested type mismatch")
	}
}

func TestValidateSchemaRootPathInError(t *testing.T) {
	schema := json.RawMessage(`{"type":"string"}`)
	err := ValidateSchema(schema, json.RawMessage(`42`))
	// Positive: error message includes (root)
	if err == nil {
		t.Fatal("expected error")
	}
	got := err.Error()
	if got == "" {
		t.Fatal("error message should not be empty")
	}
	// Negative: no error for correct type
	if err2 := ValidateSchema(
		schema, json.RawMessage(`"ok"`),
	); err2 != nil {
		t.Fatalf("unexpected error: %v", err2)
	}
}
