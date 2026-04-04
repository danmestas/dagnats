package dag

import (
	"encoding/json"
	"reflect"
)

// WithSchemas generates JSON schemas from Go types I (input) and O
// (output) and attaches them to the WorkflowDef. Applied after Build().
// Supports flat structs with primitive fields, slices, and maps.
func WithSchemas[I, O any](def WorkflowDef) WorkflowDef {
	def.InputSchema = schemaFromType[I]()
	def.OutputSchema = schemaFromType[O]()
	return def
}

// schemaFromType generates a JSON Schema from a Go type using
// reflection. Supports: structs (flat), primitives (string, int,
// float, bool), slices, and maps. Uses json struct tags for field
// names.
func schemaFromType[T any]() json.RawMessage {
	var zero T
	t := reflect.TypeOf(zero)
	if t == nil {
		// interface{} / any — accept anything
		return json.RawMessage(`{}`)
	}
	schema := typeToSchema(t)
	data, err := json.Marshal(schema)
	if err != nil {
		panic("schemaFromType: marshal failed: " + err.Error())
	}
	return data
}

// typeToSchema converts a reflect.Type to a JSON Schema map.
func typeToSchema(t reflect.Type) map[string]any {
	// Unwrap pointer types
	for t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	switch t.Kind() {
	case reflect.String:
		return map[string]any{"type": "string"}
	case reflect.Bool:
		return map[string]any{"type": "boolean"}
	case reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64:
		// JSON has no integer type — all numbers are floats.
		// Use "number" for compatibility with ValidateSchema.
		return map[string]any{"type": "number"}
	case reflect.Float32, reflect.Float64:
		return map[string]any{"type": "number"}
	case reflect.Slice, reflect.Array:
		return map[string]any{
			"type":  "array",
			"items": typeToSchema(t.Elem()),
		}
	case reflect.Map:
		return map[string]any{"type": "object"}
	case reflect.Struct:
		return structToSchema(t)
	default:
		return map[string]any{}
	}
}

// structToSchema generates a JSON Schema object from a struct type.
// Uses json struct tags for field names, skips unexported fields and
// fields with json:"-".
func structToSchema(t reflect.Type) map[string]any {
	if t.Kind() != reflect.Struct {
		panic("structToSchema: expected struct, got " + t.Kind().String())
	}
	props := make(map[string]any, t.NumField())
	var required []string
	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}
		name := field.Tag.Get("json")
		if name == "-" {
			continue
		}
		// Handle "name,omitempty" tag format
		optional := false
		for j := range len(name) {
			if name[j] == ',' {
				if j+1 < len(name) {
					optional = name[j+1:] == "omitempty"
				}
				name = name[:j]
				break
			}
		}
		if name == "" {
			name = field.Name
		}
		props[name] = typeToSchema(field.Type)
		if !optional {
			required = append(required, name)
		}
	}
	schema := map[string]any{
		"type":       "object",
		"properties": props,
	}
	if len(required) > 0 {
		schema["required"] = required
	}
	return schema
}
