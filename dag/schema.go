package dag

import (
	"encoding/json"
	"fmt"
)

// ValidateSchema validates data against a JSON Schema subset.
// Supports: type (string, number, boolean, object, array),
// required, properties (nested). Returns nil if schema is nil.
func ValidateSchema(
	schema json.RawMessage, data json.RawMessage,
) error {
	if schema == nil {
		return nil
	}
	var s schemaNode
	if err := json.Unmarshal(schema, &s); err != nil {
		return fmt.Errorf("invalid schema: %w", err)
	}
	var value interface{}
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("invalid data: %w", err)
	}
	return validateNode(s, value, "")
}

type schemaNode struct {
	Type       string                `json:"type"`
	Required   []string              `json:"required"`
	Properties map[string]schemaNode `json:"properties"`
}

// validateNode checks a value against a schema node at the given path.
// Recursion is bounded by schema depth (typically <20 levels).
func validateNode(
	s schemaNode, value interface{}, path string,
) error {
	if s.Type != "" {
		if err := checkType(s.Type, value, path); err != nil {
			return err
		}
	}
	obj, isObj := value.(map[string]interface{})
	if isObj && len(s.Required) > 0 {
		for _, key := range s.Required {
			if _, exists := obj[key]; !exists {
				return fmt.Errorf(
					"%s: missing required field %q", path, key,
				)
			}
		}
	}
	if isObj && len(s.Properties) > 0 {
		for key, propSchema := range s.Properties {
			val, exists := obj[key]
			if !exists {
				continue
			}
			childPath := path + "." + key
			if path == "" {
				childPath = key
			}
			if err := validateNode(
				propSchema, val, childPath,
			); err != nil {
				return err
			}
		}
	}
	return nil
}

func checkType(
	expected string, value interface{}, path string,
) error {
	actual := jsonType(value)
	if actual != expected {
		if path == "" {
			path = "(root)"
		}
		return fmt.Errorf(
			"%s: expected type %q, got %q", path, expected, actual,
		)
	}
	return nil
}

func jsonType(v interface{}) string {
	switch v.(type) {
	case string:
		return "string"
	case float64:
		return "number"
	case bool:
		return "boolean"
	case map[string]interface{}:
		return "object"
	case []interface{}:
		return "array"
	case nil:
		return "null"
	default:
		return "unknown"
	}
}
