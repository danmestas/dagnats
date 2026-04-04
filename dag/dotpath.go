package dag

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ExtractDotPath extracts a value from JSON data using a dot-separated path.
// The path walks nested objects: "data.user.id" accesses data["user"]["id"].
// Returns the raw value (string, number, bool, map, array, or nil).
// Panics if path is empty (programmer error); returns error for missing keys.
func ExtractDotPath(path string, data []byte) (any, error) {
	if path == "" {
		panic("ExtractDotPath: path must not be empty")
	}
	if data == nil {
		panic("ExtractDotPath: data must not be nil")
	}
	if len(data) == 0 {
		return nil, fmt.Errorf("empty data")
	}

	parts := strings.Split(path, ".")
	var current any
	if err := json.Unmarshal(data, &current); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	for _, part := range parts {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, fmt.Errorf(
				"path %q: expected object at %q, got %T",
				path, part, current,
			)
		}
		current, ok = obj[part]
		if !ok {
			return nil, fmt.Errorf("path %q: key %q not found", path, part)
		}
	}
	return current, nil
}
