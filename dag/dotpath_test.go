// dag/dotpath_test.go

// Tests for dot-path extraction from JSON data.
// Methodology: verify that nested paths extract correct values, that missing
// keys and type mismatches return errors, and that empty inputs are rejected.
package dag

import (
	"strings"
	"testing"
)

func TestDotPathExtractSimple(t *testing.T) {
	data := []byte(`{"data":{"order_id":"ord-123"}}`)
	val, err := ExtractDotPath("data.order_id", data)
	if err != nil {
		t.Fatalf("extract: %v", err)
	}
	if val != "ord-123" {
		t.Fatalf("expected ord-123, got %v", val)
	}
}

func TestDotPathExtractNested(t *testing.T) {
	data := []byte(`{"data":{"order_id":"ord-123","nested":{"value":42}}}`)
	val, err := ExtractDotPath("data.nested.value", data)
	if err != nil {
		t.Fatalf("extract nested: %v", err)
	}
	// JSON numbers unmarshal as float64
	if val != float64(42) {
		t.Fatalf("expected 42, got %v", val)
	}
}

func TestDotPathExtractMissing(t *testing.T) {
	data := []byte(`{"data":{}}`)
	_, err := ExtractDotPath("data.nonexistent", data)
	if err == nil {
		t.Fatal("expected error for missing path")
	}
	if !strings.Contains(err.Error(), "not found") {
		t.Fatalf("error should mention not found, got: %v", err)
	}
}

func TestDotPathExtractTypeMismatch(t *testing.T) {
	data := []byte(`{"data":"string"}`)
	_, err := ExtractDotPath("data.field", data)
	if err == nil {
		t.Fatal("expected error for type mismatch")
	}
	if !strings.Contains(err.Error(), "expected object") {
		t.Fatalf("error should mention expected object, got: %v", err)
	}
}

func TestDotPathExtractTopLevel(t *testing.T) {
	data := []byte(`{"user_id":"usr-456"}`)
	val, err := ExtractDotPath("user_id", data)
	if err != nil {
		t.Fatalf("extract top-level: %v", err)
	}
	if val != "usr-456" {
		t.Fatalf("expected usr-456, got %v", val)
	}
}

func TestDotPathExtractEmptyData(t *testing.T) {
	_, err := ExtractDotPath("data.field", []byte{})
	if err == nil {
		t.Fatal("expected error for empty data")
	}
	if !strings.Contains(err.Error(), "empty") {
		t.Fatalf("error should mention empty, got: %v", err)
	}
}

func TestDotPathExtractInvalidJSON(t *testing.T) {
	data := []byte(`{invalid json}`)
	_, err := ExtractDotPath("data.field", data)
	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if !strings.Contains(err.Error(), "unmarshal") {
		t.Fatalf("error should mention unmarshal, got: %v", err)
	}
}

func TestDotPathExtractBoolean(t *testing.T) {
	data := []byte(`{"data":{"active":true}}`)
	val, err := ExtractDotPath("data.active", data)
	if err != nil {
		t.Fatalf("extract boolean: %v", err)
	}
	if val != true {
		t.Fatalf("expected true, got %v", val)
	}
}

func TestDotPathExtractArray(t *testing.T) {
	data := []byte(`{"data":{"items":[1,2,3]}}`)
	val, err := ExtractDotPath("data.items", data)
	if err != nil {
		t.Fatalf("extract array: %v", err)
	}
	arr, ok := val.([]any)
	if !ok {
		t.Fatalf("expected array, got %T", val)
	}
	if len(arr) != 3 {
		t.Fatalf("expected 3 items, got %d", len(arr))
	}
}
