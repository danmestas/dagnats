// openapi/types_test.go
//
// Methodology: pure unit tests for the document model. These types own
// no synthesis logic and no I/O — every test constructs a struct by
// hand, marshals it, and asserts the wire shape. Two assertions minimum
// per test (positive presence + negative absence) so the
// extension-merging MarshalJSON contract stays pinned without any
// dependency on the dagnats-internal synthesis package.
package openapi

import (
	"bytes"
	"encoding/json"
	"testing"
)

// TestSpecMarshalMergesExtensions asserts x-* keys on Spec.Extensions
// surface as top-level keys alongside the standard fields, and that the
// standard fields survive the merge.
func TestSpecMarshalMergesExtensions(t *testing.T) {
	s := Spec{
		OpenAPI: "3.1.0",
		Info:    Info{Title: "t", Version: "1"},
		Paths:   map[string]PathItem{},
		Extensions: map[string]interface{}{
			"x-dagnats-note": "hello",
		},
	}
	out, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if !bytes.Contains(out, []byte(`"x-dagnats-note":"hello"`)) {
		t.Fatalf("extension key not merged into Spec: %s", out)
	}
	if !bytes.Contains(out, []byte(`"openapi":"3.1.0"`)) {
		t.Fatalf("standard field lost during merge: %s", out)
	}
}

// TestSpecMarshalNoExtensionsIsClean asserts the negative space: a Spec
// with no extensions emits no `x-` keys and no "extensions" field.
func TestSpecMarshalNoExtensionsIsClean(t *testing.T) {
	s := Spec{
		OpenAPI: "3.1.0",
		Info:    Info{Title: "t", Version: "1"},
		Paths:   map[string]PathItem{},
	}
	out, err := json.Marshal(s)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if bytes.Contains(out, []byte(`"x-`)) {
		t.Fatalf("unexpected extension key on clean Spec: %s", out)
	}
	if bytes.Contains(out, []byte(`"extensions"`)) {
		t.Fatalf("Extensions field leaked into JSON: %s", out)
	}
}

// TestPathItemAndOperationMergeExtensions asserts the same merging
// contract holds on PathItem and Operation, the other two carriers of
// Extensions in the document model.
func TestPathItemAndOperationMergeExtensions(t *testing.T) {
	op := Operation{
		Responses:  map[string]Response{},
		Extensions: map[string]interface{}{"x-op": 1},
	}
	opOut, err := json.Marshal(op)
	if err != nil {
		t.Fatalf("marshal op: %v", err)
	}
	if !bytes.Contains(opOut, []byte(`"x-op":1`)) {
		t.Fatalf("operation extension not merged: %s", opOut)
	}

	pi := PathItem{Extensions: map[string]interface{}{"x-path": true}}
	piOut, err := json.Marshal(pi)
	if err != nil {
		t.Fatalf("marshal pathitem: %v", err)
	}
	if !bytes.Contains(piOut, []byte(`"x-path":true`)) {
		t.Fatalf("path item extension not merged: %s", piOut)
	}
}
