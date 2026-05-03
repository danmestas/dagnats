// internal/api/testhelpers_test.go
// Test helpers for the api package. Each fails the test on
// any error so swallowed setup-error bugs don't leave the test
// running against a broken fixture.
package api

import (
	"encoding/json"
	"testing"
)

// mustMarshal calls json.Marshal and fails the test on error.
// Use only for fixture setup in tests.
func mustMarshal(t *testing.T, v any) []byte {
	t.Helper()
	if v == nil {
		panic("mustMarshal: v must not be nil")
	}
	data, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal %T: %v", v, err)
	}
	return data
}
