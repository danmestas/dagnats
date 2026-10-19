// dag/validate_malformed_test.go

// Tests that Validate reports malformed workflow definitions as errors.
// Methodology: a def reaches Validate straight from user JSON (workflow
// validate, workflow register, the API), so a missing field there is bad
// input, not a programmer error. Each case decodes JSON, calls Validate
// under a recover, and asserts an error naming the problem, never a panic.
// A well-formed def of the same shape must still pass.
package dag

import (
	"encoding/json"
	"strings"
	"testing"
)

func validateJSON(t *testing.T, raw string) (err error) {
	t.Helper()
	var def WorkflowDef
	if decodeErr := json.Unmarshal([]byte(raw), &def); decodeErr != nil {
		t.Fatalf("decode def: %v", decodeErr)
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("Validate panicked on user input: %v", r)
		}
	}()
	return Validate(def)
}

func TestValidateMalformedDefsAreErrors(t *testing.T) {
	cases := map[string]struct {
		raw  string
		want string
	}{
		"no name": {`{"version":"1","steps":[{"id":"a","task":"t"}]}`, "name"},
		"map without config": {`{"name":"w","version":"1","steps":[` +
			`{"id":"src","task":"t"},` +
			`{"id":"m","type":"map","task":"t","depends_on":["src"]}]}`, "map"},
		"step without id": {`{"name":"w","version":"1","steps":[{"task":"t"}]}`, "id"},
		"task step without task": {`{"name":"w","version":"1","steps":[{"id":"a"}]}`,
			"task"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := validateJSON(t, tc.raw)
			if err == nil {
				t.Fatal("Validate accepted a malformed def")
			}
			if !strings.Contains(strings.ToLower(err.Error()), tc.want) {
				t.Fatalf("error = %q, want it to mention %q", err, tc.want)
			}
		})
	}
}

func TestValidateWellFormedMapPasses(t *testing.T) {
	err := validateJSON(t, `{"name":"w","version":"1","steps":[`+
		`{"id":"src","task":"t"},`+
		`{"id":"m","type":"map","task":"t","depends_on":["src"],`+
		`"config":{"max_items":10}}]}`)
	if err != nil {
		t.Fatalf("Validate rejected a well-formed map def: %v", err)
	}
	if err := validateJSON(t, `{"name":"w","version":"1","steps":[{"id":"a","task":"t"}]}`); err != nil {
		t.Fatalf("Validate rejected a one-step def: %v", err)
	}
}
