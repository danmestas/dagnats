// dag/cancel_on_validate_test.go

// Tests for cancel_on validation.
// Methodology: an entry the engine cannot turn into a waiter must fail
// Validate, because registerCancelWaiters resolves every entry's Match
// against the run input when a run starts, and Match.Resolve panics on
// an empty Left or Right. Each case decodes JSON the way a registered def
// arrives, so an omitted "match" is covered as well as an empty field.
package dag

import (
	"encoding/json"
	"strings"
	"testing"
)

func cancelOnDef(t *testing.T, cancelOn string) WorkflowDef {
	t.Helper()
	raw := `{"name":"wf","version":"1","steps":[{"id":"a","task":"t"}],` +
		`"cancel_on":` + cancelOn + `}`
	var def WorkflowDef
	if err := json.Unmarshal([]byte(raw), &def); err != nil {
		t.Fatalf("decode def: %v", err)
	}
	return def
}

func TestValidateCancelOnRejectsIncompleteMatch(t *testing.T) {
	cases := map[string]struct {
		cancelOn string
		want     string
	}{
		"no match": {`[{"event":"order.cancelled"}]`, "match.left"},
		"no left": {`[{"event":"e","match":{"op":"eq","right":"input.id"}}]`,
			"match.left"},
		"no op": {`[{"event":"e","match":{"left":"id","right":"input.id"}}]`,
			"match.op"},
		"unknown op": {`[{"event":"e","match":{"left":"id","op":"ne",` +
			`"right":"input.id"}}]`, "match.op"},
		"no right": {`[{"event":"e","match":{"left":"id","op":"eq"}}]`,
			"match.right"},
		"step path": {`[{"event":"e","match":{"left":"id","op":"eq",` +
			`"right":"step.a.output.id"}}]`, "input."},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := Validate(cancelOnDef(t, tc.cancelOn))
			if err == nil {
				t.Fatal("Validate accepted an entry the engine cannot resolve")
			}
			if !strings.Contains(err.Error(), "cancel_on[0]") ||
				!strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %q, want cancel_on[0] and %q", err, tc.want)
			}
		})
	}
}

func TestValidateCancelOnAcceptsCompleteMatch(t *testing.T) {
	def := cancelOnDef(t, `[{"event":"order.cancelled",`+
		`"match":{"left":"order_id","op":"eq","right":"input.order_id"}}]`)
	if err := Validate(def); err != nil {
		t.Fatalf("Validate rejected a complete entry: %v", err)
	}
	resolved, err := def.CancelOn[0].Match.Resolve(nil, []byte(`{"order_id":"o-1"}`))
	if err != nil {
		t.Fatalf("a validated entry must resolve: %v", err)
	}
	if resolved.Right != "o-1" {
		t.Fatalf("resolved right = %v, want o-1", resolved.Right)
	}
}
