package dag

// Methodology: red-green TDD for DefHash (#630). DefHash must be
// deterministic across equal defs regardless of map-key insertion order
// (encoding/json sorts map keys), sensitive to any field-level change, and
// always emit a fixed-length lowercase hex digest so REST/CLI callers can
// treat it as an opaque comparison token.

import (
	"regexp"
	"testing"
)

func sampleDefForHash() WorkflowDef {
	return WorkflowDef{
		Name:    "hash-test",
		Version: "1",
		Steps: []StepDef{
			{ID: "a", Task: "noop",
				Metadata: map[string]string{"description": "first step"}},
			{ID: "b", Task: "noop", DependsOn: []string{"a"},
				Metadata: map[string]string{"description": "second step"}},
		},
		AuxSteps: map[string]bool{"a": false, "b": false},
	}
}

func TestDefHashSameDefTwiceEqual(t *testing.T) {
	def := sampleDefForHash()

	first := DefHash(def)
	second := DefHash(def)

	if first != second {
		t.Fatalf("DefHash not deterministic: %q != %q", first, second)
	}
	if first == "" {
		t.Fatal("DefHash returned empty string")
	}
}

func TestDefHashDiffersOnFieldChange(t *testing.T) {
	def := sampleDefForHash()
	changed := sampleDefForHash()
	changed.Steps[0].Metadata["description"] = "first step!"

	original := DefHash(def)
	modified := DefHash(changed)

	if original == modified {
		t.Fatal("DefHash did not change after a one-character edit")
	}
	if original == "" || modified == "" {
		t.Fatal("DefHash returned empty string")
	}
}

func TestDefHashMapInsertionOrderIrrelevant(t *testing.T) {
	defA := sampleDefForHash()
	defA.AuxSteps = map[string]bool{}
	defA.AuxSteps["a"] = false
	defA.AuxSteps["b"] = false

	defB := sampleDefForHash()
	defB.AuxSteps = map[string]bool{}
	defB.AuxSteps["b"] = false
	defB.AuxSteps["a"] = false

	hashA := DefHash(defA)
	hashB := DefHash(defB)

	if hashA != hashB {
		t.Fatalf("DefHash sensitive to map insertion order: %q != %q",
			hashA, hashB)
	}
	if hashA == "" {
		t.Fatal("DefHash returned empty string")
	}
}

var hexDigestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

func TestDefHashOutputShape(t *testing.T) {
	hash := DefHash(sampleDefForHash())

	if !hexDigestPattern.MatchString(hash) {
		t.Fatalf("DefHash output %q is not 64 lowercase hex chars", hash)
	}
	if len(hash) != 64 {
		t.Fatalf("DefHash output length = %d, want 64", len(hash))
	}
}

func TestDefHashPanicsOnEmptyName(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("DefHash did not panic on empty def.Name")
		}
	}()
	def := sampleDefForHash()
	def.Name = ""
	DefHash(def)
}
