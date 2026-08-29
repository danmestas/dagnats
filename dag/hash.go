package dag

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// DefHash returns a deterministic content hash for a WorkflowDef: the
// hex-encoded SHA-256 of its canonical JSON encoding. It exists so a
// caller that re-registers the same definition on every trigger (#630)
// can compare hashes and skip the POST /workflows round-trip when the
// definition is unchanged, instead of re-registering unconditionally.
//
// Determinism relies on two Go/encoding/json guarantees rather than any
// custom canonicalization: encoding/json sorts map keys before emitting
// them, and struct fields are always marshaled in declaration order. So
// two WorkflowDef values that are field-for-field equal -- including
// maps populated in a different insertion order -- always marshal to
// byte-identical JSON and therefore hash identically.
//
// Panics on an empty def.Name (a WorkflowDef without a name is a
// programmer error, not a runtime condition to handle) and if marshaling
// fails (WorkflowDef holds only JSON-safe field types, so a marshal
// failure indicates a violated invariant elsewhere, not bad input).
func DefHash(def WorkflowDef) string {
	if def.Name == "" {
		panic("DefHash: def.Name must not be empty")
	}

	data, err := json.Marshal(def)
	if err != nil {
		panic("DefHash: marshal WorkflowDef: " + err.Error())
	}
	if len(data) == 0 {
		panic("DefHash: marshaled WorkflowDef must not be empty")
	}

	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
