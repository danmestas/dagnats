// dag/defkey_test.go

// Tests for the workflow_defs KV version-key convention (#637): the
// key shape that stores an immutable, content-addressed def snapshot
// alongside the mutable name -> latest-def pointer, and the charset
// contract NATS KV keys impose on it.
// Methodology: verify the key is a valid NATS KV key, verify it can
// never collide with a plain workflow-name key, and verify the
// recognizer round-trips against the constructor.
package dag

import (
	"regexp"
	"testing"
)

// natsKVKeyPattern mirrors the NATS KV key charset:
// [-/_=.A-Za-z0-9]+. "@" is explicitly NOT a legal key character,
// which is why DefVersionKey uses ".v." rather than "@" as its
// infix.
var natsKVKeyPattern = regexp.MustCompile(`^[-/_=.A-Za-z0-9]+$`)

func TestDefVersionKeyIsValidNATSKVKey(t *testing.T) {
	t.Parallel()
	key := DefVersionKey("my-workflow", DefHash(WorkflowDef{
		Name:  "my-workflow",
		Steps: []StepDef{{ID: "a", Task: "t", Type: StepTypeNormal}},
	}))
	// Positive: the produced key matches the legal NATS KV charset.
	if !natsKVKeyPattern.MatchString(key) {
		t.Fatalf("DefVersionKey produced an invalid NATS KV key: %q", key)
	}
	// Negative: it must not equal the plain name it was derived from.
	if key == "my-workflow" {
		t.Fatalf("version key must differ from the plain name")
	}
}

func TestIsDefVersionKeyRecognizesConstructedKeys(t *testing.T) {
	t.Parallel()
	hash := DefHash(WorkflowDef{
		Name:  "orders",
		Steps: []StepDef{{ID: "a", Task: "t", Type: StepTypeNormal}},
	})
	key := DefVersionKey("orders", hash)
	// Positive: a key built by DefVersionKey is recognized.
	if !IsDefVersionKey(key) {
		t.Fatalf("IsDefVersionKey(%q) = false, want true", key)
	}
	// Negative: an ordinary workflow name is not mistaken for one.
	if IsDefVersionKey("orders") {
		t.Fatalf("IsDefVersionKey(\"orders\") = true, want false")
	}
}

func TestDefVersionKeyCannotCollideWithPlainName(t *testing.T) {
	t.Parallel()
	hash := DefHash(WorkflowDef{
		Name:  "billing",
		Steps: []StepDef{{ID: "a", Task: "t", Type: StepTypeNormal}},
	})
	versionKey := DefVersionKey("billing", hash)
	// Positive: a workflow deliberately NAMED like a version key is
	// itself recognized as one -- so RegisterWorkflow can refuse it
	// and no such name can ever collide with a real version key.
	if !IsDefVersionKey(versionKey) {
		t.Fatalf("a name shaped like a version key must be detected")
	}
	// Negative: a name that merely contains ".v." but not the full
	// 64-hex-char suffix is NOT flagged (avoids over-rejecting
	// ordinary names).
	if IsDefVersionKey("service.v.next") {
		t.Fatalf("a name with a short suffix must not be flagged")
	}
}

// TestDefHashFromVersionKeyAnchoredAgainstNamePrefixCollision verifies
// the review fix for #637: a workflow literally named "orders.v" has
// its own version keys ("orders.v" + ".v." + hash), and a loose
// prefix match on "orders" (prefix "orders.v.") would wrongly treat
// those as belonging to "orders" too -- corrupting retention
// accounting and enabling cross-workflow eviction. DefHashFromVersionKey
// must anchor on the FULL key shape (name + ".v." + exactly 64 hex
// chars, nothing more), not just a prefix.
func TestDefHashFromVersionKeyAnchoredAgainstNamePrefixCollision(t *testing.T) {
	t.Parallel()
	otherHash := DefHash(WorkflowDef{
		Name:  "orders.v",
		Steps: []StepDef{{ID: "a", Task: "t", Type: StepTypeNormal}},
	})
	collidingKey := DefVersionKey("orders.v", otherHash)
	// Positive: the colliding key IS a version key for its own name.
	if _, ok := DefHashFromVersionKey(collidingKey, "orders.v"); !ok {
		t.Fatalf("DefHashFromVersionKey(%q, %q) = false, want true",
			collidingKey, "orders.v")
	}
	// Negative: it must NOT be mistaken for a version key of "orders"
	// even though it shares the "orders.v." prefix.
	if _, ok := DefHashFromVersionKey(collidingKey, "orders"); ok {
		t.Fatalf("DefHashFromVersionKey(%q, %q) = true, want false "+
			"(cross-name collision)", collidingKey, "orders")
	}
}
