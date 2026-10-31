// secret_test.go
// Unit tests for secret: minted secrets are distinct and only their own
// hash matches; Split is strict about the credential shape.
// Methodology: pure functions, no NATS.
package secret

import (
	"crypto/sha256"
	"testing"
)

func TestNewMatchesOnlyItsOwnHash(t *testing.T) {
	plaintextA, hashA := New()
	plaintextB, hashB := New()
	if plaintextA == plaintextB {
		t.Fatal("two secrets were equal")
	}
	if len(hashA) != sha256.Size {
		t.Fatalf("hash is %d bytes, want %d", len(hashA), sha256.Size)
	}
	if !Matches(plaintextA, hashA) || !Matches(plaintextB, hashB) {
		t.Fatal("a secret did not match its own hash")
	}
	if Matches(plaintextA, hashB) || Matches(plaintextA, nil) {
		t.Fatal("a secret matched a foreign or missing hash")
	}
}

func TestSplit(t *testing.T) {
	id, plaintext, ok := Split("dgn_abc_s3cr3t", "dgn")
	if !ok || id != "abc" || plaintext != "s3cr3t" {
		t.Fatalf("Split = %q %q %v", id, plaintext, ok)
	}
	for _, bad := range []string{"", "dgn_abc", "djk_abc_s", "dgn__s", "dgn_abc_", "x"} {
		if _, _, ok := Split(bad, "dgn"); ok {
			t.Errorf("Split(%q) accepted", bad)
		}
	}
}
