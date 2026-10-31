// Package secret mints the random half of a dagnats credential and
// verifies a presented one against its stored hash. Worker tokens and
// program join keys share it: both are "prefix_id_secret" strings whose
// plaintext is shown once and whose store keeps only a SHA-256 digest.
package secret

import (
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"strings"
)

// byteCount is the number of random bytes in a secret (256 bits),
// base64url-encoded without padding so it never contains '_', the
// credential's segment separator.
const byteCount = 32

// New returns a fresh random secret and its SHA-256 hash. Panics only
// if the OS entropy source is unavailable, a fatal system condition
// matching internal/runid's precedent.
func New() (plaintext string, hash []byte) {
	raw := make([]byte, byteCount)
	n, err := rand.Read(raw)
	if err != nil {
		panic("secret.New: crypto/rand failed: " + err.Error())
	}
	if n != byteCount {
		panic("secret.New: short read from crypto/rand")
	}
	plaintext = base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(plaintext))
	return plaintext, sum[:]
}

// Matches reports whether plaintext hashes to hash, in constant time.
// A hash of the wrong length (nil, for an unknown id) is compared
// against a zero digest instead, because subtle.ConstantTimeCompare
// returns early on a length mismatch: an unknown id must cost the same
// work as a known id with a wrong secret.
func Matches(plaintext string, hash []byte) bool {
	sum := sha256.Sum256([]byte(plaintext))
	compareAgainst := hash
	if len(compareAgainst) != sha256.Size {
		compareAgainst = make([]byte, sha256.Size)
	}
	match := subtle.ConstantTimeCompare(sum[:], compareAgainst) == 1
	return match && len(hash) == sha256.Size
}

// Split parses "prefix_id_secret" strictly: exactly three
// '_'-separated segments, the first equal to prefix, and neither id
// nor secret empty.
func Split(credential, prefix string) (id, plaintext string, ok bool) {
	if prefix == "" || strings.Contains(prefix, "_") {
		panic("secret.Split: prefix must be non-empty and contain no '_'")
	}
	parts := strings.SplitN(credential, "_", 3)
	if len(parts) != 3 || parts[0] != prefix {
		return "", "", false
	}
	if parts[1] == "" || parts[2] == "" {
		return "", "", false
	}
	return parts[1], parts[2], true
}
