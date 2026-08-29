package dag

import (
	"regexp"
	"strings"
)

// defVersionKeyInfix separates a workflow name from its content hash
// in a workflow_defs KV version key (#637). "." is a legal NATS KV
// key character; "@" -- the more obvious separator -- is not (NATS
// KV keys are restricted to [-/_=.A-Za-z0-9]), so a hash-based key
// like "name@hash" would be rejected by the KV client outright.
const defVersionKeyInfix = ".v."

// defVersionKeyPattern matches the suffix DefVersionKey always
// produces: ".v." followed by exactly 64 lowercase hex characters
// (a SHA-256 digest, per DefHash). Used by IsDefVersionKey.
var defVersionKeyPattern = regexp.MustCompile(`\.v\.[0-9a-f]{64}$`)

// DefVersionKey returns the workflow_defs KV key under which the
// immutable, content-addressed snapshot of the def named name at
// content hash hash is stored, alongside the mutable name -> latest
// pointer at plain key name. hash is expected to be a DefHash
// output (64 lowercase hex chars); DefVersionKey does not itself
// validate that shape -- callers pass DefHash(def) directly.
func DefVersionKey(name, hash string) string {
	if name == "" {
		panic("DefVersionKey: name must not be empty")
	}
	if hash == "" {
		panic("DefVersionKey: hash must not be empty")
	}
	return name + defVersionKeyInfix + hash
}

// IsDefVersionKey reports whether key has the shape a DefVersionKey
// call produces: some prefix followed by ".v." and 64 lowercase hex
// characters. RegisterWorkflow rejects workflow names of this shape
// (see registerWorkflowInner) so a legitimate name -> latest pointer
// key can never collide with a version key -- any name that would
// collide is refused at registration time instead.
func IsDefVersionKey(key string) bool {
	return defVersionKeyPattern.MatchString(key)
}

// DefVersionKeyPrefix returns the prefix every DefVersionKey(name, _)
// output starts with -- name + ".v.". Callers scanning workflow_defs
// KV keys for name's retained versions match on this prefix and
// should additionally confirm IsDefVersionKey, since a prefix match
// alone doesn't guarantee the 64-hex-char hash suffix is present.
func DefVersionKeyPrefix(name string) string {
	if name == "" {
		panic("DefVersionKeyPrefix: name must not be empty")
	}
	return name + defVersionKeyInfix
}

// DefHashFromVersionKey extracts the hash suffix from a version key
// produced by DefVersionKey(name, hash). ok is false when key is not
// a version key belonging to name.
func DefHashFromVersionKey(key, name string) (hash string, ok bool) {
	if name == "" {
		panic("DefHashFromVersionKey: name must not be empty")
	}
	prefix := DefVersionKeyPrefix(name)
	if !strings.HasPrefix(key, prefix) || !IsDefVersionKey(key) {
		return "", false
	}
	return strings.TrimPrefix(key, prefix), true
}
