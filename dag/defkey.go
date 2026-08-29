package dag

import (
	"regexp"
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
// output starts with -- name + ".v.". A prefix match ALONE is not
// enough to identify name's own version keys: a workflow literally
// named e.g. "orders.v" has its own version keys sharing the prefix
// "orders.v." that "orders" would compute too. Callers scanning
// workflow_defs for name's retained versions should use
// DefHashFromVersionKey(key, name), which anchors on the full key
// shape and rejects that collision, rather than testing this prefix
// directly.
func DefVersionKeyPrefix(name string) string {
	if name == "" {
		panic("DefVersionKeyPrefix: name must not be empty")
	}
	return name + defVersionKeyInfix
}

// hex64Pattern matches exactly 64 lowercase hex characters -- a
// SHA-256 digest, per DefHash. Used by DefHashFromVersionKey to
// validate the remainder after stripping name's prefix.
var hex64Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// DefHashFromVersionKey extracts the hash suffix from a version key
// produced by DefVersionKey(name, hash). ok is false when key is not
// EXACTLY a version key belonging to name.
//
// Anchored on both length and content, not a prefix match: a naive
// strings.HasPrefix(key, name+".v.") check is fooled by a workflow
// literally named "orders.v" -- its own version keys
// ("orders.v" + ".v." + hash) also start with "orders.v.", so a
// prefix-only check would mistake them for version keys of a
// DIFFERENT workflow "orders", corrupting that name's retention
// accounting and enabling cross-workflow eviction (#637 review
// fix). Requiring the remainder after name's prefix to be exactly
// 64 hex characters -- no more, no less -- makes that impossible:
// "orders.v.v.<hash>" has "v.<hash>" (66 chars) left over under
// name "orders", which fails the length/hex check and returns false.
func DefHashFromVersionKey(key, name string) (hash string, ok bool) {
	if name == "" {
		panic("DefHashFromVersionKey: name must not be empty")
	}
	prefix := DefVersionKeyPrefix(name)
	if len(key) != len(prefix)+64 {
		return "", false
	}
	if key[:len(prefix)] != prefix {
		return "", false
	}
	candidate := key[len(prefix):]
	if !hex64Pattern.MatchString(candidate) {
		return "", false
	}
	return candidate, true
}
