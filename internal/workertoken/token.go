// Package workertoken issues, tracks, and authorizes bearer tokens that
// scope worker access to specific task-type prefixes. This is separate
// from the single global bearer configured via DAGNATS_BRIDGE_TOKEN: that
// token remains the admin/root credential (it mints and revokes worker
// tokens); worker tokens are handed to individual machines and can be
// revoked independently, without rotating the admin credential and
// bouncing every worker at once.
//
// Ousterhout note: Store is the one exported type with state. Authorize
// never round-trips to NATS — it reads a KV-watch-fed in-memory cache —
// so it stays fast on the bridge's hot poll/resolve path. Callers get a
// small surface (Mint, Revoke, List, Authorize) hiding the KV bucket
// layout, the watch/reconnect loop, and the hashing scheme.
package workertoken

import "time"

// Token is a directory entry for one minted worker token, persisted in
// the worker_tokens KV bucket keyed by ID. SecretHash is the SHA-256
// digest of the token's random secret half; the plaintext secret is
// never stored, logged, or returned again after Mint returns it once.
type Token struct {
	ID               string     `json:"id"`
	Label            string     `json:"label"`
	TaskTypePrefixes []string   `json:"task_type_prefixes"`
	CreatedAt        time.Time  `json:"created_at"`
	CreatedBy        string     `json:"created_by"`
	RevokedAt        *time.Time `json:"revoked_at,omitempty"`
	SecretHash       []byte     `json:"secret_hash"`
}

// Claims is what Authorize returns for a bearer that passed
// verification: either the env-configured admin token (Admin: true,
// unscoped) or a minted worker token scoped to TaskTypePrefixes.
type Claims struct {
	TokenID          string
	Admin            bool
	TaskTypePrefixes []string
}

// AllowsTaskType reports whether taskType may be polled under these
// claims. Admin claims bypass scoping entirely; worker-token claims
// require a prefix match. An empty TaskTypePrefixes list authorizes
// nothing — fail closed, not "all types" — matching Mint's contract
// that an empty prefix list is a deliberate no-task-types token.
//
// Matching is segment-aware, not a raw byte-prefix test: prefix p
// matches taskType t iff t == p or t has "p." as a literal prefix —
// p names a whole dot-separated segment. A byte-prefix test would let
// ["build"] match "builder.deploy" and ["echo"] match "echo-admin",
// silently widening a token's grant beyond what its label promised.
func (c Claims) AllowsTaskType(taskType string) bool {
	if taskType == "" {
		panic("Claims.AllowsTaskType: taskType must not be empty")
	}
	if c.Admin {
		return true
	}
	for _, prefix := range c.TaskTypePrefixes {
		if taskType == prefix {
			return true
		}
		if len(taskType) > len(prefix) &&
			taskType[len(prefix)] == '.' &&
			taskType[:len(prefix)] == prefix {
			return true
		}
	}
	return false
}

// Bounds enforced by Mint. These cap the worker_tokens KV bucket's
// growth and the size of any single record, so a scripting mistake
// (or a malicious admin-token holder) cannot mint unbounded state.
const (
	// TokensCountMax bounds the number of non-revoked tokens Mint will
	// create. Revoked tokens are kept for audit and do not count
	// against this limit.
	TokensCountMax = 1000
	// LabelLengthMax bounds a token's human-readable label.
	LabelLengthMax = 128
	// PrefixesCountMax bounds how many task-type prefixes one token
	// may carry.
	PrefixesCountMax = 32
	// PrefixLengthMax bounds a single task-type prefix's length.
	PrefixLengthMax = 64
)
