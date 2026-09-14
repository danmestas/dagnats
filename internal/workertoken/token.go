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
	WorkerGroups     []string   `json:"worker_groups,omitempty"`
	CreatedAt        time.Time  `json:"created_at"`
	CreatedBy        string     `json:"created_by"`
	RevokedAt        *time.Time `json:"revoked_at,omitempty"`
	SecretHash       []byte     `json:"secret_hash"`
}

// Claims is what Authorize returns for a bearer that passed
// verification: either the env-configured admin token (Admin: true,
// unscoped) or a minted worker token scoped to TaskTypePrefixes and,
// optionally, WorkerGroups.
type Claims struct {
	TokenID          string
	Admin            bool
	TaskTypePrefixes []string
	WorkerGroups     []string
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

// AllowsWorkerGroup reports whether group may be polled under these
// claims. Admin claims bypass scoping entirely, same as AllowsTaskType.
// A token with NO WorkerGroups entries is unscoped-by-group -- today's
// behavior, so existing tokens keep working -- and may poll any group,
// including the ungrouped queue (group == ""). A group-scoped token
// (at least one WorkerGroups entry) must match group EXACTLY: unlike
// AllowsTaskType there is no prefix/segment concept here, because
// worker_group is always a single subject token (dag.ValidWorkerGroup
// forbids dots) -- a group is one token, so prefix semantics would be
// a silent widening.
//
// Because "" is never a mintable group value (Mint refuses an empty
// entry), a group-scoped token can never list it, which means a
// group-scoped token can NEVER poll the ungrouped queue -- that is the
// whole point of the isolation this closes: a token scoped to one
// repo's group must not be able to fall back to draining everyone
// else's ungrouped work.
//
// Before #704, this method alone was NOT the full isolation guarantee:
// internal/consumername.FilterFor's (task type, group) -> filter mapping
// was not injective -- a dotted ungrouped task type "a.b" derived the
// byte-identical filter subject as the grouped pair (task type "a",
// group "b") -- so a caller could request the SAME queue under either
// spelling, and the bridge's poll handler needed an extra aliased-reading
// check (bridge.firstUnauthorizedAliasedReading, #695 review, removed in
// #704) on top of this method. #704's group sentinel in FilterFor makes the
// two spellings derive provably distinct filter subjects, so that
// aliasing is gone: AllowsWorkerGroup against the caller's own literal
// request value is sufficient on its own.
func (c Claims) AllowsWorkerGroup(group string) bool {
	if c.Admin {
		return true
	}
	if len(c.WorkerGroups) == 0 {
		return true
	}
	for _, g := range c.WorkerGroups {
		if g == group {
			return true
		}
	}
	return false
}

// AdminTokenID is the reserved token_id value the bridge's
// worker.Directory writes for a registration created by the admin
// bearer or dev mode (#650 round 3): using "" for those entries made
// an admin takeover indistinguishable from a genuinely unowned entry
// (a pre-#627 record, or a native Go worker outside the bridge's
// scope) and therefore reclaimable by the next bridge token to
// connect. Defined here (workertoken is the token-identity package)
// rather than in worker, which imports it, so the dependency runs
// public -> internal instead of the reverse. Mint asserts a minted id
// can never equal this value -- ids are nuids, so the collision is
// not reachable in practice, but the assertion makes the invariant
// explicit.
const AdminTokenID = "admin"

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
	// WorkerGroupsCountMax bounds how many worker groups one token may
	// carry.
	WorkerGroupsCountMax = 32
)
