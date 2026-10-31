// Package joinkey issues, tracks and verifies program join keys: the
// credential whose only right is to call program registration. An
// operator mints one key and hands it to whoever deploys programs; a
// key is reusable and unbounded by default, and may be limited to
// program names matching a glob, a number of uses, or an expiry.
//
// A join key has the same shape and once-only-plaintext rule as a
// worker token: "djk_{id}_{secret}", shown once by Create, with only a
// SHA-256 of the secret stored. Verify reads a KV-watch-fed cache, so a
// revoke is visible at once on the revoking node and within the watch's
// reconnect bound everywhere else.
//
// Registration uses a key in this order: Verify (cache only, cheap,
// rejects a bad key before any work), then ConsumeUse to reserve a use
// by compare-and-set BEFORE committing the registration, then RefundUse
// if anything after that fails (a taken name, an exhausted budget, a
// failed store or reload). Reserving first is what keeps a key with
// UsesMax uses from admitting more programs than that under
// concurrency; refunding keeps a refused registration free.
package joinkey

import (
	"errors"
	"fmt"
	"path"
	"time"
)

// BucketName is the KV bucket join keys live in, keyed by ID.
// Provisioned by natsutil.SetupAll; Open also creates it when absent.
const BucketName = "join_keys"

// keyPrefix is the first segment of every join key.
const keyPrefix = "djk"

// JoinKey is one join key's record. SecretHash is the SHA-256 of the
// key's secret half; every value the Store hands out has it zeroed.
// UsesMax 0 means unbounded, in which case UsesLeft is unused.
type JoinKey struct {
	ID         string     `json:"id"`
	Label      string     `json:"label"`
	NameGlob   string     `json:"name_glob,omitempty"`
	UsesMax    int        `json:"uses_max"`
	UsesLeft   int        `json:"uses_left"`
	ExpiresAt  *time.Time `json:"expires_at,omitempty"`
	CreatedBy  string     `json:"created_by"`
	RevokedAt  *time.Time `json:"revoked_at,omitempty"`
	SecretHash []byte     `json:"secret_hash,omitempty"`
}

// Spec is what an operator chooses when creating a key. The zero value
// of each bound means "unbounded": any name, any number of uses, never
// expires.
type Spec struct {
	Label     string
	NameGlob  string
	UsesMax   int
	ExpiresAt *time.Time
}

// Bounds enforced by Create, capping the bucket and each record.
const (
	// KeysCountMax bounds non-revoked keys, and separately the revoked
	// records kept for audit (the oldest are pruned past it).
	KeysCountMax   = 1000
	LabelLengthMax = 128
	GlobLengthMax  = 64
	UsesMaxLimit   = 1_000_000
)

// Verify refusals. ErrInvalid covers every case where the caller has
// not proved it holds the key (malformed, unknown id, wrong secret), so
// those cannot be told apart. The rest are returned only after the
// secret matched, so they tell nothing to someone without the key.
var (
	ErrInvalid        = errors.New("join key invalid")
	ErrRevoked        = errors.New("join key revoked")
	ErrExpired        = errors.New("join key expired")
	ErrExhausted      = errors.New("join key exhausted")
	ErrNameNotAllowed = errors.New("join key does not cover program name")
	ErrNotFound       = errors.New("join key not found")
	// ErrContention: a write lost the revision race casAttemptsMax
	// times in a row. Nothing was written; the caller may retry.
	ErrContention = errors.New("join key contended")
	// ErrInvalidSpec wraps every Create refusal caused by the request;
	// any other Create error is a backend failure.
	ErrInvalidSpec = errors.New("invalid join key spec")
	// ErrLimit: Create refused because KeysCountMax keys are active.
	ErrLimit = errors.New("join key limit reached")
)

// validateSpec checks spec against the Create bounds at time now,
// wrapping any refusal in ErrInvalidSpec.
func validateSpec(spec Spec, now time.Time) error {
	if err := checkSpec(spec, now); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSpec, err)
	}
	return nil
}

func checkSpec(spec Spec, now time.Time) error {
	if spec.Label == "" {
		return fmt.Errorf("label is required")
	}
	if len(spec.Label) > LabelLengthMax {
		return fmt.Errorf("label exceeds %d bytes", LabelLengthMax)
	}
	if spec.UsesMax < 0 || spec.UsesMax > UsesMaxLimit {
		return fmt.Errorf("uses must be 0 (unbounded) to %d", UsesMaxLimit)
	}
	if spec.ExpiresAt != nil && !spec.ExpiresAt.After(now) {
		return fmt.Errorf("expires_at %s is not in the future",
			spec.ExpiresAt.Format(time.RFC3339))
	}
	return validateGlob(spec.NameGlob)
}

// validateGlob accepts "" (any name) or a pattern over the program-name
// charset [a-z0-9] plus the wildcards '*' and '?'. Character classes and
// escapes are refused: a name glob is a short operator-written filter,
// and a pattern that can never match a valid name is a mistake worth a
// 400 now rather than a key that silently admits nothing.
func validateGlob(glob string) error {
	if len(glob) > GlobLengthMax {
		return fmt.Errorf("names glob exceeds %d bytes", GlobLengthMax)
	}
	for i := 0; i < len(glob); i++ {
		c := glob[i]
		if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '*' || c == '?' {
			continue
		}
		return fmt.Errorf("names glob %q: illegal character %q (allowed: a-z 0-9 * ?)",
			glob, string(c))
	}
	return nil
}

// allowsName reports whether key may reserve program name.
func (key JoinKey) allowsName(name string) bool {
	if key.NameGlob == "" {
		return true
	}
	// validateGlob admits no '[' or '\\', so Match cannot return
	// ErrBadPattern; a stored record that somehow does fails closed.
	match, err := path.Match(key.NameGlob, name)
	return err == nil && match
}

// redacted returns key without its secret hash.
func (key JoinKey) redacted() JoinKey {
	key.SecretHash = nil
	return key
}
