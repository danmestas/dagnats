// Package lease implements TTL leases with revision fencing on the
// "leases" KV bucket (ADR-024). The NATS server enforces expiry with a
// per-key TTL, so a holder's clock never decides who owns a key. The
// key's KV revision is the fencing token: it strictly increases across
// every write to the key, including writes by a later holder.
//
// The package is a deep module behind four calls -- Acquire, Renew,
// Release, Owner. Every write is a compare-and-set against the revision
// last read, so two contenders can never both be granted the same key.
// Change events need no code here: the bucket republishes every write
// onto dagnats.lease.changed.<key> (see natsutil.LeaseChangedSubjectPrefix).
package lease

import (
	"errors"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
)

// Bounds on caller input. Keys and holders are restricted to subject-
// token characters so a key maps 1:1 onto its event subject -- no
// sanitization step, so no two keys can collide on one subject.
const (
	KeyLengthMax    = 128
	HolderLengthMax = 128
	TTLMin          = time.Second
	TTLMax          = 10 * time.Minute
)

// Event headers. Writes made through this package stamp EventHeader with
// the kind; an expiry is written by the NATS server itself and carries
// MarkerReasonHeader instead (value "MaxAge").
const (
	EventHeader        = "Dagnats-Lease-Event"
	EventAcquired      = "acquired"
	EventRenewed       = "renewed"
	EventReleased      = "released"
	MarkerReasonHeader = "Nats-Marker-Reason"
)

// ErrInvalid marks a request rejected for bad input (key, holder, TTL,
// revision). The wrapped message names the offending field.
var ErrInvalid = errors.New("lease: invalid request")

// ErrLost means the caller does not hold the lease at the revision it
// presented: it expired, was released, or someone else holds it now. A
// holder that sees ErrLost must stop acting as owner immediately.
var ErrLost = errors.New("lease: lost")

// ErrUnsupported means the leases bucket does not exist. SetupAll skips
// it (with a warning) on a NATS server older than 2.11, which lacks the
// per-key TTL leases depend on.
var ErrUnsupported = errors.New(
	"leases unsupported: requires nats-server >= 2.11 " +
		"(the leases bucket is not provisioned)",
)

// Lease is one key's current grant. Revision is the fencing token.
// ExpiresAt is advisory (computed from a clock, not enforced by it); the
// server-side TTL is what actually frees the key.
type Lease struct {
	Key        string
	Holder     string
	Revision   uint64
	AcquiredAt time.Time
	ExpiresAt  time.Time
	TTL        time.Duration
}

// record is the stored KV value.
type record struct {
	Holder     string    `json:"holder"`
	AcquiredAt time.Time `json:"acquired_at"`
	TTLMs      int64     `json:"ttl_ms"`
}

// ChangedSubject is the subject a key's change events arrive on.
func ChangedSubject(key string) string {
	if key == "" {
		panic("ChangedSubject: key must not be empty")
	}
	subject := natsutil.LeaseChangedSubjectPrefix + "." + key
	if len(subject) <= len(natsutil.LeaseChangedSubjectPrefix)+1 {
		panic("ChangedSubject: subject must carry the key")
	}
	return subject
}

// validateToken checks that s is a non-empty subject token no longer
// than lengthMax. field names the input in the returned error.
func validateToken(field, s string, lengthMax int) error {
	if field == "" {
		panic("validateToken: field must not be empty")
	}
	if lengthMax <= 0 {
		panic("validateToken: lengthMax must be positive")
	}
	if s == "" {
		return fmt.Errorf("%w: %s must not be empty", ErrInvalid, field)
	}
	if len(s) > lengthMax {
		return fmt.Errorf("%w: %s longer than %d bytes",
			ErrInvalid, field, lengthMax)
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		token := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
			(c >= '0' && c <= '9') || c == '_' || c == '-'
		if !token {
			return fmt.Errorf("%w: %s may contain only [A-Za-z0-9_-], got %q",
				ErrInvalid, field, s)
		}
	}
	return nil
}

// validateTTL checks ttl is a whole number of seconds in [TTLMin, TTLMax].
// Whole seconds because the server truncates a per-message TTL to
// seconds; accepting 1.5s would silently grant 1s.
func validateTTL(ttl time.Duration) error {
	if TTLMin > TTLMax {
		panic("validateTTL: TTLMin above TTLMax")
	}
	if TTLMin < time.Second {
		panic("validateTTL: TTLMin below the server's 1s minimum")
	}
	if ttl < TTLMin || ttl > TTLMax {
		return fmt.Errorf("%w: ttl_ms must be within [%d, %d], got %d",
			ErrInvalid, TTLMin.Milliseconds(), TTLMax.Milliseconds(),
			ttl.Milliseconds())
	}
	if ttl%time.Second != 0 {
		return fmt.Errorf("%w: ttl_ms must be a whole number of seconds, got %d",
			ErrInvalid, ttl.Milliseconds())
	}
	return nil
}

// validateFence checks the key, holder and (when fenced) revision a
// renew or release presents. revision 0 is never a real KV revision.
func validateFence(key, holder string, revision uint64) error {
	if KeyLengthMax <= 0 || HolderLengthMax <= 0 {
		panic("validateFence: length bounds must be positive")
	}
	if err := validateToken("key", key, KeyLengthMax); err != nil {
		return err
	}
	if err := validateToken("holder", holder, HolderLengthMax); err != nil {
		return err
	}
	if revision == 0 {
		return fmt.Errorf("%w: revision must be the fence from acquire/renew",
			ErrInvalid)
	}
	return nil
}
