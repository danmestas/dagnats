package joinkey

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/danmestas/dagnats/internal/secret"
	"github.com/nats-io/nats.go/jetstream"
)

// Verify checks plaintext against the cache for registering
// programName and returns the key (without its hash). It never
// round-trips to NATS and never spends a use; see ConsumeUse.
//
// The secret is compared before any other check, so every refusal but
// ErrInvalid proves the caller holds the key.
func (s *Store) Verify(plaintext, programName string) (JoinKey, error) {
	if s.keys == nil {
		panic("joinkey.Verify: store not opened")
	}
	if s.now == nil {
		panic("joinkey.Verify: store has no clock")
	}
	id, secretPart, ok := secret.Split(plaintext, keyPrefix)
	if !ok {
		return JoinKey{}, ErrInvalid
	}
	key, found := s.lookup(id)
	// Matches runs even when id is unknown, so an unknown id costs the
	// same as a wrong secret.
	if !secret.Matches(secretPart, key.SecretHash) || !found {
		return JoinKey{}, ErrInvalid
	}
	if err := s.usable(key); err != nil {
		return JoinKey{}, err
	}
	if !key.allowsName(programName) {
		return JoinKey{}, ErrNameNotAllowed
	}
	return key.redacted(), nil
}

// ConsumeUse spends one use of key id. Registration calls it after
// Verify and before committing, and refunds with RefundUse if the
// registration then fails, so concurrent registrations cannot admit
// more than UsesMax programs. It decrements by KV revision
// compare-and-set, so across any number of Stores exactly UsesMax calls
// succeed and UsesLeft never goes below zero; the rest get ErrExhausted.
//
// It reads KV, not the cache, so it is the authoritative check: a key
// revoked or expired since this Store's cache last saw it is refused
// with ErrRevoked or ErrExpired even though Verify passed. An unbounded
// key (UsesMax 0) that is live has nothing to spend and is not written.
func (s *Store) ConsumeUse(ctx context.Context, id string) error {
	if ctx == nil {
		panic("joinkey.ConsumeUse: ctx must not be nil")
	}
	if id == "" {
		panic("joinkey.ConsumeUse: id must not be empty")
	}
	_, err := s.mutate(ctx, id, func(key *JoinKey) error {
		if err := s.usable(*key); err != nil {
			return err
		}
		if key.UsesMax == 0 {
			return errUnchanged
		}
		key.UsesLeft--
		return nil
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// RefundUse returns one use spent by ConsumeUse, for a registration
// that failed after reserving it. It never raises UsesLeft past
// UsesMax, so a duplicate refund is harmless, and it refunds a revoked
// or expired key too: the record stays truthful for audit.
func (s *Store) RefundUse(ctx context.Context, id string) error {
	if ctx == nil {
		panic("joinkey.RefundUse: ctx must not be nil")
	}
	if id == "" {
		panic("joinkey.RefundUse: id must not be empty")
	}
	_, err := s.mutate(ctx, id, func(key *JoinKey) error {
		if key.UsesMax == 0 || key.UsesLeft >= key.UsesMax {
			return errUnchanged
		}
		key.UsesLeft++
		return nil
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	return err
}

// usable reports why key may not be used now, or nil. Verify applies
// the same rules to its cached copy.
func (s *Store) usable(key JoinKey) error {
	if s.now == nil {
		panic("joinkey.usable: store has no clock")
	}
	switch {
	case key.RevokedAt != nil:
		return ErrRevoked
	case key.ExpiresAt != nil && !s.now().Before(*key.ExpiresAt):
		return ErrExpired
	case key.UsesMax > 0 && key.UsesLeft <= 0:
		return ErrExhausted
	}
	return nil
}

// Revoke marks id revoked, keeping the record for audit. Verify refuses
// it at once on this Store and once the watch delivers it elsewhere.
// Idempotent; ErrNotFound (wrapped) for an unknown id.
func (s *Store) Revoke(ctx context.Context, id string) error {
	if ctx == nil {
		panic("joinkey.Revoke: ctx must not be nil")
	}
	if id == "" {
		panic("joinkey.Revoke: id must not be empty")
	}
	_, err := s.mutate(ctx, id, func(key *JoinKey) error {
		if key.RevokedAt != nil {
			return errUnchanged
		}
		now := s.now().UTC()
		key.RevokedAt = &now
		return nil
	})
	if errors.Is(err, errUnchanged) {
		return nil
	}
	if err != nil {
		return err
	}
	s.pruneOldestRevoked(ctx)
	return nil
}

// mutate applies change to the stored record for id by compare-and-set
// on its KV revision, re-reading and retrying on a conflict, and
// updates the cache from the winning write. It reads KV, not the cache,
// because the CAS needs the record's current revision.
func (s *Store) mutate(
	ctx context.Context, id string, change func(*JoinKey) error,
) (JoinKey, error) {
	if s.kv == nil {
		panic("joinkey.mutate: store not opened")
	}
	if change == nil {
		panic("joinkey.mutate: change must not be nil")
	}
	for attempt := 0; attempt < casAttemptsMax; attempt++ {
		entry, err := s.kv.Get(ctx, id)
		// An id that is not even a valid key (a path value with '*' or
		// '.') names no record either.
		if errors.Is(err, jetstream.ErrKeyNotFound) || errors.Is(err, jetstream.ErrInvalidKey) {
			return JoinKey{}, fmt.Errorf("%w: %q", ErrNotFound, id)
		}
		if err != nil {
			return JoinKey{}, fmt.Errorf("read join key %q: %w", id, err)
		}
		key, err := decode(entry)
		if err != nil {
			return JoinKey{}, err
		}
		if err := change(&key); err != nil {
			return JoinKey{}, err
		}
		data, err := json.Marshal(key)
		if err != nil {
			return JoinKey{}, fmt.Errorf("marshal join key: %w", err)
		}
		revision, err := s.kv.Update(ctx, id, data, entry.Revision())
		if errors.Is(err, jetstream.ErrKeyRevisionMismatch) {
			continue
		}
		if err != nil {
			return JoinKey{}, fmt.Errorf("update join key %q: %w", id, err)
		}
		s.store(id, key, revision)
		return key, nil
	}
	return JoinKey{}, fmt.Errorf("%w: join key %q: %d conflicting updates",
		ErrContention, id, casAttemptsMax)
}

// pruneOldestRevoked deletes the oldest revoked records past
// KeysCountMax, so a create/revoke loop cannot grow the bucket without
// bound. Revoked records are audit only, so the oldest go first.
// Best-effort: a failed delete is logged, not returned, since the
// caller's revoke already succeeded.
func (s *Store) pruneOldestRevoked(ctx context.Context) {
	if ctx == nil {
		panic("joinkey.pruneOldestRevoked: ctx must not be nil")
	}
	s.mu.RLock()
	revoked := make([]JoinKey, 0, len(s.keys))
	for _, key := range s.keys {
		if key.RevokedAt != nil {
			revoked = append(revoked, key)
		}
	}
	s.mu.RUnlock()
	if len(revoked) <= KeysCountMax {
		return
	}
	sort.Slice(revoked, func(i, j int) bool {
		return revoked[i].RevokedAt.Before(*revoked[j].RevokedAt)
	})
	for _, key := range revoked[:len(revoked)-KeysCountMax] {
		if err := s.kv.Delete(ctx, key.ID); err != nil {
			slog.Warn("joinkey: prune oldest revoked failed", "id", key.ID, "error", err)
		}
	}
}

func decode(entry jetstream.KeyValueEntry) (JoinKey, error) {
	var key JoinKey
	if err := json.Unmarshal(entry.Value(), &key); err != nil {
		return JoinKey{}, fmt.Errorf("decode join key %q: %w", entry.Key(), err)
	}
	return key, nil
}
