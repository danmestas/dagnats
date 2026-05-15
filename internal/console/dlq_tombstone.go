package console

import (
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// dlq_tombstone.go owns the in-memory state for DLQ soft-discard with
// undo. When the operator clicks Discard we don't immediately remove
// the JetStream entry — we mark the sequence as tombstoned with an
// expiry, issue a one-shot undo token, and return both to the
// front-end via the action response. The toast renders an Undo button
// pinned to that token; if the operator clicks within the grace
// window the original entry is restored (no-op — we never removed it)
// and the audit log records dlq.undo_discard. If the window expires,
// a sweeper goroutine deletes the entry for real.
//
// Why in-memory: the soft-discard window is short (5s default) and
// scoped to one operator session. Persisting it across server
// restarts is overkill — a restart-during-undo just leaves the entry
// in the DLQ, which is the safe outcome.
//
// Bounded:
//   - 1024 tombstones max in flight; oldest evicted on overflow.
//   - 100ms sweeper tick.

// DLQUndoWindow is the default grace period for soft-discard undo.
const DLQUndoWindow = 5 * time.Second

// dlqTombstoneMax bounds the in-flight tombstone count so a misbehaving
// client can't OOM the server.
const dlqTombstoneMax = 1024

// dlqTombstone is one pending soft-discard. Token is the secret the
// undo POST must present. Sweep deadline (Expires) is wall-clock so
// even a stuck-server delay still trips the right window once it
// resumes.
type dlqTombstone struct {
	Seq     uint64
	Token   string
	Expires time.Time
}

// dlqTombstoneStore is the registry of tombstones in-flight, plus the
// sweeper goroutine driving permanent deletion past the window.
type dlqTombstoneStore struct {
	mu       sync.Mutex
	entries  map[uint64]dlqTombstone
	window   time.Duration
	now      func() time.Time
	onExpire func(seq uint64)
}

// newDLQTombstoneStore returns a registry tied to onExpire as the
// permanent-removal callback. Pass the DataSource.DiscardDeadLetter
// closure as onExpire so an expired tombstone really vanishes.
func newDLQTombstoneStore(
	window time.Duration, onExpire func(seq uint64),
) *dlqTombstoneStore {
	if window <= 0 {
		panic("newDLQTombstoneStore: window must be positive")
	}
	if onExpire == nil {
		panic("newDLQTombstoneStore: onExpire is nil")
	}
	return &dlqTombstoneStore{
		entries:  make(map[uint64]dlqTombstone),
		window:   window,
		now:      time.Now,
		onExpire: onExpire,
	}
}

// Tombstone marks seq as soft-discarded and returns the one-shot undo
// token + the wall-clock expiry. Caller embeds both in the action
// response so the toast can render an Undo button and the client
// knows when to give up. Evicts the oldest entry on overflow.
func (s *dlqTombstoneStore) Tombstone(seq uint64) (string, time.Time) {
	if seq == 0 {
		panic("Tombstone: seq must be positive")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	tok := newUndoToken()
	expires := s.now().Add(s.window)
	if len(s.entries) >= dlqTombstoneMax {
		s.evictOldestLocked()
	}
	s.entries[seq] = dlqTombstone{
		Seq: seq, Token: tok, Expires: expires,
	}
	return tok, expires
}

// Undo verifies token matches seq's tombstone and is still inside the
// window. On success: removes the tombstone and returns true. On
// any failure (wrong token, expired, missing) returns false. Always
// O(1).
func (s *dlqTombstoneStore) Undo(seq uint64, token string) bool {
	if seq == 0 {
		return false
	}
	if token == "" {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t, ok := s.entries[seq]
	if !ok {
		return false
	}
	if t.Token != token {
		return false
	}
	if s.now().After(t.Expires) {
		// Window closed; let the sweeper consume it normally.
		return false
	}
	delete(s.entries, seq)
	return true
}

// SweepOnce runs one pass over the registry and invokes onExpire for
// every tombstone whose window has closed. The sweeper goroutine
// calls this on a ticker; tests call it directly to fast-forward.
// Returns the count of swept entries.
func (s *dlqTombstoneStore) SweepOnce() int {
	s.mu.Lock()
	now := s.now()
	expired := make([]uint64, 0, 8)
	for seq, t := range s.entries {
		if now.After(t.Expires) {
			expired = append(expired, seq)
		}
	}
	for _, seq := range expired {
		delete(s.entries, seq)
	}
	s.mu.Unlock()
	for _, seq := range expired {
		s.onExpire(seq)
	}
	return len(expired)
}

// HasTombstone reports whether seq is currently tombstoned. Useful for
// the SSE list patch to know whether a DLQ row should render as
// dimmed (pending sweep) or absent.
func (s *dlqTombstoneStore) HasTombstone(seq uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.entries[seq]
	return ok
}

// evictOldestLocked removes the tombstone with the soonest expiry.
// Caller must hold s.mu.
func (s *dlqTombstoneStore) evictOldestLocked() {
	var oldestSeq uint64
	oldestT := time.Time{}
	first := true
	for seq, t := range s.entries {
		if first || t.Expires.Before(oldestT) {
			oldestSeq = seq
			oldestT = t.Expires
			first = false
		}
	}
	if oldestSeq != 0 {
		delete(s.entries, oldestSeq)
	}
}

// newUndoToken returns a fresh 64-bit-entropy hex string. Tokens are
// throwaway so 8 bytes is plenty.
func newUndoToken() string {
	var buf [8]byte
	_, err := rand.Read(buf[:])
	if err != nil {
		panic("newUndoToken: rand.Read: " + err.Error())
	}
	return hex.EncodeToString(buf[:])
}
