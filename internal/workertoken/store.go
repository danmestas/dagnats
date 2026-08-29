package workertoken

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/runid"
	"github.com/nats-io/nats.go/jetstream"
)

// bucketName is the KV bucket Store persists tokens in. Provisioned by
// internal/natsutil.SetupKVBuckets alongside "workers" — Open also
// ensures it exists so a Store used outside the standard server
// bootstrap (e.g. a test, or a future standalone token-admin CLI)
// still works.
const bucketName = "worker_tokens"

// reconnectBackoffInitial / reconnectBackoffMax bound the watch
// reconnect loop's exponential backoff. Capped at 30s so a prolonged
// NATS outage does not busy-loop the bridge, while a brief blip
// recovers in well under a second.
const (
	reconnectBackoffInitial = 250 * time.Millisecond
	reconnectBackoffMax     = 30 * time.Second
)

// Store is a cached, watch-kept-current view of the worker_tokens KV
// bucket. Authorize reads the cache only — it never round-trips to
// NATS — so it is safe on the bridge's hot poll/resolve path.
//
// Watch-loss behavior: when the underlying KV watch drops (NATS
// reconnect, server restart), Store keeps serving the last-known cache
// while it reconnects with bounded exponential backoff. This means a
// Revoke committed by another Store instance during the outage is not
// visible here until the watch reconnects — revocation latency is
// bounded by reconnectBackoffMax, not instant. This is a deliberate
// availability/consistency tradeoff: a bridge that hard-failed poll/
// resolve during a brief NATS blip would be worse than admitting stale
// tokens for at most ~30s.
type Store struct {
	kv jetstream.KeyValue

	mu     sync.RWMutex
	tokens map[string]Token // id -> Token, includes revoked (for List/audit)

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Open ensures the worker_tokens bucket exists, loads its current
// contents into the cache, and starts the background watch that keeps
// the cache current. ctx bounds only the initial ensure-bucket and
// initial-load calls; the watch itself runs on a Store-owned context
// until Close is called.
func Open(ctx context.Context, js jetstream.JetStream) (*Store, error) {
	if ctx == nil {
		panic("Open: ctx must not be nil")
	}
	if js == nil {
		panic("Open: js must not be nil")
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket:  bucketName,
		History: 1,
	})
	if err != nil {
		return nil, fmt.Errorf("ensure %s bucket: %w", bucketName, err)
	}
	watchCtx, cancel := context.WithCancel(context.Background())
	s := &Store{
		kv:     kv,
		tokens: make(map[string]Token),
		ctx:    watchCtx,
		cancel: cancel,
	}
	if err := s.loadAll(ctx); err != nil {
		cancel()
		return nil, fmt.Errorf("load %s bucket: %w", bucketName, err)
	}
	s.wg.Add(1)
	go s.watchLoop()
	return s, nil
}

// Close stops the background watch and waits for it to exit. Safe to
// call once; a nil Store is not accepted (programmer error).
func (s *Store) Close() {
	if s == nil {
		panic("Store.Close: s must not be nil")
	}
	if s.cancel == nil {
		panic("Store.Close: cancel must not be nil")
	}
	s.cancel()
	s.wg.Wait()
}

// loadAll populates the cache from a full bucket scan. Called once at
// Open, before the watch starts, so the cache is warm from the first
// Authorize call rather than waiting on the watch's initial replay.
func (s *Store) loadAll(ctx context.Context) error {
	if ctx == nil {
		panic("loadAll: ctx must not be nil")
	}
	if s.kv == nil {
		panic("loadAll: kv must not be nil")
	}
	keys, err := s.kv.ListKeys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil
		}
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for key := range keys.Keys() {
		entry, err := s.kv.Get(ctx, key)
		if err != nil {
			continue // deleted between ListKeys and Get; watch will settle it
		}
		var tok Token
		if err := json.Unmarshal(entry.Value(), &tok); err != nil {
			slog.Warn("workertoken: skipping unparsable record",
				"key", key, "error", err)
			continue
		}
		s.tokens[key] = tok
	}
	return nil
}

// watchLoop keeps the cache current for the life of the Store,
// reconnecting the underlying KV watch with bounded exponential
// backoff whenever it drops. Exits only when s.ctx is cancelled
// (Close).
func (s *Store) watchLoop() {
	defer s.wg.Done()
	backoff := reconnectBackoffInitial
	for s.ctx.Err() == nil {
		watcher, err := s.kv.Watch(s.ctx, ">")
		if err != nil {
			slog.Warn("workertoken: watch failed, retrying",
				"error", err, "backoff", backoff)
			if !s.sleep(backoff) {
				return
			}
			backoff = nextBackoff(backoff)
			continue
		}
		backoff = reconnectBackoffInitial
		s.consumeWatch(watcher)
		shuttingDown := s.ctx.Err() != nil
		if stopErr := watcher.Stop(); stopErr != nil && !shuttingDown {
			// A Stop failure during Close is expected: the connection
			// underlying the subscription is already tearing down.
			slog.Warn("workertoken: watch stop failed", "error", stopErr)
		}
		if shuttingDown {
			return
		}
		slog.Warn("workertoken: watch closed, reconnecting")
	}
}

// consumeWatch drains watcher's update channel into the cache until it
// closes or the Store's context is cancelled.
func (s *Store) consumeWatch(watcher jetstream.KeyWatcher) {
	if watcher == nil {
		panic("consumeWatch: watcher must not be nil")
	}
	updates := watcher.Updates()
	for {
		select {
		case <-s.ctx.Done():
			return
		case entry, ok := <-updates:
			if !ok {
				return
			}
			if entry == nil {
				continue // nil marks "initial replay complete"
			}
			s.applyEntry(entry)
		}
	}
}

// applyEntry updates the cache from one watch entry. Delete/purge
// operations remove the token from the cache entirely — this is fine
// even for an admin-visible audit trail, because Revoke (not Delete)
// is the only mutation path token-management ever takes; a KV delete
// only happens if an operator manually purges the bucket.
func (s *Store) applyEntry(entry jetstream.KeyValueEntry) {
	if entry == nil {
		panic("applyEntry: entry must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if entry.Operation() == jetstream.KeyValueDelete ||
		entry.Operation() == jetstream.KeyValuePurge {
		delete(s.tokens, entry.Key())
		return
	}
	var tok Token
	if err := json.Unmarshal(entry.Value(), &tok); err != nil {
		slog.Warn("workertoken: skipping unparsable watch update",
			"key", entry.Key(), "error", err)
		return
	}
	s.tokens[entry.Key()] = tok
}

// sleep blocks for d or until the Store's context is cancelled,
// reporting false in the latter case so callers can stop retrying.
func (s *Store) sleep(d time.Duration) bool {
	if d <= 0 {
		panic("sleep: d must be positive")
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-s.ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// nextBackoff doubles d, capped at reconnectBackoffMax.
func nextBackoff(d time.Duration) time.Duration {
	if d <= 0 {
		panic("nextBackoff: d must be positive")
	}
	next := d * 2
	if next > reconnectBackoffMax || next <= 0 {
		return reconnectBackoffMax
	}
	return next
}

// Mint creates a new worker token scoped to prefixes, refusing beyond
// TokensCountMax non-revoked tokens. Returns the token's ID and its
// bearer string ("dgn_{id}_{secret}") — the bearer is the ONLY time
// the secret is ever available; it is not recoverable afterward.
func (s *Store) Mint(
	ctx context.Context, label string, prefixes []string, createdBy string,
) (id, bearer string, err error) {
	if ctx == nil {
		panic("Mint: ctx must not be nil")
	}
	if s.kv == nil {
		panic("Mint: kv must not be nil")
	}
	if err := validateLabel(label); err != nil {
		return "", "", err
	}
	if err := validatePrefixes(prefixes); err != nil {
		return "", "", err
	}
	if s.activeTokenCount() >= TokensCountMax {
		return "", "", fmt.Errorf(
			"worker token limit reached (%d active)", TokensCountMax,
		)
	}

	id = runid.New()
	secret, secretHash, err := mintSecret()
	if err != nil {
		return "", "", err
	}
	tok := Token{
		ID:               id,
		Label:            label,
		TaskTypePrefixes: prefixes,
		CreatedAt:        time.Now().UTC(),
		CreatedBy:        createdBy,
		SecretHash:       secretHash,
	}
	data, err := json.Marshal(tok)
	if err != nil {
		return "", "", fmt.Errorf("marshal token: %w", err)
	}
	// Create (not Put): a collision with runid's 128-bit random ID
	// would mean two tokens overwriting each other's secret — Create
	// turns that astronomically unlikely event into a loud error
	// instead of silent data loss.
	if _, err := s.kv.Create(ctx, id, data); err != nil {
		return "", "", fmt.Errorf("mint token: %w", err)
	}
	s.mu.Lock()
	s.tokens[id] = tok
	s.mu.Unlock()
	return id, "dgn_" + id + "_" + secret, nil
}

// secretByteCount is the number of random bytes in a minted secret (32
// bytes = 256 bits, base64url-encoded without padding for a clean
// bearer string with no '_'-colliding characters).
const secretByteCount = 32

// mintSecret returns a fresh random secret and its SHA-256 hash.
// Panics only if the OS entropy source is unavailable — a fatal system
// condition, matching internal/runid's precedent.
func mintSecret() (secret string, hash []byte, err error) {
	raw := make([]byte, secretByteCount)
	n, err := rand.Read(raw)
	if err != nil {
		panic("mintSecret: crypto/rand failed: " + err.Error())
	}
	if n != secretByteCount {
		panic("mintSecret: short read from crypto/rand")
	}
	secret = base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(secret))
	return secret, sum[:], nil
}

// activeTokenCount counts non-revoked tokens in the cache.
func (s *Store) activeTokenCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, tok := range s.tokens {
		if tok.RevokedAt == nil {
			count++
		}
	}
	return count
}

// validateLabel bounds a token's human-readable label.
func validateLabel(label string) error {
	if label == "" {
		return fmt.Errorf("label is required")
	}
	if len(label) > LabelLengthMax {
		return fmt.Errorf("label exceeds %d bytes", LabelLengthMax)
	}
	return nil
}

// validatePrefixes bounds the prefix list and each entry. An empty
// (nil or zero-length) list is valid — it mints a token that
// authorizes no task types (fail closed), not "all types".
func validatePrefixes(prefixes []string) error {
	if len(prefixes) > PrefixesCountMax {
		return fmt.Errorf(
			"task_type_prefixes exceeds %d entries", PrefixesCountMax,
		)
	}
	for _, prefix := range prefixes {
		if prefix == "" {
			return fmt.Errorf("task_type_prefixes entries must not be empty")
		}
		if len(prefix) > PrefixLengthMax {
			return fmt.Errorf(
				"task type prefix exceeds %d bytes", PrefixLengthMax,
			)
		}
	}
	return nil
}

// ErrTokenNotFound is returned by Revoke (wrapped with the requested
// id) when no token with that id is cached. Callers use errors.Is to
// distinguish "unknown id" (404-shaped) from a genuine backend failure
// (500-shaped).
var ErrTokenNotFound = errors.New("worker token not found")

// Revoke marks id revoked, keeping the record for audit. Authorize
// rejects revoked tokens. Returns ErrTokenNotFound (wrapped) if id is
// unknown.
func (s *Store) Revoke(ctx context.Context, id string) error {
	if ctx == nil {
		panic("Revoke: ctx must not be nil")
	}
	if id == "" {
		panic("Revoke: id must not be empty")
	}
	s.mu.RLock()
	tok, ok := s.tokens[id]
	s.mu.RUnlock()
	if !ok {
		return fmt.Errorf("%w: %q", ErrTokenNotFound, id)
	}
	if tok.RevokedAt != nil {
		return nil // already revoked; idempotent
	}
	now := time.Now().UTC()
	tok.RevokedAt = &now
	data, err := json.Marshal(tok)
	if err != nil {
		return fmt.Errorf("marshal token: %w", err)
	}
	if _, err := s.kv.Put(ctx, id, data); err != nil {
		return fmt.Errorf("revoke token: %w", err)
	}
	s.mu.Lock()
	s.tokens[id] = tok
	s.mu.Unlock()
	return nil
}

// List returns every token (minted and revoked) with SecretHash zeroed
// — the hash is a verification-only secret and must never leave the
// store via a listing surface. Reads the cache only, same as
// Authorize.
func (s *Store) List(ctx context.Context) ([]Token, error) {
	if ctx == nil {
		panic("List: ctx must not be nil")
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Token, 0, len(s.tokens))
	for _, tok := range s.tokens {
		tok.SecretHash = nil
		out = append(out, tok)
	}
	return out, nil
}

// errInvalidToken is returned for every Authorize failure mode
// (malformed bearer, unknown ID, wrong secret, revoked). One error
// text for all of them by design: distinguishing "unknown id" from
// "bad secret" in the response would let a caller enumerate valid
// token IDs by timing or message content.
var errInvalidToken = errors.New("invalid worker token")

// Authorize verifies a "dgn_{id}_{secret}" bearer against the cache —
// no NATS round-trip. Returns Claims scoped to the token's
// TaskTypePrefixes, or errInvalidToken for any failure.
func (s *Store) Authorize(bearer string) (Claims, error) {
	if bearer == "" {
		panic("Authorize: bearer must not be empty")
	}
	if s.kv == nil {
		panic("Authorize: store not initialized")
	}
	id, secret, err := parseBearer(bearer)
	if err != nil {
		return Claims{}, errInvalidToken
	}
	s.mu.RLock()
	tok, ok := s.tokens[id]
	s.mu.RUnlock()
	if !ok || tok.RevokedAt != nil {
		return Claims{}, errInvalidToken
	}
	sum := sha256.Sum256([]byte(secret))
	if subtle.ConstantTimeCompare(sum[:], tok.SecretHash) != 1 {
		return Claims{}, errInvalidToken
	}
	return Claims{
		TokenID:          id,
		TaskTypePrefixes: tok.TaskTypePrefixes,
	}, nil
}

// bearerPrefix is the fixed first segment of every minted bearer.
const bearerPrefix = "dgn"

// parseBearer splits "dgn_{id}_{secret}" strictly: exactly three
// underscore-separated segments, the first literally "dgn", and
// neither id nor secret empty. SplitN(..., 3) is safe against an id
// containing '_' in principle, but runid.New() only emits hex, so this
// never actually splits the id.
func parseBearer(bearer string) (id, secret string, err error) {
	parts := strings.SplitN(bearer, "_", 3)
	if len(parts) != 3 {
		return "", "", fmt.Errorf("malformed bearer: wrong segment count")
	}
	if parts[0] != bearerPrefix {
		return "", "", fmt.Errorf("malformed bearer: wrong prefix")
	}
	if parts[1] == "" || parts[2] == "" {
		return "", "", fmt.Errorf("malformed bearer: empty id or secret")
	}
	return parts[1], parts[2], nil
}
