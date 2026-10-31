package joinkey

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/kvmirror"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/internal/secret"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// casAttemptsMax bounds the read-modify-write loop in mutate. A
// conflict means another writer committed, so a contender for a key
// with UsesMax uses retries at most UsesMax+1 times before it either
// wins or reads zero; 64 covers that with a wide margin while keeping
// the loop bounded if a writer misbehaves.
const casAttemptsMax = 64

// errUnchanged lets a mutate change func report "already in the wanted
// state" without a write.
var errUnchanged = errors.New("join key unchanged")

// Store is a cached, watch-kept-current view of the join_keys bucket.
// Verify and List read only the cache; Create, Revoke and ConsumeUse
// write through KV and update the cache from their own write.
type Store struct {
	kv     jetstream.KeyValue
	stream jetstream.Stream
	// now is the clock Create and Verify judge expiry by; tests replace it.
	now func() time.Time

	mu   sync.RWMutex
	keys map[string]JoinKey // id -> record, revoked included, hash kept
	// revisions orders cache writes: a local write can land before the
	// watch replays an older one, which must not overwrite it.
	revisions map[string]uint64

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// Open binds the join_keys bucket, warms the cache from it and starts
// the watch that keeps the cache current until Close. ctx bounds only
// the bind and the initial load.
func Open(ctx context.Context, js jetstream.JetStream) (*Store, error) {
	if ctx == nil {
		panic("joinkey.Open: ctx must not be nil")
	}
	if js == nil {
		panic("joinkey.Open: js must not be nil")
	}
	kv, stream, err := kvmirror.Bind(ctx, js, BucketName)
	if err != nil {
		return nil, err
	}
	watchCtx, cancel := context.WithCancel(context.Background())
	s := &Store{
		kv:        kv,
		stream:    stream,
		now:       time.Now,
		keys:      make(map[string]JoinKey),
		revisions: make(map[string]uint64),
		ctx:       watchCtx,
		cancel:    cancel,
	}
	if err := kvmirror.Load(ctx, kv, stream, s.applyEntry); err != nil {
		cancel()
		return nil, fmt.Errorf("load %s bucket: %w", BucketName, err)
	}
	failures, meterErr := otel.Meter("dagnats/joinkey").
		Int64Counter("joinkey.watch_failures")
	if meterErr != nil {
		// Losing one counter must not stop the store serving Verify.
		slog.Warn("joinkey watch_failures counter not registered", "error", meterErr)
	}
	s.wg.Add(1)
	go s.watch(failures)
	return s, nil
}

func (s *Store) watch(failures metric.Int64Counter) {
	defer s.wg.Done()
	kvmirror.Watch(s.ctx, s.kv, failures, s.applyEntry)
}

// Close stops the watch and waits for it to exit.
func (s *Store) Close() {
	if s == nil || s.cancel == nil {
		panic("joinkey.Store.Close: store not opened")
	}
	s.cancel()
	s.wg.Wait()
}

// applyEntry folds one KV entry into the cache. A delete or purge drops
// the record; Revoke never deletes, so that only follows pruning or a
// manual purge.
func (s *Store) applyEntry(entry jetstream.KeyValueEntry) {
	if entry == nil {
		panic("joinkey.applyEntry: entry must not be nil")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	op := entry.Operation()
	if op == jetstream.KeyValueDelete || op == jetstream.KeyValuePurge {
		delete(s.keys, entry.Key())
		delete(s.revisions, entry.Key())
		return
	}
	key, err := decode(entry)
	if err != nil {
		slog.Warn("joinkey: skipping unparsable record", "id", entry.Key(), "error", err)
		return
	}
	s.storeLocked(entry.Key(), key, entry.Revision())
}

// storeLocked caches key unless the cache holds a newer revision.
// Callers hold s.mu for writing.
func (s *Store) storeLocked(id string, key JoinKey, revision uint64) {
	if id == "" {
		panic("joinkey.storeLocked: id must not be empty")
	}
	if revision == 0 {
		panic("joinkey.storeLocked: revision must be positive")
	}
	if revision <= s.revisions[id] {
		return
	}
	s.revisions[id] = revision
	s.keys[id] = key
}

func (s *Store) store(id string, key JoinKey, revision uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.storeLocked(id, key, revision)
}

// lookup returns the cached record for id, hash included.
func (s *Store) lookup(id string) (JoinKey, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	key, ok := s.keys[id]
	return key, ok
}

// Create stores a new key and returns it (without its hash) and its
// plaintext "djk_{id}_{secret}". The plaintext exists only in this
// return value; it is not recoverable afterward.
func (s *Store) Create(
	ctx context.Context, spec Spec, createdBy string,
) (JoinKey, string, error) {
	if ctx == nil {
		panic("joinkey.Create: ctx must not be nil")
	}
	if s.kv == nil {
		panic("joinkey.Create: store not opened")
	}
	if err := validateSpec(spec, s.now()); err != nil {
		return JoinKey{}, "", err
	}
	// Check-then-act, so concurrent Creates can overshoot by a few.
	// Accepted: Create is admin-gated, not an external exhaustion path.
	if s.activeCount() >= KeysCountMax {
		return JoinKey{}, "", fmt.Errorf("join key limit reached (%d active)", KeysCountMax)
	}
	plaintext, hash := secret.New()
	key := JoinKey{
		ID:         runid.New(),
		Label:      spec.Label,
		NameGlob:   spec.NameGlob,
		UsesMax:    spec.UsesMax,
		UsesLeft:   spec.UsesMax,
		ExpiresAt:  spec.ExpiresAt,
		CreatedBy:  createdBy,
		SecretHash: hash,
	}
	data, err := json.Marshal(key)
	if err != nil {
		return JoinKey{}, "", fmt.Errorf("marshal join key: %w", err)
	}
	// Create, not Put: an id collision must fail loudly rather than
	// overwrite another key's secret.
	revision, err := s.kv.Create(ctx, key.ID, data)
	if err != nil {
		return JoinKey{}, "", fmt.Errorf("create join key: %w", err)
	}
	s.store(key.ID, key, revision)
	return key.redacted(), keyPrefix + "_" + key.ID + "_" + plaintext, nil
}

func (s *Store) activeCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	count := 0
	for _, key := range s.keys {
		if key.RevokedAt == nil {
			count++
		}
	}
	return count
}

// List returns every key, revoked included, without hashes, ordered by
// ID so output is stable.
func (s *Store) List() []JoinKey {
	if s.keys == nil {
		panic("joinkey.List: store not opened")
	}
	s.mu.RLock()
	out := make([]JoinKey, 0, len(s.keys))
	for _, key := range s.keys {
		out = append(out, key.redacted())
	}
	s.mu.RUnlock()
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
