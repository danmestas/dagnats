package console

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// audit.go owns the operator-action audit emitter and reader. ADR-014
// settled on a JetStream KV bucket (console_audit) keyed by
// <RFC3339-nanos>-<random>; values are JSON-serialised AuditEvent
// structs. 90-day TTL caps storage. The reader walks the bucket and
// returns events sorted newest-first.

// AuditEvent records one operator action against the control plane.
// Time is server-side wall clock (UTC). Actor comes from the
// console.Actor in context; Action is a short verb tag; Target names
// the affected entity; Data carries action-specific structured fields.
// Outcome is one of "success", "denied", "failed".
type AuditEvent struct {
	Time    time.Time      `json:"time"`
	Actor   string         `json:"actor"`
	Action  string         `json:"action"`
	Target  string         `json:"target"`
	Data    map[string]any `json:"data,omitempty"`
	Outcome string         `json:"outcome"`
}

// AuditBucket is the JetStream KV bucket name. Created at engine
// startup; absent buckets cause Emit to slog.Warn and continue —
// audit gaps must never block operator actions.
const AuditBucket = "console_audit"

// AuditTTL is the KV bucket TTL. 90 days per ADR-014's metrics
// retention sibling; long enough for a quarterly audit.
const AuditTTL = 90 * 24 * time.Hour

// NewAuditKV opens the console_audit KV bucket on the given JetStream
// handle, creating it if missing. Idempotent — safe to call from any
// path that already has a JetStream connection.
func NewAuditKV(
	ctx context.Context, js jetstream.JetStream,
) (jetstream.KeyValue, error) {
	if ctx == nil {
		panic("NewAuditKV: ctx is nil")
	}
	if js == nil {
		panic("NewAuditKV: js is nil")
	}
	cfg := jetstream.KeyValueConfig{
		Bucket: AuditBucket,
		TTL:    AuditTTL,
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create audit bucket: %w", err)
	}
	return kv, nil
}

// emitAuditEventInner writes evt into the console_audit bucket. Returns
// nil + warning-slog when the bucket isn't configured. Keys are
// <RFC3339-nanos>-<6 hex bytes> so they sort chronologically inside the
// KV — the reader can list keys and walk them in order.
func emitAuditEventInner(
	ctx context.Context, kv jetstream.KeyValue,
	logger *slog.Logger, evt AuditEvent,
) error {
	if ctx == nil {
		panic("emitAuditEventInner: ctx is nil")
	}
	if logger == nil {
		panic("emitAuditEventInner: logger is nil")
	}
	if kv == nil {
		logger.Warn("console: audit bucket not configured, dropping event",
			"action", evt.Action, "target", evt.Target)
		return nil
	}
	if evt.Time.IsZero() {
		evt.Time = time.Now().UTC()
	}
	body, err := json.Marshal(evt)
	if err != nil {
		return fmt.Errorf("marshal audit: %w", err)
	}
	key, err := auditKeyFor(evt.Time)
	if err != nil {
		return fmt.Errorf("audit key: %w", err)
	}
	if _, err := kv.Put(ctx, key, body); err != nil {
		return fmt.Errorf("audit put: %w", err)
	}
	return nil
}

// auditKeyFor builds the storage key for an audit event. Format:
// <RFC3339-nanos>-<6-hex-bytes>. The wall-clock prefix orders the
// bucket; the random suffix prevents collisions when two emits land
// in the same nanosecond.
func auditKeyFor(t time.Time) (string, error) {
	if t.IsZero() {
		return "", errors.New("zero timestamp")
	}
	var buf [6]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("rand: %w", err)
	}
	prefix := t.UTC().Format("20060102T150405.000000000Z07")
	return prefix + "-" + hex.EncodeToString(buf[:]), nil
}

// listAuditEventsInner reads up to limit recent audit events from the
// bucket. Returns nil + nil-error on an empty bucket so callers can
// render the zero state without branching. Bounded loop on a positive
// limit; ≤2k cap is the hard ceiling.
func listAuditEventsInner(
	ctx context.Context, kv jetstream.KeyValue, limit int,
) ([]AuditEvent, error) {
	if ctx == nil {
		panic("listAuditEventsInner: ctx is nil")
	}
	if limit <= 0 {
		panic("listAuditEventsInner: limit must be positive")
	}
	if kv == nil {
		return nil, nil
	}
	const maxKeys = 2000
	if limit > maxKeys {
		limit = maxKeys
	}
	keys, err := listAuditKeys(ctx, kv, maxKeys)
	if err != nil {
		// Empty bucket reports an error; treat as benign zero-state.
		return nil, nil //nolint:nilerr
	}
	// Newest first: keys sort chronologically ascending, so iterate
	// in reverse.
	out := make([]AuditEvent, 0, limit)
	for i := len(keys) - 1; i >= 0 && len(out) < limit; i-- {
		entry, err := kv.Get(ctx, keys[i])
		if err != nil {
			continue
		}
		var evt AuditEvent
		if err := json.Unmarshal(entry.Value(), &evt); err != nil {
			continue
		}
		out = append(out, evt)
	}
	return out, nil
}

// listAuditKeys returns up to max keys from the bucket. JetStream
// KV.ListKeys streams asynchronously; we drain to a slice. Bounded by
// max so a runaway bucket doesn't OOM the caller.
func listAuditKeys(
	ctx context.Context, kv jetstream.KeyValue, max int,
) ([]string, error) {
	if max <= 0 {
		panic("listAuditKeys: max must be positive")
	}
	lister, err := kv.ListKeys(ctx)
	if err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	defer lister.Stop() //nolint:errcheck
	out := make([]string, 0, max)
	for key := range lister.Keys() {
		out = append(out, key)
		if len(out) >= max {
			break
		}
	}
	return out, nil
}
