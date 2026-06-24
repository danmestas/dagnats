package console

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/auditkv"
)

// audit.go owns the operator-action audit reader. The schema, bucket, key
// builder, and emit path now live in the internal/auditkv leaf package so
// the engine and api can emit against the SAME shape without importing
// console. This file aliases those symbols and keeps the bucket reader
// (which is console-specific render plumbing). ADR-014.

// AuditEvent records one operator action against the control plane. The
// shape is owned by internal/auditkv; this alias preserves the console
// call sites and the wire format read by the Audit page.
type AuditEvent = auditkv.AuditEvent

// AuditBucket is the JetStream KV bucket name (auditkv.Bucket).
const AuditBucket = auditkv.Bucket

// AuditTTL is the KV bucket TTL (auditkv.TTL).
const AuditTTL = auditkv.TTL

// NewAuditKV opens the console_audit KV bucket, delegating to auditkv.
func NewAuditKV(
	ctx context.Context, js jetstream.JetStream,
) (jetstream.KeyValue, error) {
	return auditkv.NewKV(ctx, js)
}

// emitAuditEventInner writes evt into the console_audit bucket via the
// shared auditkv emitter. Best-effort: nil kv warns and returns nil.
func emitAuditEventInner(
	ctx context.Context, kv jetstream.KeyValue,
	logger *slog.Logger, evt AuditEvent,
) error {
	return auditkv.Emit(ctx, kv, logger, evt)
}

// auditKeyFor builds the storage key for an audit event (auditkv.KeyFor).
func auditKeyFor(t time.Time) (string, error) {
	return auditkv.KeyFor(t)
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
