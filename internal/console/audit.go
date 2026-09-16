package console

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/danmestas/dagnats/internal/auditkv"
	"github.com/danmestas/dagnats/internal/natsutil"
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
//
// js resolves the bucket's backing stream for key enumeration
// (#698) and is only dereferenced when kv is non-nil — a caller that
// passes a non-nil kv must also pass a non-nil js.
func listAuditEventsInner(
	ctx context.Context, js jetstream.JetStream, kv jetstream.KeyValue,
	limit int,
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
	if js == nil {
		panic("listAuditEventsInner: js must not be nil when kv is non-nil")
	}
	const maxKeys = 2000
	if limit > maxKeys {
		limit = maxKeys
	}
	stream, err := js.Stream(ctx, "KV_"+kv.Bucket())
	if err != nil {
		return nil, fmt.Errorf("audit bucket stream bind: %w", err)
	}
	keys, err := listAuditKeys(ctx, stream, kv.Bucket(), maxKeys)
	if err != nil {
		return nil, err
	}
	// Newest first: keys sort chronologically ascending, so iterate
	// in reverse.
	out := make([]AuditEvent, 0, limit)
	for i := len(keys) - 1; i >= 0 && len(out) < limit; i-- {
		entry, err := kv.Get(ctx, keys[i])
		if errors.Is(err, jetstream.ErrKeyNotFound) {
			// Expected: the bucket has a TTL (AuditTTL), so an entry
			// listed a moment ago can have expired between the list
			// and this Get. Not a bug, just a stale key.
			continue
		}
		if err != nil {
			// A transient Get failure is indistinguishable from a
			// TTL-expired key once swallowed, and swallowing it drops
			// a LIVE audit event from the page. Fail the call instead
			// of silently returning a short list.
			return nil, err
		}
		var evt AuditEvent
		if err := json.Unmarshal(entry.Value(), &evt); err != nil {
			continue
		}
		out = append(out, evt)
	}
	return out, nil
}

// listAuditKeys returns up to max keys from the bucket, oldest first.
//
// Key enumeration is natsutil.ListKeys (#698), not kv.ListKeys: audit
// events are technically Put (auditkv.Emit), not Create, so a
// once-in-2^48 KeyFor collision would expose this to the same
// watcher-snapshot race #699 fixed for the workers bucket.
//
// Unlike listRunIndexKeys (internal/engine/snapshot.go), which must
// keep the watcher-based ListKeysFiltered because its creation-order
// replay IS the ordering contract, this bucket's keys are safe to
// enumerate from the stream's unordered subject state: auditkv.KeyFor
// prefixes every key with a nanosecond UTC timestamp
// (TestAuditKeyFor_chronologicalOrder pins that later times sort
// lexicographically after earlier ones), so an explicit sort below
// recovers chronological order regardless of enumeration order.
func listAuditKeys(
	ctx context.Context, stream jetstream.Stream, bucket string, max int,
) ([]string, error) {
	if max <= 0 {
		panic("listAuditKeys: max must be positive")
	}
	keys, err := natsutil.ListKeys(ctx, stream, bucket)
	if err != nil {
		return nil, fmt.Errorf("list keys: %w", err)
	}
	sort.Strings(keys)
	if len(keys) > max {
		keys = keys[:max]
	}
	return keys, nil
}
