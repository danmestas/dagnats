// Package auditkv is the single source of truth for the operator-action
// audit schema and its JetStream KV transport. It is a leaf: it imports
// neither console, api, nor engine — those packages import it, which keeps
// the schema in one place and avoids an import cycle. ADR-014 settled on a
// KV bucket (console_audit) keyed by <RFC3339-nanos>-<random>; values are
// JSON-serialised AuditEvent structs with a 90-day TTL.
package auditkv

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

// AuditEvent records one action against the control plane. Time is
// server-side wall clock (UTC). Actor identifies who acted (a console
// operator, or "orchestrator"/"runtime:<id>" for engine/api emits); Action
// is a short verb tag; Target names the affected entity; Data carries
// action-specific structured fields. Outcome is "success", "denied", or
// "failed". The JSON tags are the wire format read by the console Audit
// page — do not reshape without migrating existing bucket contents.
type AuditEvent struct {
	Time    time.Time      `json:"time"`
	Actor   string         `json:"actor"`
	Action  string         `json:"action"`
	Target  string         `json:"target"`
	Data    map[string]any `json:"data,omitempty"`
	Outcome string         `json:"outcome"`
}

// Bucket is the JetStream KV bucket name.
const Bucket = "console_audit"

// TTL caps audit storage at 90 days (ADR-014).
const TTL = 90 * 24 * time.Hour

// NewKV opens the console_audit KV bucket on the given JetStream handle,
// creating it if missing. Idempotent — safe to call from any path holding a
// JetStream connection.
func NewKV(
	ctx context.Context, js jetstream.JetStream,
) (jetstream.KeyValue, error) {
	if ctx == nil {
		panic("auditkv.NewKV: ctx is nil")
	}
	if js == nil {
		panic("auditkv.NewKV: js is nil")
	}
	kv, err := js.CreateOrUpdateKeyValue(ctx, jetstream.KeyValueConfig{
		Bucket: Bucket,
		TTL:    TTL,
	})
	if err != nil {
		return nil, fmt.Errorf("create audit bucket: %w", err)
	}
	return kv, nil
}

// Emit writes evt into the bucket. Best-effort: a nil KV warns and returns
// nil rather than erroring, so an audit gap never blocks the action that
// triggered it. The caller is expected to slog.Warn on a non-nil error and
// continue — auditing is observability, not a load-bearing write.
func Emit(
	ctx context.Context, kv jetstream.KeyValue,
	logger *slog.Logger, evt AuditEvent,
) error {
	if ctx == nil {
		panic("auditkv.Emit: ctx is nil")
	}
	if logger == nil {
		panic("auditkv.Emit: logger is nil")
	}
	if kv == nil {
		logger.Warn("auditkv: audit bucket not configured, dropping event",
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
	key, err := KeyFor(evt.Time)
	if err != nil {
		return fmt.Errorf("audit key: %w", err)
	}
	if _, err := kv.Put(ctx, key, body); err != nil {
		return fmt.Errorf("audit put: %w", err)
	}
	return nil
}

// KeyFor builds the storage key for an event. Format:
// <RFC3339-nanos>-<6-hex-bytes>. The wall-clock prefix orders the bucket
// chronologically; the random suffix prevents collisions when two emits
// land in the same nanosecond.
func KeyFor(t time.Time) (string, error) {
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
