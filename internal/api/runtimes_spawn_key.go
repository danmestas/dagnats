// api/runtimes_spawn_key.go
// Idempotent child-run spawn (#803). A spawn may carry an optional key;
// the child run ID is then claimed with KV Create in the idempotency_keys
// bucket (24h TTL) under a slot derived from (parent run, parent step,
// key). The slot stores the child run ID BEFORE the spawn event is
// published, and the spawn event's Nats-Msg-Id embeds that child ID, so a
// crash between Create and publish is healed by the next call: it finds
// the claim, sees the child run does not exist yet, and republishes (a
// duplicate publish inside the stream's dedup window is dropped).
package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// spawnKeyMaxBytes bounds the caller-supplied key. The key is hashed into
// the slot, so the limit only guards request size, not KV key validity.
const spawnKeyMaxBytes = 256

// spawnKeySlot derives the KV key. Hashing makes it bounded and free of
// subject metacharacters whatever the caller sends; NUL separators keep
// ("a","bc") and ("ab","c") from colliding.
func spawnKeySlot(parentRunID, parentStepID, key string) string {
	if parentRunID == "" {
		panic("spawnKeySlot: parentRunID must not be empty")
	}
	if key == "" {
		panic("spawnKeySlot: key must not be empty")
	}
	sum := sha256.Sum256([]byte(
		parentRunID + "\x00" + parentStepID + "\x00" + key))
	return "spawn." + hex.EncodeToString(sum[:])
}

// spawnMsgID is the spawn event's dedup ID. Keyed spawns embed the child
// run ID so distinct keys under one parent step are distinct messages and
// a republish of the same claim is deduplicated. Unkeyed spawns keep the
// event's own ID, unchanged from before #803.
func spawnMsgID(evt protocol.Event, childRunID string, keyed bool) string {
	if childRunID == "" {
		panic("spawnMsgID: childRunID must not be empty")
	}
	if !keyed {
		return evt.NATSMsgID()
	}
	return evt.NATSMsgID() + "." + childRunID
}

// replayKeyedSpawn answers a replayed keyed spawn from an existing claim.
// hit is false when no claim exists (the caller proceeds to a fresh
// spawn). It runs before the depth/quota checks on purpose: the existing
// child itself counts toward the quota, so a replay must not be refused
// by a cap the original call already passed.
func (s *Service) replayKeyedSpawn(
	ctx context.Context,
	childWorkflow, parentRunID, parentStepID string,
	input []byte, key string,
) (runID, kind string, hit bool, err error) {
	if parentRunID == "" {
		return "", "", false, nil // spawnChildRun rejects it
	}
	if len(key) > spawnKeyMaxBytes {
		return "", cpKindInvalidDef, true, fmt.Errorf(
			"spawn key is %d bytes, max %d", len(key), spawnKeyMaxBytes)
	}
	if s.idempotencyKV == nil {
		return "", cpKindTransport, true,
			errors.New("idempotency_keys bucket unavailable")
	}
	slot := spawnKeySlot(parentRunID, parentStepID, key)
	entry, getErr := s.idempotencyKV.Get(ctx, slot)
	if errors.Is(getErr, jetstream.ErrKeyNotFound) {
		return "", "", false, nil
	}
	if getErr != nil {
		return "", cpKindTransport, true,
			fmt.Errorf("read spawn claim: %w", getErr)
	}
	childRunID := string(entry.Value())
	if err := s.ensureChildSpawned(
		ctx, childWorkflow, parentRunID, parentStepID, input, childRunID,
	); err != nil {
		return "", cpKindTransport, true, err
	}
	return childRunID, "", true, nil
}

// ensureChildSpawned republishes the spawn event when the claimed child
// run does not exist yet (the original publish never landed).
func (s *Service) ensureChildSpawned(
	ctx context.Context,
	childWorkflow, parentRunID, parentStepID string,
	input []byte, childRunID string,
) error {
	if childRunID == "" {
		panic("ensureChildSpawned: childRunID must not be empty")
	}
	_, err := s.store.Load(ctx, childRunID)
	if err == nil {
		return nil
	}
	if !errors.Is(err, engine.ErrRunNotFound) {
		return fmt.Errorf("check claimed child run: %w", err)
	}
	return s.publishSpawn(ctx, childWorkflow, parentRunID, parentStepID,
		input, childRunID, true)
}

// claimChildRunID mints the child run ID. Unkeyed it is a fresh ID; keyed
// it is claimed with KV Create, and a lost race adopts the winner's ID.
func (s *Service) claimChildRunID(
	ctx context.Context, parentRunID, parentStepID, key string,
) (string, string, error) {
	if parentRunID == "" {
		panic("claimChildRunID: parentRunID must not be empty")
	}
	fresh := runid.New()
	if key == "" {
		return fresh, "", nil
	}
	slot := spawnKeySlot(parentRunID, parentStepID, key)
	if _, err := s.idempotencyKV.Create(ctx, slot, []byte(fresh)); err == nil {
		return fresh, "", nil
	} else if !errors.Is(err, jetstream.ErrKeyExists) {
		return "", cpKindTransport, fmt.Errorf("claim spawn key: %w", err)
	}
	entry, err := s.idempotencyKV.Get(ctx, slot)
	if err != nil {
		return "", cpKindTransport, fmt.Errorf("read spawn claim: %w", err)
	}
	return string(entry.Value()), "", nil
}
