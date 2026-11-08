// internal/natsutil/protected_floor.go
// The journal's protected floor (journal.ProtectedFloor), published as
// AGENT_JOURNAL stream metadata so an archiver holding only STREAM.INFO
// and STREAM.PURGE can read it.
//
// The writer's contract: the floor is written when its value changes
// and, unchanged, at least every floorRefreshInterval (5 minutes), so
// dagnats.protected_floor_at stays fresh during a long iteration or a
// quiet journal; never more often than once every floorWriteInterval
// (1 minute), because each write is a stream config update through
// the meta group.
//
// The archiver's contract: never purge at or above
// dagnats.protected_floor. When the key is missing or malformed, or
// dagnats.protected_floor_at is missing, malformed, in the future, or
// older than ProtectedFloorStaleAfter (10 minutes, twice the refresh
// interval), treat the floor as 0 and purge nothing. A missing or
// stale floor is the normal state of a cluster whose writer is down,
// over its budget, or an older binary that does not publish one.
package natsutil

import (
	"context"
	"fmt"
	"maps"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

// Metadata keys on AGENT_JOURNAL.
const (
	// MetaProtectedFloor is the lowest sequence a live agent step may
	// still read, in decimal. Nothing at or above it may be purged.
	MetaProtectedFloor = "dagnats.protected_floor"
	// MetaProtectedFloorAt is when the floor was computed, RFC3339.
	MetaProtectedFloorAt = "dagnats.protected_floor_at"
)

// ProtectedFloorStaleAfter is how old a published floor may be before
// an archiver must treat it as 0. The writer refreshes well inside it.
const ProtectedFloorStaleAfter = 10 * time.Minute

const (
	// floorWriteInterval is the least time between two writes. A
	// config update goes through the meta group, so a moving floor is
	// not written on every change.
	floorWriteInterval = time.Minute
	// floorRefreshInterval is how often an unchanged floor is
	// rewritten, so its time never reaches ProtectedFloorStaleAfter.
	floorRefreshInterval = 5 * time.Minute

	// serverMetadataPrefix marks the keys nats-server sets on every
	// stream itself; a config update leaves them to the server.
	serverMetadataPrefix = "_nats."
)

// PublishProtectedFloor writes floor as AGENT_JOURNAL's protected floor
// when it should (see floorWriteDue) and reports whether it wrote. The
// update carries the stream's current config with only the two keys
// changed, so its messages, limits and other metadata stay as they are.
func PublishProtectedFloor(
	ctx context.Context, js jetstream.JetStream, floor uint64, now time.Time,
) (bool, error) {
	if ctx == nil || js == nil {
		panic("PublishProtectedFloor: ctx and js must not be nil")
	}
	if floor == 0 || now.IsZero() {
		panic("PublishProtectedFloor: floor and now must be set")
	}
	stream, err := js.Stream(ctx, AgentJournalStream)
	if err != nil {
		return false, fmt.Errorf("protected floor: stream: %w", err)
	}
	cfg := stream.CachedInfo().Config
	if !floorWriteDue(cfg.Metadata, floor, now) {
		return false, nil
	}
	cfg.Metadata = mergeKeptMetadata(map[string]string{
		MetaProtectedFloor:   strconv.FormatUint(floor, 10),
		MetaProtectedFloorAt: now.UTC().Format(time.RFC3339),
	}, cfg.Metadata)
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		return false, fmt.Errorf("protected floor: update %s: %w", AgentJournalStream, err)
	}
	return true, nil
}

// floorWriteDue decides a write from what is published: always when
// nothing usable is; otherwise, once floorWriteInterval has passed for
// a changed floor, and once floorRefreshInterval has for an unchanged
// one.
func floorWriteDue(published map[string]string, floor uint64, now time.Time) bool {
	if floor == 0 {
		panic("floorWriteDue: floor must be positive")
	}
	if now.IsZero() {
		panic("floorWriteDue: now must be set")
	}
	current, err := strconv.ParseUint(published[MetaProtectedFloor], 10, 64)
	if err != nil {
		return true
	}
	at, err := time.Parse(time.RFC3339, published[MetaProtectedFloorAt])
	if err != nil || at.After(now) {
		return true
	}
	if floor != current {
		return now.Sub(at) >= floorWriteInterval
	}
	return now.Sub(at) >= floorRefreshInterval
}

// mergeKeptMetadata is own over the existing metadata a stream already
// carries, minus the server's reserved keys: provisioning keeps the
// protected floor and anything an operator set instead of wiping them.
// nil when there is nothing to carry.
func mergeKeptMetadata(own, existing map[string]string) map[string]string {
	if len(existing) > 1<<16 {
		panic("mergeKeptMetadata: existing metadata is implausibly large")
	}
	if len(own) > 1<<16 {
		panic("mergeKeptMetadata: own metadata is implausibly large")
	}
	merged := make(map[string]string, len(own)+len(existing))
	for key, value := range existing {
		if !strings.HasPrefix(key, serverMetadataPrefix) {
			merged[key] = value
		}
	}
	maps.Copy(merged, own)
	if len(merged) == 0 {
		return nil
	}
	return merged
}
