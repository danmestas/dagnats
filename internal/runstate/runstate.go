// Package runstate answers the one question a dispatch path asks before
// handing a task to a worker: has the task's run been cancelled (#737)?
// Both dispatch transports ask it, the native NATS worker and the HTTP
// bridge, so the lookup and its fail-open rule live here once.
package runstate

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/nats-io/nats.go/jetstream"
)

// lookupTimeout bounds the KV read so a slow store cannot stall a poll
// or a worker's fetch loop; on timeout the task proceeds (fail open). A
// healthy read takes milliseconds, and a poll may check several tasks
// in turn, so this stays well under a second.
const lookupTimeout = 500 * time.Millisecond

// Cancelled reports whether runID's snapshot in the workflow_runs bucket
// reads cancelled. It fails open: a nil bucket (not provisioned), a
// missing run, a read error, or an undecodable snapshot all report
// false, so a flaky lookup never drops live work. The engine stays the
// authority on run state; this only spares a worker a task the engine
// would ignore.
//
// Only cancellation counts, not every terminal status: cancel is the
// one transition that withdraws queued work, and the engine already
// discards completions for terminal runs (handleStepCompleted).
func Cancelled(ctx context.Context, kv jetstream.KeyValue, runID string) bool {
	if ctx == nil {
		panic("runstate.Cancelled: ctx must not be nil")
	}
	if runID == "" {
		panic("runstate.Cancelled: runID must not be empty")
	}
	if kv == nil {
		return false
	}
	lookupCtx, cancel := context.WithTimeout(ctx, lookupTimeout)
	defer cancel()
	entry, err := kv.Get(lookupCtx, "run."+runID)
	if err != nil {
		return false
	}
	// Decode the status alone: a snapshot carries every step's state
	// and output, none of which this question needs.
	var snapshot struct {
		Status dag.RunStatus `json:"status"`
	}
	if err := json.Unmarshal(entry.Value(), &snapshot); err != nil {
		slog.Warn("run snapshot status unreadable; proceeding",
			"run_id", runID, "error", err)
		return false
	}
	return snapshot.Status == dag.RunStatusCancelled
}
