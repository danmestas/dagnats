// engine/journal_floor.go
// Publishes AGENT_JOURNAL's protected floor (journal.ProtectedFloor)
// over the runs the runactive index holds, so an archiver purging the
// journal never removes records a live agent step reads on its next
// delivery. One node writes: the one WithJournalFloorWriter's func
// names (the server passes "this node is the JetStream meta leader").
// Without the option nothing is published, and an archiver reading a
// missing floor purges nothing.
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/journal"
)

const (
	// journalFloorInterval is the cadence of the floor pass; the
	// publish itself writes at most once a minute
	// (natsutil.PublishProtectedFloor).
	journalFloorInterval = time.Minute
	// journalFloorPassTimeout bounds one pass.
	journalFloorPassTimeout = 30 * time.Second
)

// WithJournalFloorWriter makes the orchestrator publish the journal's
// protected floor while isWriter reports true. isWriter must name at
// most one node of a cluster at a time; the server passes the meta
// leader check. Every pass asks again, so the writer follows
// leadership as it moves.
func WithJournalFloorWriter(isWriter func() bool) OrchestratorOption {
	if isWriter == nil {
		panic("WithJournalFloorWriter: isWriter must not be nil")
	}
	return func(o *Orchestrator) {
		o.journalFloorWriter = isWriter
	}
}

// startJournalFloor runs a pass every journalFloorInterval until ctx
// ends, when a writer func is wired.
func (o *Orchestrator) startJournalFloor(ctx context.Context) {
	if ctx == nil {
		panic("startJournalFloor: ctx must not be nil")
	}
	if journalFloorInterval <= 0 {
		panic("startJournalFloor: interval must be positive")
	}
	if o.journalFloorWriter == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(journalFloorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				passCtx, cancel := context.WithTimeout(ctx, journalFloorPassTimeout)
				err := o.publishJournalFloor(passCtx, now)
				cancel()
				if err != nil {
					slog.WarnContext(ctx, "journal protected floor not published", "error", err)
				}
			}
		}
	}()
}

// publishJournalFloor runs one pass at now: on the writer, compute the
// floor and publish it. A pass over its budget is skipped with a
// warning; the published floor then ages and an archiver stops
// purging, which is the safe direction. No journal stream is no work.
func (o *Orchestrator) publishJournalFloor(ctx context.Context, now time.Time) error {
	if ctx == nil || now.IsZero() {
		panic("publishJournalFloor: ctx and now must be set")
	}
	if o.store == nil || o.js == nil {
		panic("publishJournalFloor: store and js must not be nil")
	}
	if o.journalFloorWriter == nil || !o.journalFloorWriter() {
		return nil
	}
	floor, err := journal.ProtectedFloor(ctx, o.js, o.store.listActiveRunIDs)
	switch {
	case errors.Is(err, journal.ErrNotProvisioned):
		return nil
	case errors.Is(err, journal.ErrFloorOverBudget):
		slog.WarnContext(ctx, "journal protected floor skipped: over budget; "+
			"archivers stop purging once the published floor is stale", "error", err)
		return nil
	case err != nil:
		return fmt.Errorf("compute protected floor: %w", err)
	}
	wrote, err := natsutil.PublishProtectedFloor(ctx, o.js, floor, now)
	if err != nil {
		return err
	}
	if wrote {
		slog.DebugContext(ctx, "journal protected floor published", "floor", floor)
	}
	return nil
}
