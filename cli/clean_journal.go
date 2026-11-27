// cli/clean_journal.go
// AGENT_JOURNAL is purged under the archiver's contract (#1030): never at
// or above the protected floor its writer publishes, and not at all when
// that floor is missing or stale. A long agent step reads its journal from
// the floor up, so a purge past it loses records the step still needs.
package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// isJournal reports whether info is the agent journal stream.
func isJournal(info *jetstream.StreamInfo) bool {
	return info != nil && info.Config.Name == natsutil.AgentJournalStream
}

// keepBoundary is the purge boundary that keeps the newest keep messages:
// everything below it goes. 0 when nothing would go.
func keepBoundary(info *jetstream.StreamInfo, keep uint64) uint64 {
	if info == nil {
		panic("keepBoundary: info must not be nil")
	}
	if keep == 0 {
		panic("keepBoundary: keep must be positive")
	}
	end := info.State.LastSeq + 1
	if end <= keep {
		return 0
	}
	return end - keep
}

// purgeJournalBelow purges the journal below boundary, capped at its
// protected floor, and reports whether it purged. With no usable floor it
// purges nothing and says so, as the work-queue skip does.
func purgeJournalBelow(
	ctx context.Context, stream jetstream.Stream, info *jetstream.StreamInfo, boundary uint64,
) bool {
	if stream == nil || !isJournal(info) {
		panic("purgeJournalBelow: needs the agent journal stream")
	}
	floor := natsutil.ProtectedFloorOf(info.Config.Metadata, time.Now())
	if floor == 0 {
		fmt.Fprintf(os.Stderr, "info: skip purge of %s: no fresh protected floor "+
			"(the journal's writer publishes one while agent steps run)\n", info.Config.Name)
		return false
	}
	boundary = min(boundary, floor)
	if boundary <= info.State.FirstSeq {
		return false
	}
	if err := stream.Purge(ctx, jetstream.WithPurgeSequence(boundary)); err != nil {
		fmt.Fprintf(os.Stderr, "warn: purge %s before seq %d: %v\n", info.Config.Name, boundary, err)
		return false
	}
	return true
}
