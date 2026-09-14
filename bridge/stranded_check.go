// bridge/stranded_check.go
// Defense-in-depth counterpart to the engine's primary stranded-subject
// check (Orchestrator.checkStrandedGroupSubjects, internal/engine): #704
// moved the grouped subject/durable encoding behind the group sentinel
// (consumername.GroupSentinel) with
// no compatibility consumer, so a message published to a legacy grouped
// subject before every process upgrades is never reclaimed by any other
// mechanism -- see the pinned design on issue #704. This narrows the
// check to the one (task, group) pair the bridge is about to serve,
// checked right before adopting or creating that pair's consumer.
package bridge

import (
	"context"
	"log/slog"

	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/nats-io/nats.go/jetstream"
)

// checkStrandedGroupSubject reports (never fails) messages still pending
// on (taskType, group)'s pre-#704 legacy-shaped subject. Best-effort: a
// failure to check is logged and swallowed, never surfaced as a poll
// error -- this is a detector, not a precondition for serving traffic.
func checkStrandedGroupSubject(
	ctx context.Context, js jetstream.JetStream, taskType, group string,
) {
	if taskType == "" {
		panic("checkStrandedGroupSubject: taskType must not be empty")
	}
	if group == "" {
		panic("checkStrandedGroupSubject: group must not be empty")
	}
	stream, err := js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		slog.WarnContext(ctx,
			"stranded-subject check: could not open TASK_QUEUES",
			"error", err)
		return
	}
	stranded, err := consumername.CheckStrandedSubjects(
		ctx, stream, []consumername.GroupPair{{Task: taskType, Group: group}},
	)
	if err != nil {
		slog.WarnContext(ctx, "stranded-subject check failed",
			"task", taskType, "group", group, "error", err)
		return
	}
	for _, s := range stranded {
		// Ambiguous by construction, and the remediation is destructive
		// on the wrong reading -- see the engine's reportStranded.
		slog.ErrorContext(ctx,
			"possible stranded grouped task subject: #704 moved this "+
				"pair's consumer filter behind the \""+
				consumername.GroupSentinel+"\" sentinel, so this "+
				"legacy subject holds unacked messages this bridge "+
				"will never poll. AMBIGUOUS: the same subject is also "+
				"the current, healthy one for the UNGROUPED task type "+
				"formed by joining the pair. Republish ONLY if this "+
				"deployment does not also run that ungrouped task "+
				"type -- republishing healthy work duplicates it",
			"subject", s.Subject, "task", s.Task, "group", s.Group,
			"ungrouped_task_type_if_healthy", s.Task+"."+s.Group,
			"pending", s.Count,
		)
	}
}
