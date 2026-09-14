// internal/engine/stranded_check.go
// The engine's primary stranded-subject detector for #704's clean-cut
// migration (no compatibility consumer): a message published to a
// legacy grouped subject before every process upgrades to the group
// sentinel (consumername.GroupSentinel) is NEVER reclaimed by any other
// mechanism (see the pinned
// design on issue #704 — AckWait/MaxDeliver are inert with no consumer,
// work-queue retention removes only on ack, and TASK_QUEUES sets no
// MaxAge). The engine runs this check because it is the process
// guaranteed to restart under the supported engine-first upgrade order;
// worker and bridge carry a narrower, defense-in-depth counterpart at
// their own consumer setup (worker/consumer_collision_xprocess.go,
// bridge/stranded_check.go).
package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/nats-io/nats.go/jetstream"
)

// checkStrandedGroupSubjectsLogMax bounds the logged subject sample so a
// pathological number of stranded subjects cannot flood the log; the
// total is always reported alongside the sample.
const checkStrandedGroupSubjectsLogMax = 10

// checkStrandedGroupSubjectsScanMax bounds how many def keys this check
// will load. Deliberately NOT reconciler.go's defReaperMaxScan: that
// bound guards a background ticker, where a million serial KV loads
// merely runs long, while this check blocks Start(). A bound that
// permits the pathological case it appears to guard is not a safety
// valve, so this one is sized for a real registry and truncates loudly.
const checkStrandedGroupSubjectsScanMax = 4096

// checkStrandedGroupSubjectsTimeout bounds the whole pass. Start() must
// not hang behind a slow or unreachable KV/stream; on expiry the check
// logs truncation and returns, because a missed report is recoverable
// and a stalled startup is not.
const checkStrandedGroupSubjectsTimeout = 30 * time.Second

// checkStrandedGroupSubjects enumerates every registered workflow def's
// (Task, WorkerGroup) pairs and queries TASK_QUEUES for each pair's
// legacy-shaped subjects, logging (never failing on) any non-zero
// count. Deliberately non-fatal: Start must not refuse to serve because
// a stranded subject exists — finding one is exactly the operating
// condition this check exists to surface, not a reason to stay down.
func (o *Orchestrator) checkStrandedGroupSubjects(ctx context.Context) {
	if o.defKV == nil {
		panic("checkStrandedGroupSubjects: defKV must not be nil")
	}
	if o.js == nil {
		panic("checkStrandedGroupSubjects: js must not be nil")
	}
	ctx, cancel := context.WithTimeout(
		ctx, checkStrandedGroupSubjectsTimeout,
	)
	defer cancel()
	pairs, err := o.collectGroupPairs(ctx)
	if err != nil {
		slog.ErrorContext(ctx,
			"stranded-subject check: could not enumerate registered groups",
			"error", err)
		return
	}
	if len(pairs) == 0 {
		return
	}
	stream, err := o.js.Stream(ctx, "TASK_QUEUES")
	if err != nil {
		slog.ErrorContext(ctx,
			"stranded-subject check: could not open TASK_QUEUES",
			"error", err)
		return
	}
	stranded, err := consumername.CheckStrandedSubjects(ctx, stream, pairs)
	if err != nil {
		slog.ErrorContext(ctx, "stranded-subject check failed", "error", err)
		return
	}
	logStrandedGroupSubjects(ctx, stranded)
}

// logStrandedGroupSubjects reports a bounded sample of stranded
// subjects plus the true total, split out so checkStrandedGroupSubjects
// stays under the 70-line function limit.
func logStrandedGroupSubjects(
	ctx context.Context, stranded []consumername.StrandedSubject,
) {
	if len(stranded) == 0 {
		return
	}
	sample := stranded
	if len(sample) > checkStrandedGroupSubjectsLogMax {
		sample = sample[:checkStrandedGroupSubjectsLogMax]
	}
	for _, s := range sample {
		// AMBIGUOUS BY CONSTRUCTION, and the remediation is destructive
		// if applied to the wrong reading: a legacy grouped subject
		// "task.a.b.*" is byte-identical to the CURRENT subject for
		// ungrouped task type "a.b" -- that indistinguishability is the
		// whole reason #704 exists, so no query can separate them. Say
		// so rather than telling an operator to republish what may be
		// live work.
		slog.ErrorContext(ctx,
			"possible stranded grouped task subject: #704 moved the "+
				"grouped encoding behind the \""+
				consumername.GroupSentinel+"\" sentinel, so this "+
				"legacy subject holds unacked messages no grouped "+
				"consumer will match. AMBIGUOUS: this same subject is "+
				"also the current, healthy one for the UNGROUPED task "+
				"type formed by joining the pair. Republish under the "+
				"current (task, group) pair ONLY if this deployment "+
				"does not also run that ungrouped task type -- "+
				"republishing healthy work would duplicate it",
			"subject", s.Subject, "task", s.Task, "group", s.Group,
			"ungrouped_task_type_if_healthy",
			s.Task+"."+s.Group,
			"pending", s.Count,
		)
	}
	slog.ErrorContext(ctx,
		"stranded-subject check: found stranded grouped subjects",
		"total", len(stranded), "logged", len(sample),
	)
}

// collectGroupPairs enumerates every registered workflow def's distinct
// (Task, WorkerGroup) pairs with a non-empty WorkerGroup. Reading pairs
// from the defs themselves removes the guesswork from PAIR SELECTION —
// it never has to infer a (task, group) split out of a subject string.
// It does NOT disambiguate the legacy subject SHAPE those pairs derive:
// "task.a.b.*" is byte-identical whether it means grouped (a, b) or the
// ungrouped task type "a.b", which is precisely the indistinguishability
// #704 exists to remove going forward and cannot remove retroactively.
// That is why reportStranded's message is phrased as a possibility with
// a conditional remediation rather than an instruction.
//
// FAIL-SAFE, same posture as reconciler.go's collectReapable: on ANY def
// load error it aborts the whole pass rather than silently skipping a
// def, since a def this pass cannot read is a def whose pairs it cannot
// rule safe.
func (o *Orchestrator) collectGroupPairs(
	ctx context.Context,
) ([]consumername.GroupPair, error) {
	keys, err := o.defKV.Keys(ctx)
	if err != nil {
		if errors.Is(err, jetstream.ErrNoKeysFound) {
			return nil, nil
		}
		return nil, err
	}
	// Truncate rather than abort: a registry larger than the bound still
	// deserves a check over as much of it as the bound allows, and a
	// startup-blocking pass must not grow with the registry. Loud, so a
	// deployment that outgrows the bound learns the report is partial.
	if len(keys) > checkStrandedGroupSubjectsScanMax {
		slog.WarnContext(ctx,
			"stranded-subject check: def scan truncated; report is "+
				"partial",
			"def_keys", len(keys),
			"scanned", checkStrandedGroupSubjectsScanMax,
		)
		keys = keys[:checkStrandedGroupSubjectsScanMax]
	}
	seen := make(map[consumername.GroupPair]struct{})
	pairs := make([]consumername.GroupPair, 0)
	for _, key := range keys {
		wfDef, err := o.loadDef(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("collectGroupPairs: load %q: %w", key, err)
		}
		for _, step := range wfDef.Steps {
			if step.Task == "" || step.WorkerGroup == "" {
				continue
			}
			pair := consumername.GroupPair{
				Task: step.Task, Group: step.WorkerGroup,
			}
			if _, ok := seen[pair]; ok {
				continue
			}
			seen[pair] = struct{}{}
			pairs = append(pairs, pair)
		}
	}
	return pairs, nil
}
