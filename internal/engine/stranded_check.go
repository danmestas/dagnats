// internal/engine/stranded_check.go
// The engine's primary stranded-subject detector for #704's clean-cut
// migration (no compatibility consumer): a message published to a
// legacy grouped subject before every process upgrades to the "@"
// sentinel is NEVER reclaimed by any other mechanism (see the pinned
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

	"github.com/danmestas/dagnats/internal/consumername"
	"github.com/nats-io/nats.go/jetstream"
)

// checkStrandedGroupSubjectsLogMax bounds the logged subject sample so a
// pathological number of stranded subjects cannot flood the log; the
// total is always reported alongside the sample.
const checkStrandedGroupSubjectsLogMax = 10

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
		slog.ErrorContext(ctx,
			"stranded grouped task subject: #704 moved the consumer "+
				"filter behind the \"@\" sentinel, but this legacy "+
				"subject still holds unacked messages no consumer will "+
				"ever match -- republish under the current (task, "+
				"group) pair to recover them",
			"subject", s.Subject, "task", s.Task, "group", s.Group,
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
// from the defs themselves sidesteps guessing a subject from the
// dotted-versus-grouped ambiguity #704 exists to remove.
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
	if len(keys) > defReaperMaxScan {
		return nil, fmt.Errorf(
			"collectGroupPairs: %d def keys exceeds bound %d",
			len(keys), defReaperMaxScan,
		)
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
