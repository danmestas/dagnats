// internal/consumername/stranded.go
// The #704 clean-cut migration has no compatibility consumer (see the
// pinned design on issue #704): a message published to a legacy
// grouped subject before every process upgrades is NEVER reclaimed by
// any other mechanism — AckWait/MaxDeliver are inert with no consumer,
// work-queue retention removes only on ack, and TASK_QUEUES sets no
// MaxAge. This file is the entire safety net: it detects (never
// remediates) messages stranded on a legacy grouped subject.
package consumername

import (
	"context"
	"fmt"

	"github.com/nats-io/nats.go/jetstream"
)

// groupPairsMax bounds one CheckStrandedSubjects call (TigerStyle: every
// loop over caller-supplied data needs an explicit upper bound). The
// engine's caller passes distinct (Task, WorkerGroup) pairs collected
// from registered workflow defs — nowhere near this in practice; the
// bound exists to fail loud on a pathological caller, not to accommodate
// a real deployment.
const groupPairsMax = 10_000

// GroupPair identifies one (taskType, group) pairing with a non-empty
// group — the shape #704's "@" sentinel now disambiguates. Used only to
// drive CheckStrandedSubjects.
type GroupPair struct {
	Task  string
	Group string
}

// StrandedSubject reports one legacy-shaped (pre-#704) grouped subject
// that still carries pending messages no #704-upgraded consumer filter
// will ever match.
type StrandedSubject struct {
	Task    string
	Group   string
	Subject string
	Count   uint64
}

// CheckStrandedSubjects queries TASK_QUEUES for pending messages on each
// pair's legacy-shaped subject(s) (LegacyFilterFor) and returns one
// StrandedSubject per matching subject with a non-zero count. Pairs with
// an empty Group are skipped — ungrouped subjects are byte-identical
// before and after #704, so nothing can be stranded by this change.
//
// Detection only: callers report the result (slog.Error naming the
// subject, the count, and the republish remediation) but must NEVER
// fail startup or consumer setup on it — see the package doc.
func CheckStrandedSubjects(
	ctx context.Context, stream jetstream.Stream, pairs []GroupPair,
) ([]StrandedSubject, error) {
	if stream == nil {
		panic("CheckStrandedSubjects: stream must not be nil")
	}
	if len(pairs) > groupPairsMax {
		return nil, fmt.Errorf(
			"CheckStrandedSubjects: %d pairs exceeds bound %d",
			len(pairs), groupPairsMax,
		)
	}
	var stranded []StrandedSubject
	for _, pair := range pairs {
		if pair.Task == "" {
			panic("CheckStrandedSubjects: pair.Task must not be empty")
		}
		if pair.Group == "" {
			continue
		}
		// A pair's two legacy shapes (pre-#674 ">" and post-#674-pre-#704
		// "*") can both match the SAME literal subject — the ">" wildcard
		// is a strict superset of "*" for a single trailing token. Dedupe
		// within the pair so a subject matching both shapes is reported
		// once, not twice.
		seen := make(map[string]struct{})
		for _, legacy := range LegacyFilterFor(pair.Task, pair.Group) {
			info, err := stream.Info(ctx, jetstream.WithSubjectFilter(legacy))
			if err != nil {
				return stranded, fmt.Errorf(
					"CheckStrandedSubjects: StreamInfo(%q): %w", legacy, err,
				)
			}
			for subject, count := range info.State.Subjects {
				if count == 0 {
					continue
				}
				if _, dup := seen[subject]; dup {
					continue
				}
				seen[subject] = struct{}{}
				stranded = append(stranded, StrandedSubject{
					Task:    pair.Task,
					Group:   pair.Group,
					Subject: subject,
					Count:   count,
				})
			}
		}
	}
	return stranded, nil
}
