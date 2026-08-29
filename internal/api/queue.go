// api/queue.go
// GET /v1/queue (#632): a snapshot of pending task counts per task
// type on the TASK_QUEUES work queue. Source of truth is the stream
// state itself (subject filter on StreamInfo), not a KV mirror or an
// engine in-memory count -- TASK_QUEUES is a JetStream WorkQueuePolicy
// stream, so an unacked message IS the pending task, and the stream's
// own per-subject message counts are already exactly that number.
//
// Labels grouping (mentioned in #632 as "if #629 lands") is
// deliberately NOT implemented: tasks on TASK_QUEUES do not carry run
// labels in their subject or a cheap-to-read header, so grouping by
// label would require fetching and decoding every pending payload --
// unbounded work on a queue that can hold an unbounded number of
// pending tasks. TaskType grouping is free (it's the subject); label
// grouping is not, so it stays out of scope here.
package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
)

// QueueGroupsMax bounds how many distinct task-type subjects a single
// GET /v1/queue response or event.queue.snapshot event will report.
// TASK_QUEUES subjects are only ever created by workflow/task-type
// registration (a bounded, operator-controlled set in practice), but
// nothing in the wire protocol enforces that -- this cap keeps a
// pathological number of ad hoc task types from producing an unbounded
// response.
const QueueGroupsMax = 256

// taskQueuesStreamName and taskSubjectPrefix are the TASK_QUEUES
// stream identity (internal/natsutil/conn.go). Duplicated here rather
// than imported as an exported natsutil constant because natsutil
// deliberately keeps stream names as unexported string literals in
// its own SetupStreams config; api only needs the two values below.
const (
	taskQueuesStreamName = "TASK_QUEUES"
	taskSubjectPrefix    = "task."
	taskSubjectWildcard  = "task.>"
)

// queueDepthResponse is the JSON body for GET /v1/queue. It embeds
// protocol.QueueSnapshot's fields directly (rather than nesting) so
// the wire shape matches the issue's fixed design exactly:
// {"groups":[...],"snapshot_at":"...","truncated":true}.
type queueDepthResponse = protocol.QueueSnapshot

// handleGetQueueV1 serves GET /v1/queue. Method mismatches never reach
// this handler -- the "GET /v1/queue" mux pattern rejects other
// methods with 405 before dispatch.
func (s *Service) handleGetQueueV1(w http.ResponseWriter, r *http.Request) {
	if s == nil {
		panic("handleGetQueueV1: s must not be nil")
	}
	if r == nil {
		panic("handleGetQueueV1: r must not be nil")
	}
	snap, err := buildQueueSnapshot(r.Context(), s.js, time.Now(), s.logger)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	if encErr := json.NewEncoder(w).Encode(snap); encErr != nil {
		http.Error(w, encErr.Error(), http.StatusInternalServerError)
	}
}

// buildQueueSnapshot reads TASK_QUEUES stream state and returns the
// current QueueSnapshot. Shared by the synchronous GET /v1/queue
// handler and the periodic event.queue.snapshot publisher
// (queue_snapshot.go) so both surfaces answer from the identical
// query rather than two independently-drifting implementations.
func buildQueueSnapshot(
	ctx context.Context, js jetstream.JetStream, now time.Time,
	logger *slog.Logger,
) (protocol.QueueSnapshot, error) {
	if js == nil {
		panic("buildQueueSnapshot: js must not be nil")
	}
	if now.IsZero() {
		panic("buildQueueSnapshot: now must not be zero")
	}
	if logger == nil {
		logger = slog.Default()
	}
	stream, err := js.Stream(ctx, taskQueuesStreamName)
	if err != nil {
		return protocol.QueueSnapshot{}, err
	}
	info, err := stream.Info(ctx, jetstream.WithSubjectFilter(taskSubjectWildcard))
	if err != nil {
		return protocol.QueueSnapshot{}, err
	}
	subjects := sortedSubjectKeys(info.State.Subjects)
	truncated := len(subjects) > QueueGroupsMax
	if truncated {
		subjects = subjects[:QueueGroupsMax]
	}
	groups := make([]protocol.QueueGroup, 0, len(subjects))
	for _, subject := range subjects {
		groups = append(groups, buildQueueGroup(
			ctx, stream, subject, info.State.Subjects[subject],
			info.State.FirstSeq, now, logger,
		))
	}
	return protocol.QueueSnapshot{
		Groups:     groups,
		SnapshotAt: now,
		Truncated:  truncated,
	}, nil
}

// sortedSubjectKeys returns subjects' keys sorted ascending, so the
// response is deterministic (task_type order) and, when there are more
// than QueueGroupsMax, so the same subset is truncated to on every
// call.
func sortedSubjectKeys(subjects map[string]uint64) []string {
	keys := make([]string, 0, len(subjects))
	for k := range subjects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// buildQueueGroup builds one QueueGroup for subject. oldestWaitMs is
// best-effort: a direct-get failure for this one subject is logged at
// debug and OldestWaitMs is left nil rather than failing the whole
// snapshot (design fixed by #632 -- one flaky subject must not blank
// out every other group's data).
//
// Called once per subject from buildQueueSnapshot's loop, sequentially
// (not batched/parallelized) and bounded by QueueGroupsMax (256) --
// fine at the default 5s snapshot cadence; revisit with a batched
// direct-get or a lower cadence-to-cardinality ratio if task-type
// cardinality grows well past that bound.
func buildQueueGroup(
	ctx context.Context, stream jetstream.Stream, subject string,
	pending uint64, firstSeq uint64, now time.Time, logger *slog.Logger,
) protocol.QueueGroup {
	group := protocol.QueueGroup{
		TaskType: strings.TrimPrefix(subject, taskSubjectPrefix),
		Pending:  int64(pending),
	}
	msg, err := stream.GetMsg(ctx, firstSeq, jetstream.WithGetMsgSubject(subject))
	if err != nil {
		logger.Debug("queue depth: direct-get failed for subject",
			"subject", subject, "error", err)
		return group
	}
	waitMs := now.Sub(msg.Time).Milliseconds()
	if waitMs < 0 {
		waitMs = 0
	}
	group.OldestWaitMs = &waitMs
	return group
}
