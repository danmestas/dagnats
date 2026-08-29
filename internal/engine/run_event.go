// internal/engine/run_event.go
// finalizeRun is the SINGLE funnel every terminal run transition
// routes through: mark terminal, persist the snapshot, run the
// caller's afterPersist release logic (locks/slots/queue advance),
// publish the existing history.{runID} workflow event for
// Completed/Failed (the consumer internal/trigger/http.go already
// polls — this absorbs and replaces the old publishWorkflowCompleted/
// publishWorkflowFailed), then publish the new, reliable
// event.run.{workflow}.{runID}.{status} RunEvent on the EVENTS stream
// (docs/wire-protocol.md "Consumer contract: run lifecycle events").
//
// Ordering matters: afterPersist (lock/slot release, next-pending-run
// advance) runs BEFORE either publish. A subscriber reacting to
// run.completed by starting the next run must see admission state
// that already reflects this run being done — publishing first would
// let that subscriber race the release and get incorrectly
// skipped/queued. See #625 review round 2.
//
// Before this, publishWorkflowCompleted/publishWorkflowFailed were
// called ad hoc from 4 of 12 markTerminal call sites, so most terminal
// transitions (cancelled, compensated, compensate-failed, failed-start,
// singleton-skip) produced a persisted run with NO corresponding
// notification at all — a forge had no choice but to poll GET /runs/{id}.
// finalizeRun closes that gap for every terminal status in one place
// instead of teaching each call site its own publish logic.
package engine

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

// subjectTokenMaxLen bounds the sanitized workflow token so a
// pathological workflow name cannot grow the subject beyond NATS'
// practical limits or blow out subject-based dashboards/ACLs.
const subjectTokenMaxLen = 128

// subjectToken makes s safe for use as a NATS subject token: any byte
// outside [A-Za-z0-9_-] becomes '_', and the result is capped to
// subjectTokenMaxLen. Reused by the event.run.* subject builder below;
// task subjects (taskSubject in task_publish.go) don't sanitize today
// because workflow names there are validated at register time — this
// is the first NATS-subject use of a workflow name that ISN'T already
// validated, so the sanitizer is new rather than reused.
//
// Pure and total, like RootRunIDOf: every input (including "") is
// valid and produces a well-formed (if degenerate) output, so there
// is no invariant to assert here.
func subjectToken(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') ||
			(r >= '0' && r <= '9') || r == '_' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	out := b.String()
	if len(out) > subjectTokenMaxLen {
		out = out[:subjectTokenMaxLen]
	}
	return out
}

// runEventSubject builds the event.run.{workflow}.{runID}.{status}
// subject. workflowToken falls back to "_" when it sanitizes to empty
// (an unset WorkflowID) so the subject never contains a double dot,
// which NATS subject matching treats as a malformed/empty token.
//
// runID is NOT sanitized like workflow is — every runID in this
// codebase is engine- or nuid-generated (never raw user input), so
// the assert below is a programmer-error guard against a caller
// passing something subject-metacharacter-laden, not a defense
// against untrusted input.
func runEventSubject(workflow, runID, status string) string {
	if strings.ContainsAny(runID, ". \t*>") {
		panic("runEventSubject: runID must not contain NATS subject metacharacters")
	}
	token := subjectToken(workflow)
	if token == "" {
		token = "_"
	}
	return "event.run." + token + "." + runID + "." + status
}

// runEventType coarsens a dag.RunStatus string into one of the three
// RunEventType buckets a consumer filters on. Compensated and
// compensate-failed both bucket to Failed: compensation only runs
// after the workflow itself failed, so from a "did this run's original
// goal succeed" standpoint both are failures — the exact outcome still
// rides RunEvent.Status and the subject's status segment.
//
// Pure and total: every dag.RunStatus string maps to a bucket (the
// default case covers failed/compensated/compensate_failed), so
// there is no invariant to assert.
func runEventType(status string) protocol.RunEventType {
	switch status {
	case "completed":
		return protocol.RunEventCompleted
	case "cancelled":
		return protocol.RunEventCancelled
	default:
		// failed, compensated, compensate_failed.
		return protocol.RunEventFailed
	}
}

// runEventPublishFailures counts best-effort event.run.* publish
// failures. Package-level (not threaded through Orchestrator /
// RecoveryManager constructors) because the publish path is a pure
// function shared by both, following the internal/trigger package's
// pkgFirings precedent for a counter with no natural owning struct.
var runEventPublishFailures metric.Int64Counter

func init() {
	m := otel.Meter("dagnats/engine")
	c, err := m.Int64Counter("engine.run_event.publish_failures")
	if err != nil {
		// A meter that cannot create a counter at process startup is a
		// programmer/config error (bad instrument name, misconfigured
		// provider) — there is no runtime fallback that makes sense.
		panic("init: create engine.run_event.publish_failures counter: " + err.Error())
	}
	runEventPublishFailures = c
}

// publishRunEvent publishes the new EVENTS-stream terminal
// notification for run. Best-effort: a failure here does NOT fail the
// caller — history.{runID} (written by finalizeRun before this call)
// remains the source of truth, matching the WORKFLOW_HISTORY
// consumer contract that already exists. Dedup key is keyed on runID
// alone: a run reaches exactly one terminal status once, so redelivery
// of the same finalize (e.g. a NAK'd handler retrying) must collapse
// to one EVENTS message — within JetStream's dedup window. EVENTS
// (internal/natsutil/conn.go) sets no explicit Duplicates window, so
// that window is the server default (2 minutes), not a
// stream-specific override.
func publishRunEvent(
	ctx context.Context, tp *natsutil.TracingPublisher, run dag.WorkflowRun,
) {
	if tp == nil {
		panic("publishRunEvent: tp must not be nil")
	}
	if run.RunID == "" {
		panic("publishRunEvent: RunID must not be empty")
	}
	status := run.Status.String()
	evt := protocol.RunEvent{
		Type:        runEventType(status),
		RunID:       run.RunID,
		WorkflowID:  run.WorkflowID,
		Status:      status,
		CreatedAt:   run.CreatedAt,
		CompletedAt: run.CompletedAt,
		Labels:      copyLabels(run.Labels),
		TraceParent: run.TraceParent,
	}
	data, err := json.Marshal(evt)
	if err != nil {
		recordRunEventPublishFailure(ctx, run, err)
		return
	}
	subject := runEventSubject(run.WorkflowID, run.RunID, status)
	msgID := "run-terminal-" + run.RunID
	_, err = tp.JSPublish(
		ctx, subject, data, jetstream.WithMsgID(msgID),
	)
	if err != nil {
		recordRunEventPublishFailure(ctx, run, err)
	}
}

// copyLabels returns an independent copy of labels, or nil for a nil/
// empty input (so the RunEvent's `labels` json tag stays omitempty
// rather than serializing an empty object). Copying — not aliasing —
// matters because RunEvent may be marshalled asynchronously relative
// to any future mutation of the run's own Labels map by its caller.
func copyLabels(labels map[string]string) map[string]string {
	if len(labels) == 0 {
		return nil
	}
	out := make(map[string]string, len(labels))
	for k, v := range labels {
		out[k] = v
	}
	return out
}

// recordRunEventPublishFailure logs and counts a failed event.run.*
// publish. Split out so publishRunEvent's two failure sites (marshal,
// publish) share one WARN+metric shape.
func recordRunEventPublishFailure(
	ctx context.Context, run dag.WorkflowRun, err error,
) {
	slog.WarnContext(ctx,
		"failed to publish run-terminal event",
		"error", err,
		"run_id", run.RunID,
		"workflow_id", run.WorkflowID,
	)
	if runEventPublishFailures != nil {
		runEventPublishFailures.Add(ctx, 1)
	}
}

// finalizeRun marks run terminal, persists it via saveFn, runs
// afterPersist (the caller's lock/slot-release and queue-advance
// logic — nil for sites with none of that), publishes the existing
// history workflow event for Completed/Failed (so
// internal/trigger/http.go's poll-for-completion path keeps working
// unchanged), and finally publishes the new event.run.* RunEvent.
//
// Guard against the double-terminal operating race: if run arrives
// ALREADY FINALIZED — terminal AND already carrying a CompletedAt —
// the FIRST terminal status wins. finalizeRun no-ops rather than
// re-marking, re-saving, or re-publishing, so the persisted snapshot
// and the one event already published stay in agreement instead of
// the snapshot silently drifting to a second status behind the first
// (and only) event. This covers e.g. a cancel and a fail racing to
// finalize the same run — each handler reloads its own copy from the
// store, so a persisted terminal status here is not a programmer
// error.
//
// The guard checks CompletedAt, not just Status.IsTerminal(): the
// pure state-machine core (Advance, advance.go) legitimately presets
// run.Status to Completed/Failed as part of computing the SAME call's
// transition, before this run has ever been through markTerminal —
// CompletedAt is nil at that point since markTerminal (called only
// from inside finalizeRun) is the sole writer of it. Guarding on
// Status alone would treat that first, legitimate transition as an
// already-finalized run and silently skip persisting/publishing it.
//
// Returns the mutated run and any save/afterPersist/history-publish
// error; the new event's publish failure never propagates (see
// publishRunEvent).
func finalizeRun(
	ctx context.Context,
	tp *natsutil.TracingPublisher,
	saveFn SaveSnapshotFunc,
	run dag.WorkflowRun,
	status dag.RunStatus,
	stepID string,
	afterPersist func(context.Context) error,
) (dag.WorkflowRun, error) {
	if tp == nil {
		panic("finalizeRun: tp must not be nil")
	}
	if saveFn == nil {
		panic("finalizeRun: saveFn must not be nil")
	}
	if run.Status.IsTerminal() && run.CompletedAt != nil {
		slog.DebugContext(ctx,
			"finalizeRun: run already terminal, ignoring redundant "+
				"terminal transition",
			"run_id", run.RunID,
			"existing_status", run.Status.String(),
			"requested_status", status.String(),
		)
		return run, nil
	}
	run = markTerminal(run, status)
	if err := saveFn(ctx, run, stepID); err != nil {
		return run, err
	}
	if afterPersist != nil {
		if err := afterPersist(ctx); err != nil {
			return run, err
		}
	}
	if err := publishHistoryTerminalEvent(ctx, tp, status, run.RunID); err != nil {
		return run, err
	}
	publishRunEvent(ctx, tp, run)
	return run, nil
}

// publishHistoryTerminalEvent publishes workflow.completed/failed to
// history.{runID} for the two statuses that already have a documented
// protocol.EventType and an existing consumer (internal/trigger/http.go).
// Cancelled/Compensated/CompensateFailed have no history counterpart to
// preserve — cancellation already publishes EventWorkflowCancelled as
// the CANCEL REQUEST (see publishCancelEvent), not a completion
// notification, and compensation outcomes never had a history event —
// finalizeRun does not invent new history-stream semantics for those;
// event.run.* is the one place they now get a terminal notification.
func publishHistoryTerminalEvent(
	ctx context.Context, tp *natsutil.TracingPublisher,
	status dag.RunStatus, runID string,
) error {
	var eventType protocol.EventType
	switch status {
	case dag.RunStatusCompleted:
		eventType = protocol.EventWorkflowCompleted
	case dag.RunStatusFailed:
		eventType = protocol.EventWorkflowFailed
	default:
		return nil
	}
	return publishWorkflowEvent(ctx, tp, eventType, runID)
}
