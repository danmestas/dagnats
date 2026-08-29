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
	token := natsutil.SubjectToken(workflow)
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
		Type:         runEventType(status),
		RunID:        run.RunID,
		WorkflowID:   run.WorkflowID,
		Status:       status,
		CreatedAt:    run.CreatedAt,
		CompletedAt:  run.CompletedAt,
		Labels:       copyLabels(run.Labels),
		TraceParent:  run.TraceParent,
		TriggerDepth: run.TriggerDepth,
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
// Returns the mutated run and any save/history-publish error. An
// afterPersist error does NOT propagate (#648): finalizeWithReleaseDebt
// converts it into a durably recorded ReleasePending debt instead — see
// its doc comment for why. The new event's publish failure never
// propagates either (see publishRunEvent).
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
			return finalizeWithReleaseDebt(
				ctx, tp, saveFn, run, status, stepID, err,
			)
		}
	}
	if err := publishHistoryTerminalEvent(ctx, tp, status, run.RunID); err != nil {
		return run, err
	}
	publishRunEvent(ctx, tp, run)
	return run, nil
}

// finalizeReleaseFailures counts afterPersist (admission release)
// failures observed by finalizeRun after the terminal snapshot was
// already saved (#648). Package-level, mirroring
// runEventPublishFailures — the release-debt path is a pure function
// shared by every finalizeRun call site, not owned by one struct.
var finalizeReleaseFailures metric.Int64Counter

// finalizeReleaseRecovered counts successful reconciler-driven
// recoveries of a ReleasePending debt (see reconciler.go).
var finalizeReleaseRecovered metric.Int64Counter

// finalizeReleaseAbandoned counts ReleasePending runs the reconciler
// gave up on after releaseAttemptsMax failed recovery attempts (#648
// PR review round 3, reconcileReleasePending in reconciler.go).
var finalizeReleaseAbandoned metric.Int64Counter

// finalizeReleaseMalformedSkipped counts ReleasePending runs the
// reconciler refused to hand to releaseAdmission because their
// identity was malformed (empty RunID or WorkflowID) -- releaseAdmission
// panics on either (a programmer-error invariant at that call
// boundary), so the reconciler must validate BEFORE calling it: one
// corrupt KV snapshot must not take down the reconcile goroutine
// (#648 PR review round 4).
var finalizeReleaseMalformedSkipped metric.Int64Counter

func init() {
	m := otel.Meter("dagnats/engine")
	f, err := m.Int64Counter("engine.finalize.release_failures")
	if err != nil {
		panic(
			"init: create engine.finalize.release_failures counter: " +
				err.Error(),
		)
	}
	finalizeReleaseFailures = f
	r, err := m.Int64Counter("engine.finalize.release_recovered")
	if err != nil {
		panic(
			"init: create engine.finalize.release_recovered counter: " +
				err.Error(),
		)
	}
	finalizeReleaseRecovered = r
	a, err := m.Int64Counter("engine.finalize.release_abandoned")
	if err != nil {
		panic(
			"init: create engine.finalize.release_abandoned counter: " +
				err.Error(),
		)
	}
	finalizeReleaseAbandoned = a
	ms, err := m.Int64Counter("engine.finalize.release_malformed_skipped")
	if err != nil {
		panic(
			"init: create engine.finalize.release_malformed_skipped " +
				"counter: " + err.Error(),
		)
	}
	finalizeReleaseMalformedSkipped = ms
}

// finalizeWithReleaseDebt handles an afterPersist (admission release)
// failure for an already-persisted terminal run (#648). Before this
// fix, that error propagated straight to finalizeRun's caller, which
// left the singleton lock / concurrency slot held forever: a
// redelivery of the triggering message reloads the run, finds it
// already terminal, and hits the caller's own early-return before
// ever reaching finalizeRun again — nothing ever retried the release.
//
// Instead: log + count the failure, record the debt durably by
// re-persisting the run with ReleasePending=true FIRST, then STILL
// publish both terminal events (the run IS terminal; consumers must
// hear it even though its admission slot is stuck) so the
// reconciler's terminal-run sweep (reconciler.go) can retry the
// release later via the exact same releaseAdmission logic the normal
// path uses.
//
// The debt-recording save runs BEFORE the publish attempts, not after
// (#648 PR review round 2): publishing is best-effort either way (a
// publish failure is only logged, never propagated -- see
// publishRunEvent and the WARN below), so save-first strictly
// dominates save-after. Saving after publishing would mean a run
// already announced as terminal to every consumer could still end up
// with an unrecoverable leak the reconciler has no way to see, if the
// save that follows the announcement then itself fails. If the save
// fails, its error is returned (redelivery is the only remaining
// recovery channel) and this run is left terminal but NOT
// (necessarily) notified and WITHOUT a persisted ReleasePending flag
// — a residual window bounded to this specific double-failure
// (afterPersist AND the debt save both failing) that the reconciler
// cannot see and therefore cannot recover from automatically.
func finalizeWithReleaseDebt(
	ctx context.Context,
	tp *natsutil.TracingPublisher,
	saveFn SaveSnapshotFunc,
	run dag.WorkflowRun,
	status dag.RunStatus,
	stepID string,
	afterPersistErr error,
) (dag.WorkflowRun, error) {
	slog.WarnContext(ctx,
		"finalizeRun: afterPersist failed after terminal snapshot "+
			"was saved -- admission release is owed, recording debt "+
			"for the reconciler to recover",
		"run_id", run.RunID,
		"workflow_id", run.WorkflowID,
		"status", status.String(),
		"error", afterPersistErr,
	)
	if finalizeReleaseFailures != nil {
		finalizeReleaseFailures.Add(ctx, 1)
	}
	run.ReleasePending = true
	if err := saveFn(ctx, run, stepID); err != nil {
		return run, err
	}
	if err := publishHistoryTerminalEvent(ctx, tp, status, run.RunID); err != nil {
		slog.WarnContext(ctx,
			"finalizeRun: history terminal event publish failed "+
				"during release-debt handling",
			"run_id", run.RunID, "error", err,
		)
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
