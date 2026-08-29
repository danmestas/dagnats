// internal/engine/run_event.go
// finalizeRun is the SINGLE funnel every terminal run transition
// routes through: mark terminal, persist the snapshot, publish the
// existing history.{runID} workflow event for Completed/Failed (the
// consumer internal/trigger/http.go already polls), then — only once
// the snapshot write has succeeded — publish the new, reliable
// event.run.{workflow}.{runID}.{status} RunEvent on the EVENTS stream
// (docs/wire-protocol.md "Consumer contract: run lifecycle events").
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
func runEventSubject(workflow, runID, status string) string {
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
	c, _ := m.Int64Counter("engine.run_event.publish_failures")
	runEventPublishFailures = c
}

// publishRunEvent publishes the new EVENTS-stream terminal
// notification for run. Best-effort: a failure here does NOT fail the
// caller — history.{runID} (written by finalizeRun before this call)
// remains the source of truth, matching the WORKFLOW_HISTORY
// consumer contract that already exists. Dedup key is keyed on runID
// alone: a run reaches exactly one terminal status once, so redelivery
// of the same finalize (e.g. a NAK'd handler retrying) must collapse
// to one EVENTS message.
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

// finalizeRun marks run terminal, persists it via saveFn, publishes
// the existing history workflow event for Completed/Failed (so
// internal/trigger/http.go's poll-for-completion path keeps working
// unchanged), and — only after the snapshot write succeeds — publishes
// the new event.run.* RunEvent. Returns the mutated run and any save
// or history-publish error; the new event's publish failure never
// propagates (see publishRunEvent).
func finalizeRun(
	ctx context.Context,
	tp *natsutil.TracingPublisher,
	saveFn SaveSnapshotFunc,
	run dag.WorkflowRun,
	status dag.RunStatus,
	stepID string,
) (dag.WorkflowRun, error) {
	if tp == nil {
		panic("finalizeRun: tp must not be nil")
	}
	if saveFn == nil {
		panic("finalizeRun: saveFn must not be nil")
	}
	run = markTerminal(run, status)
	if err := saveFn(ctx, run, stepID); err != nil {
		return run, err
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
