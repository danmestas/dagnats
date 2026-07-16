// Package trigger
//
// fire.go owns the shared "publish workflow.started + record
// TriggerFire" logic. Both the Scheduler (cron tick path) and the
// api.Service (manual fire-now via console / CLI, #352) call Fire so
// the dedup-msg-id contract, the TriggerEnvelope wire shape, and the
// TRIGGER_HISTORY emission stay in one place. The scheduler used to
// own this inline; #352 split it out without changing wire behaviour
// for the cron path.
package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// SourceCron is the envelope/source tag the scheduler uses on the
// cron-tick fire path. Manual fires from the console / CLI use
// SourceManual so the operator can grep history for human-initiated
// runs versus scheduled runs.
const (
	SourceCron   = "cron"
	SourceManual = "manual"
)

// fireTracer is Fire's package-level tracer. A plain package var
// (not struct-held) because Fire is a free function shared by both
// the Scheduler (cron) and api.Service (manual) call sites rather
// than a method on either — mirrors metrics.go's pkgFirings
// package-global idiom for the same reason.
var fireTracer = otel.Tracer("dagnats/trigger")

// Fire publishes a workflow.started event plus the matching
// TriggerFire history record for one trigger fire.
//
// The dedup-msg-id strategy depends on source:
//   - SourceCron: minute-bucketed ID ("trigger.<id>.<unix_minute>")
//     so two scheduler ticks inside the same matching minute collapse
//     to one fire. This preserves the existing #173 contract.
//   - SourceManual: nanosecond-unique ID ("trigger.<id>.manual.<ns>")
//     so the operator can fire-now repeatedly inside one minute and
//     each call produces a distinct run. The fire-rate-limit gating
//     is the operator-facing throttle for manual fires — JetStream
//     dedup is just a safety net against duplicate publishes.
//
// Returns the run ID the workflow.started event carried; the manual
// fire path surfaces this to the operator (UI toast + CLI stdout).
func Fire(
	ctx context.Context,
	tp *natsutil.TracingPublisher,
	def TriggerDef,
	source string,
	now time.Time,
) (string, error) {
	if ctx == nil {
		panic("trigger.Fire: ctx must not be nil")
	}
	if tp == nil {
		panic("trigger.Fire: tp must not be nil")
	}
	if def.ID == "" {
		panic("trigger.Fire: def.ID is empty")
	}
	if def.WorkflowID == "" {
		panic("trigger.Fire: def.WorkflowID is empty")
	}
	if source == "" {
		panic("trigger.Fire: source is empty")
	}

	ctx, span := startFireSpan(ctx, def, source)
	defer span.End()

	envelope := TriggerEnvelope{
		Trigger:    source,
		Source:     def.ID,
		WorkflowID: def.WorkflowID,
		Timestamp:  now.UTC(),
	}
	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return "", fmt.Errorf("marshal envelope: %w", err)
	}

	// run_id is set on the span only after minting — it does not
	// exist yet when startFireSpan starts the span above.
	runID := runid.New()
	span.SetAttributes(attribute.String("run_id", runID))
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, runID, payloadBytes,
	)

	// msg.Data is intentionally left unset: JSPublishMsgEvent
	// marshals evt itself, after trace-context injection, so the
	// persisted JetStream record's body carries TraceParent too
	// (dual-write, see natsutil.TracingPublisher.JSPublishMsgEvent).
	startedMsgID := startedDedupID(def.ID, source, now)
	msg := &nats.Msg{Subject: evt.NATSSubject()}
	if _, err = tp.JSPublishMsgEvent(
		ctx, msg, &evt, jetstream.WithMsgID(startedMsgID),
	); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("publish workflow.started: %w", err)
	}

	if err := publishTriggerFireRecord(
		ctx, tp, def, runID, source, now,
	); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return "", fmt.Errorf("publishTriggerFire: %w", err)
	}
	return runID, nil
}

// startFireSpan starts the "trigger.fire" span with the identity
// attributes known before the run ID is minted (trigger_id,
// workflow_id, trigger_source). Extracted out of Fire so Fire stays
// under the 70-line function limit; re-checks ctx/def.ID even though
// Fire already checked them, matching this file's convention of
// redundant assertions at every function boundary.
func startFireSpan(
	ctx context.Context, def TriggerDef, source string,
) (context.Context, trace.Span) {
	if ctx == nil {
		panic("startFireSpan: ctx must not be nil")
	}
	if def.ID == "" {
		panic("startFireSpan: def.ID is empty")
	}
	return fireTracer.Start(ctx, "trigger.fire",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("trigger_id", def.ID),
			attribute.String("workflow_id", def.WorkflowID),
			attribute.String("trigger_source", source),
		),
	)
}

// startedDedupID picks the Nats-Msg-Id for the workflow.started
// publish. Cron uses the minute-bucket form preserved from #173;
// manual uses nanosecond-unique so concurrent operator clicks don't
// silently collapse.
func startedDedupID(
	triggerID string, source string, now time.Time,
) string {
	if triggerID == "" {
		panic("startedDedupID: triggerID is empty")
	}
	if source == SourceCron {
		return fmt.Sprintf("trigger.%s.%d", triggerID, now.Unix()/60)
	}
	return fmt.Sprintf(
		"trigger.%s.%s.%d", triggerID, source, now.UnixNano(),
	)
}

// publishTriggerFireRecord writes one TriggerFire row to the
// TRIGGER_HISTORY stream. Extracted from the scheduler so the manual
// fire path uses the same wire format the CLI history command reads.
//
// ctx carries the trigger.fire span from the caller (Fire) so this
// publish is no longer trace-detached (it used to run under
// context.Background()); the TriggerFire wire shape has no
// TraceParent field, so JSPublish's header-only injection is
// sufficient here — no dual-write is needed or possible.
func publishTriggerFireRecord(
	ctx context.Context,
	tp *natsutil.TracingPublisher,
	def TriggerDef,
	runID string,
	source string,
	now time.Time,
) error {
	if ctx == nil {
		panic("publishTriggerFireRecord: ctx must not be nil")
	}
	if tp == nil {
		panic("publishTriggerFireRecord: tp must not be nil")
	}
	if def.ID == "" {
		panic("publishTriggerFireRecord: def.ID is empty")
	}
	if def.WorkflowID == "" {
		panic("publishTriggerFireRecord: def.WorkflowID is empty")
	}
	fire := TriggerFire{
		TriggerID:  def.ID,
		WorkflowID: def.WorkflowID,
		RunID:      runID,
		Source:     source,
		FiredAt:    now.UTC(),
	}
	fireBytes, err := json.Marshal(fire)
	if err != nil {
		return fmt.Errorf("marshal trigger fire: %w", err)
	}
	fireMsgID := fireDedupID(def.ID, source, now)
	subject := fmt.Sprintf("trigger.fire.%s", def.ID)
	_, err = tp.JSPublish(
		ctx, subject, fireBytes,
		jetstream.WithMsgID(fireMsgID),
	)
	return err
}

// fireDedupID mirrors startedDedupID but with the ".fire" suffix the
// scheduler historically used on the TRIGGER_HISTORY publish.
func fireDedupID(
	triggerID string, source string, now time.Time,
) string {
	if triggerID == "" {
		panic("fireDedupID: triggerID is empty")
	}
	if source == SourceCron {
		return fmt.Sprintf(
			"trigger.%s.%d.fire", triggerID, now.Unix()/60,
		)
	}
	return fmt.Sprintf(
		"trigger.%s.%s.%d.fire",
		triggerID, source, now.UnixNano(),
	)
}
