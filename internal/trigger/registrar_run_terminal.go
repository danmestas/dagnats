// internal/trigger/registrar_run_terminal.go
// runTerminalRegistrar owns the run_terminal trigger table (#634).
// Unlike subjectRegistrar (core NATS ephemeral subscribe), each
// run_terminal trigger gets its own DURABLE JetStream consumer on the
// EVENTS stream: the source of truth for "did this trigger already
// react to run X's completion" must survive an engine restart, or a
// redelivered event.run.* message after a crash mid-fire would be
// silently dropped instead of retried — mirroring
// internal/engine/correlator.go's durable "event-correlator"
// consumer for the same stream.
package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// runTerminalFireTimeout bounds the workflow.started publish a
// matching RunEvent triggers. Short: this is a local JetStream
// publish, not a network call to an external system.
const runTerminalFireTimeout = 5 * time.Second

// runTerminalTrigger holds the live consumer for one run_terminal
// TriggerDef.
type runTerminalTrigger struct {
	def TriggerDef
	cc  jetstream.ConsumeContext
}

// runTerminalRegistrar implements TriggerRegistrar for run_terminal.
// ADR-016.
type runTerminalRegistrar struct {
	js       jetstream.JetStream
	tp       *natsutil.TracingPublisher
	triggers map[string]*runTerminalTrigger
	mu       sync.RWMutex
}

// newRunTerminalRegistrar builds the registrar. Panics on nil js/tp —
// programmer errors, matching every other registrar constructor.
func newRunTerminalRegistrar(
	js jetstream.JetStream, tp *natsutil.TracingPublisher,
) *runTerminalRegistrar {
	if js == nil {
		panic("newRunTerminalRegistrar: js must not be nil")
	}
	if tp == nil {
		panic("newRunTerminalRegistrar: tp must not be nil")
	}
	return &runTerminalRegistrar{
		js:       js,
		tp:       tp,
		triggers: make(map[string]*runTerminalTrigger),
	}
}

// ValidateConfig delegates to the shared validator.
func (r *runTerminalRegistrar) ValidateConfig(def TriggerDef) error {
	if def.RunTerminal == nil {
		return fmt.Errorf("trigger %q: run_terminal config missing", def.ID)
	}
	return validateRunTerminalConfig(def)
}

// Activate creates (or reuses) a durable consumer on EVENTS filtered
// to event.run.{sanitized source workflow}.*.* and starts consuming.
// Idempotent: a second call with the same def.ID is a no-op, matching
// every other registrar's contract (TriggerRegistrar's doc comment).
func (r *runTerminalRegistrar) Activate(
	ctx context.Context, def TriggerDef,
) error {
	if def.ID == "" {
		panic("runTerminalRegistrar.Activate: def.ID must not be empty")
	}
	if def.RunTerminal == nil {
		return fmt.Errorf("trigger %q: run_terminal config missing", def.ID)
	}

	r.mu.Lock()
	if _, exists := r.triggers[def.ID]; exists {
		r.mu.Unlock()
		return nil
	}
	r.mu.Unlock()

	stream, err := r.js.Stream(ctx, "EVENTS")
	if err != nil {
		return fmt.Errorf("stream EVENTS: %w", err)
	}
	durable := "run-terminal-" + sanitizeSubjectToken(def.ID)
	cons, err := stream.CreateOrUpdateConsumer(
		ctx, jetstream.ConsumerConfig{
			Durable: durable,
			FilterSubject: runTerminalSubject(
				def.RunTerminal.Workflow,
			),
			AckPolicy:     jetstream.AckExplicitPolicy,
			DeliverPolicy: jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		return fmt.Errorf("consumer %s: %w", durable, err)
	}

	rt := &runTerminalTrigger{def: def}
	cc, err := cons.Consume(func(msg jetstream.Msg) {
		r.handleRunEvent(def, msg)
	})
	if err != nil {
		return fmt.Errorf("consume %s: %w", durable, err)
	}
	rt.cc = cc

	r.mu.Lock()
	r.triggers[def.ID] = rt
	r.mu.Unlock()
	return nil
}

// Deactivate stops the trigger's ConsumeContext and removes it from
// the table. Idempotent. The durable consumer itself is left on the
// server (matching the register-then-forget lifecycle every other
// registrar follows — a re-Activate of the same def.ID reuses it via
// CreateOrUpdateConsumer rather than erroring on "already exists").
func (r *runTerminalRegistrar) Deactivate(
	_ context.Context, def TriggerDef,
) error {
	if def.ID == "" {
		panic("runTerminalRegistrar.Deactivate: def.ID must not be empty")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	rt, ok := r.triggers[def.ID]
	if !ok {
		return nil
	}
	if rt.cc != nil {
		rt.cc.Stop()
	}
	delete(r.triggers, def.ID)
	return nil
}

// handleRunEvent processes one event.run.* message for trigger def:
// decode, status-filter, depth-cap, fire, ack. Every exit path acks
// or naks exactly once — see the inline comments at each return.
func (r *runTerminalRegistrar) handleRunEvent(
	def TriggerDef, msg jetstream.Msg,
) {
	if msg == nil {
		panic("handleRunEvent: msg must not be nil")
	}
	if def.RunTerminal == nil {
		panic("handleRunEvent: def.RunTerminal must not be nil")
	}

	ctx := context.Background()
	var evt protocol.RunEvent
	if err := json.Unmarshal(msg.Data(), &evt); err != nil {
		// A malformed event.run.* body is an engine-internal producer
		// bug, not something redelivery will fix — ack it so it does
		// not jam this trigger's consumer forever, and log loudly.
		slog.ErrorContext(ctx,
			"run_terminal: unmarshal RunEvent failed",
			"error", err, "trigger_id", def.ID,
		)
		_ = msg.Ack()
		RecordFiring(ctx, TypeRunTerminal, OutcomeError)
		return
	}

	if !def.RunTerminal.matchesStatus(evt.Status) {
		_ = msg.Ack()
		RecordFiring(ctx, TypeRunTerminal, OutcomeSkipped)
		return
	}

	nextDepth := evt.TriggerDepth + 1
	if nextDepth > TriggerDepthMax {
		slog.WarnContext(ctx,
			"run_terminal: refusing chained run — TriggerDepthMax exceeded",
			"trigger_id", def.ID,
			"source_run_id", evt.RunID,
			"source_workflow_id", evt.WorkflowID,
			"target_workflow_id", def.WorkflowID,
			"next_depth", nextDepth,
			"trigger_depth_max", TriggerDepthMax,
		)
		RecordDepthRefusal(ctx)
		_ = msg.Ack()
		RecordFiring(ctx, TypeRunTerminal, OutcomeSkipped)
		return
	}

	if _, err := fireRunTerminal(ctx, r.tp, def, evt, nextDepth); err != nil {
		slog.ErrorContext(ctx,
			"run_terminal: fire failed",
			"error", err, "trigger_id", def.ID,
			"source_run_id", evt.RunID,
		)
		// Nak (not Ack): the publish may have failed transiently
		// (NATS hiccup) — redelivery retries. If it actually
		// succeeded and only the ack path failed, JSPublish's
		// Nats-Msg-Id dedup on the msgID below (fireRunTerminal's
		// "trig-"+triggerID+"-"+sourceRunID) collapses the retry to
		// the same started run rather than double-starting it.
		_ = msg.Nak()
		RecordFiring(ctx, TypeRunTerminal, OutcomeError)
		return
	}
	_ = msg.Ack()
	RecordFiring(ctx, TypeRunTerminal, OutcomeFired)
}

// runTerminalChainPayload is the wire shape internal/engine's
// decodeRunTerminalChainPayload recognizes (orchestrator.go). Trigger
// literal "run_terminal" distinguishes it from the generic
// TriggerEnvelope every other trigger type publishes — see that
// function's doc comment for why the two shapes can't share a
// decoder.
type runTerminalChainPayload struct {
	Trigger      string          `json:"trigger"`
	Source       string          `json:"source"`
	WorkflowID   string          `json:"workflow_id"`
	Input        json.RawMessage `json:"input"`
	TriggerDepth int             `json:"trigger_depth"`
}

// runTerminalChainInput is the flat {run_id, workflow_id, status,
// labels} object the issue specifies as the started run's Input.
type runTerminalChainInput struct {
	RunID      string            `json:"run_id"`
	WorkflowID string            `json:"workflow_id"`
	Status     string            `json:"status"`
	Labels     map[string]string `json:"labels,omitempty"`
}

// fireRunTerminal publishes workflow.started for def's target
// workflow, in reaction to source event evt. Returns the new run ID.
//
// Nats-Msg-Id is "trig-"+def.ID+"-"+evt.RunID (the issue's exact
// dedup contract): keyed on the SOURCE run, not a freshly minted ID,
// so a redelivered event.run.* message — the durable consumer's
// crash-recovery path — collapses to the SAME started run instead of
// starting a second one. This is why fireRunTerminal cannot reuse
// fire.go's Fire(): Fire mints its own dedup ID scheme (cron-minute-
// bucket or manual-nanosecond) that has no notion of "the source
// event that caused this fire," and Fire's envelope shape has no
// TriggerDepth field.
func fireRunTerminal(
	ctx context.Context,
	tp *natsutil.TracingPublisher,
	def TriggerDef,
	evt protocol.RunEvent,
	depth int,
) (string, error) {
	if ctx == nil {
		panic("fireRunTerminal: ctx must not be nil")
	}
	if tp == nil {
		panic("fireRunTerminal: tp must not be nil")
	}
	if def.ID == "" {
		panic("fireRunTerminal: def.ID must not be empty")
	}

	inputBytes, err := json.Marshal(runTerminalChainInput{
		RunID:      evt.RunID,
		WorkflowID: evt.WorkflowID,
		Status:     evt.Status,
		Labels:     evt.Labels,
	})
	if err != nil {
		return "", fmt.Errorf("marshal chain input: %w", err)
	}
	payloadBytes, err := json.Marshal(runTerminalChainPayload{
		Trigger:      "run_terminal",
		Source:       def.ID,
		WorkflowID:   def.WorkflowID,
		Input:        inputBytes,
		TriggerDepth: depth,
	})
	if err != nil {
		return "", fmt.Errorf("marshal chain payload: %w", err)
	}

	newRunID := runid.New()
	startedEvt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted, newRunID, payloadBytes,
	)
	msgID := "trig-" + def.ID + "-" + evt.RunID
	msg := &nats.Msg{Subject: startedEvt.NATSSubject()}

	pubCtx, cancel := context.WithTimeout(ctx, runTerminalFireTimeout)
	defer cancel()
	if _, err := tp.JSPublishMsgEvent(
		pubCtx, msg, &startedEvt, jetstream.WithMsgID(msgID),
	); err != nil {
		return "", fmt.Errorf("publish workflow.started: %w", err)
	}

	if err := publishTriggerFireRecord(
		pubCtx, tp, def, newRunID, "run_terminal", time.Now(),
	); err != nil {
		// The workflow.started publish above already succeeded — the
		// run WILL start. A TRIGGER_HISTORY record failure is
		// observability-only, matching Fire()'s own treatment of this
		// same call (fire.go). Do not fail the caller over it.
		slog.WarnContext(pubCtx,
			"run_terminal: publish trigger fire record failed",
			"error", err, "trigger_id", def.ID, "run_id", newRunID,
		)
	}
	return newRunID, nil
}
