package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/internal/runid"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// SubjectTrigger subscribes to a NATS subject and publishes workflow.started
// events for each incoming message. The original message payload is embedded
// in the TriggerEnvelope.
type SubjectTrigger struct {
	nc       *nats.Conn
	js       jetstream.JetStream
	def      TriggerDef
	sub      *nats.Subscription
	done     chan struct{}
	debounce *Debouncer
}

// SubjectTriggerOpt configures optional behavior on SubjectTrigger.
type SubjectTriggerOpt func(*SubjectTrigger)

// WithDebouncer attaches a Debouncer for debounce-configured triggers.
func WithDebouncer(d *Debouncer) SubjectTriggerOpt {
	return func(st *SubjectTrigger) { st.debounce = d }
}

// NewSubjectTrigger creates a SubjectTrigger that subscribes to def.Subject.
// Returns error if def lacks Subject config or subscription fails.
// Panics if nc is nil (programmer error).
func NewSubjectTrigger(
	nc *nats.Conn,
	def TriggerDef,
	opts ...SubjectTriggerOpt,
) (*SubjectTrigger, error) {
	if nc == nil {
		panic("NewSubjectTrigger: connection must not be nil")
	}
	if def.ID == "" {
		panic("NewSubjectTrigger: def.ID must not be empty")
	}

	if def.Subject == nil {
		return nil, fmt.Errorf("trigger %q has no subject config", def.ID)
	}
	if def.Subject.Subject == "" {
		return nil, fmt.Errorf("trigger %q: subject must not be empty", def.ID)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	trigger := &SubjectTrigger{
		nc:   nc,
		js:   js,
		def:  def,
		done: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(trigger)
	}

	if def.Enabled {
		sub, err := nc.Subscribe(def.Subject.Subject, trigger.handleMessage)
		if err != nil {
			return nil, fmt.Errorf("subscribe %q: %w", def.Subject.Subject, err)
		}
		trigger.sub = sub
	}

	return trigger, nil
}

// Close unsubscribes and releases resources.
// Panics if done channel is nil (uninitialized trigger).
func (s *SubjectTrigger) Close() error {
	if s.done == nil {
		panic("Close: done channel must not be nil")
	}
	if s.nc == nil {
		panic("Close: connection must not be nil")
	}

	close(s.done)
	if s.sub != nil {
		return s.sub.Unsubscribe()
	}
	return nil
}

// handleMessage processes incoming NATS messages and publishes workflow.started.
// Panics if msg is nil (NATS library invariant).
func (s *SubjectTrigger) handleMessage(msg *nats.Msg) {
	if msg == nil {
		panic("handleMessage: msg must not be nil")
	}
	if s.js == nil {
		panic("handleMessage: JetStream context must not be nil")
	}

	bg := context.Background()
	if !s.def.Enabled {
		RecordFiring(bg, TypeSubject, OutcomeSkipped)
		return
	}

	var data json.RawMessage
	if len(msg.Data) > 0 {
		data = json.RawMessage(msg.Data)
	}

	// Route through debounce if configured
	if s.def.Debounce != nil && s.debounce != nil {
		debounceCtx, debounceCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer debounceCancel()
		fire, eventData, err := s.debounce.DebounceOrFire(
			debounceCtx, s.def, data,
		)
		if err != nil {
			RecordFiring(bg, TypeSubject, OutcomeError)
			return
		}
		if !fire {
			RecordFiring(bg, TypeSubject, OutcomeSkipped)
			return
		}
		data = eventData
	}

	s.publishWorkflowStarted(data)
}

// publishWorkflowStarted wraps data in a TriggerEnvelope and publishes
// it as a workflow.started event to JetStream.
func (s *SubjectTrigger) publishWorkflowStarted(
	data json.RawMessage,
) {
	if s.js == nil {
		panic("publishWorkflowStarted: js must not be nil")
	}

	now := time.Now().UTC()
	envelope := TriggerEnvelope{
		Trigger:    "subject",
		Source:     s.def.ID,
		WorkflowID: s.def.WorkflowID,
		Timestamp:  now,
		Data:       data,
	}

	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		slog.Error("marshal trigger envelope",
			"error", err,
			"trigger_id", s.def.ID)
		return
	}

	runID := runid.New()
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		runID,
		payloadBytes,
	)

	evtBytes, err := evt.Marshal()
	if err != nil {
		slog.Error("marshal workflow event",
			"error", err,
			"trigger_id", s.def.ID)
		return
	}

	pubCtx, pubCancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer pubCancel()
	if _, err := s.js.Publish(
		pubCtx, evt.NATSSubject(), evtBytes,
	); err != nil {
		slog.Error("publish workflow event",
			"error", err,
			"trigger_id", s.def.ID,
			"run_id", runID)
		RecordFiring(pubCtx, TypeSubject, OutcomeError)
		return
	}
	RecordFiring(pubCtx, TypeSubject, OutcomeFired)
}
