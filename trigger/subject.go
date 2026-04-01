package trigger

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// SubjectTrigger subscribes to a NATS subject and publishes workflow.started
// events for each incoming message. The original message payload is embedded
// in the TriggerEnvelope.
type SubjectTrigger struct {
	nc   *nats.Conn
	js   nats.JetStreamContext
	def  TriggerDef
	sub  *nats.Subscription
	done chan struct{}
}

// NewSubjectTrigger creates a SubjectTrigger that subscribes to def.Subject.
// Returns error if def lacks Subject config or subscription fails.
// Panics if nc is nil (programmer error).
func NewSubjectTrigger(nc *nats.Conn, def TriggerDef) (*SubjectTrigger, error) {
	if nc == nil {
		panic("NewSubjectTrigger: connection must not be nil")
	}
	if def.Subject == nil {
		return nil, fmt.Errorf("trigger %q has no subject config", def.ID)
	}
	if def.Subject.Subject == "" {
		return nil, fmt.Errorf("trigger %q: subject must not be empty", def.ID)
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("JetStream: %w", err)
	}

	trigger := &SubjectTrigger{
		nc:   nc,
		js:   js,
		def:  def,
		done: make(chan struct{}),
	}

	if def.Enabled {
		sub, err := nc.Subscribe(def.Subject.Subject, trigger.handleMessage)
		if err != nil {
			return nil, fmt.Errorf("Subscribe %q: %w", def.Subject.Subject, err)
		}
		trigger.sub = sub
	}

	return trigger, nil
}

// Close unsubscribes and releases resources.
func (s *SubjectTrigger) Close() error {
	close(s.done)
	if s.sub != nil {
		return s.sub.Unsubscribe()
	}
	return nil
}

// handleMessage processes incoming NATS messages and publishes workflow.started.
func (s *SubjectTrigger) handleMessage(msg *nats.Msg) {
	if !s.def.Enabled {
		return
	}

	now := time.Now().UTC()

	var data json.RawMessage
	if len(msg.Data) > 0 {
		data = json.RawMessage(msg.Data)
	}

	envelope := TriggerEnvelope{
		Trigger:   "subject",
		Source:    s.def.ID,
		Timestamp: now,
		Data:      data,
	}

	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return
	}

	runID := fmt.Sprintf("%s-%d", s.def.WorkflowID, now.UnixNano())
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		runID,
		payloadBytes,
	)

	evtBytes, err := evt.Marshal()
	if err != nil {
		return
	}

	_, _ = s.js.Publish(evt.NATSSubject(), evtBytes)
}
