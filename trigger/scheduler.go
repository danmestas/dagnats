package trigger

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

// Scheduler evaluates cron triggers and publishes workflow.started events
// when schedules match. Uses NATS KV for last-run tracking and JetStream
// Nats-Msg-Id for deduplication.
type Scheduler struct {
	nc       *nats.Conn
	js       nats.JetStreamContext
	stateKV  nats.KeyValue
	triggers map[string]TriggerDef
	mu       sync.RWMutex
}

// NewScheduler creates a Scheduler that uses the trigger_state KV bucket.
// Panics if nc is nil (programmer error).
func NewScheduler(nc *nats.Conn) (*Scheduler, error) {
	if nc == nil {
		panic("NewScheduler: connection must not be nil")
	}

	js, err := nc.JetStream()
	if err != nil {
		return nil, fmt.Errorf("JetStream: %w", err)
	}

	kv, err := js.KeyValue("trigger_state")
	if err != nil {
		return nil, fmt.Errorf("KeyValue trigger_state: %w", err)
	}

	return &Scheduler{
		nc:       nc,
		js:       js,
		stateKV:  kv,
		triggers: make(map[string]TriggerDef),
	}, nil
}

// AddTrigger registers a cron trigger. Only processes triggers with Cron
// config. Panics on empty ID (programmer error).
func (s *Scheduler) AddTrigger(def TriggerDef) error {
	if def.ID == "" {
		panic("AddTrigger: trigger ID must not be empty")
	}
	if def.Cron == nil {
		return fmt.Errorf("AddTrigger: trigger %q has no cron config", def.ID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.triggers[def.ID] = def
	return nil
}

// RemoveTrigger unregisters a trigger by ID.
func (s *Scheduler) RemoveTrigger(id string) error {
	if id == "" {
		panic("RemoveTrigger: trigger ID must not be empty")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.triggers, id)
	return nil
}

// Tick evaluates all enabled cron triggers at the given time. For each
// matching trigger, publishes workflow.started with dedup Nats-Msg-Id.
// Returns first publish error encountered.
func (s *Scheduler) Tick(now time.Time) error {
	if s.js == nil {
		panic("Tick: JetStream context is nil")
	}

	s.mu.RLock()
	snapshot := make(map[string]TriggerDef, len(s.triggers))
	for k, v := range s.triggers {
		snapshot[k] = v
	}
	s.mu.RUnlock()

	for _, def := range snapshot {
		if !def.Enabled || def.Cron == nil {
			continue
		}

		shouldFire, err := s.shouldFire(def, now)
		if err != nil {
			return fmt.Errorf("shouldFire %q: %w", def.ID, err)
		}
		if !shouldFire {
			continue
		}

		if err := s.fireWorkflow(def, now); err != nil {
			return fmt.Errorf("fireWorkflow %q: %w", def.ID, err)
		}
	}
	return nil
}

// Start runs Tick in a loop at the given interval until stopChan closes.
// Blocks until shutdown. Interval should be <= 1 minute for production.
func (s *Scheduler) Start(interval time.Duration, stopChan <-chan struct{}) {
	if interval <= 0 {
		panic("Start: interval must be positive")
	}
	if stopChan == nil {
		panic("Start: stopChan must not be nil")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-stopChan:
			return
		case now := <-ticker.C:
			_ = s.Tick(now)
		}
	}
}

// shouldFire returns true if the trigger matches the given time in its
// configured timezone.
func (s *Scheduler) shouldFire(def TriggerDef, now time.Time) (bool, error) {
	if def.Cron == nil {
		panic("shouldFire: def.Cron is nil")
	}

	expr, err := ParseCron(def.Cron.Expression)
	if err != nil {
		return false, fmt.Errorf("ParseCron: %w", err)
	}

	loc, err := time.LoadLocation(def.Cron.Timezone)
	if err != nil {
		return false, fmt.Errorf("LoadLocation %q: %w", def.Cron.Timezone, err)
	}

	localTime := now.In(loc)
	return expr.Matches(localTime), nil
}

// fireWorkflow publishes workflow.started with TriggerEnvelope payload.
// Uses Nats-Msg-Id for deduplication: trigger.{id}.{unix_minute}.
func (s *Scheduler) fireWorkflow(def TriggerDef, now time.Time) error {
	if def.ID == "" {
		panic("fireWorkflow: def.ID is empty")
	}
	if def.WorkflowID == "" {
		panic("fireWorkflow: def.WorkflowID is empty")
	}

	envelope := TriggerEnvelope{
		Trigger:   "cron",
		Source:    def.ID,
		Timestamp: now.UTC(),
	}
	payloadBytes, err := json.Marshal(envelope)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}

	runID := fmt.Sprintf("%s-%d", def.WorkflowID, now.UnixNano())
	evt := protocol.NewWorkflowEvent(
		protocol.EventWorkflowStarted,
		runID,
		payloadBytes,
	)

	evtBytes, err := evt.Marshal()
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	minuteTimestamp := now.Unix() / 60
	msgID := fmt.Sprintf("trigger.%s.%d", def.ID, minuteTimestamp)

	_, err = s.js.Publish(evt.NATSSubject(), evtBytes, nats.MsgId(msgID))
	if err != nil {
		return fmt.Errorf("publish: %w", err)
	}

	return nil
}
