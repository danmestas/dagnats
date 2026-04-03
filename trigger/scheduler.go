package trigger

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/errgroup"
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
// Triggers are evaluated and fired concurrently.
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

	var g errgroup.Group
	for _, def := range snapshot {
		if !def.Enabled || def.Cron == nil {
			continue
		}
		def := def
		g.Go(func() error {
			shouldFire, err := s.shouldFire(def, now)
			if err != nil {
				return fmt.Errorf("shouldFire %q: %w", def.ID, err)
			}
			if !shouldFire {
				return nil
			}
			if err := s.fireWorkflow(def, now); err != nil {
				return fmt.Errorf("fireWorkflow %q: %w", def.ID, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// Start runs Tick in a loop at the given interval until ctx is cancelled.
// Blocks until shutdown. Interval should be <= 1 minute for production.
func (s *Scheduler) Start(ctx context.Context, interval time.Duration) {
	if ctx == nil {
		panic("Start: ctx must not be nil")
	}
	if interval <= 0 {
		panic("Start: interval must be positive")
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			_ = s.Tick(now)
		}
	}
}

// Backfill replays missed cron schedules from last_run_at to now for
// triggers with Backfill=true. Caps at 100 fires per trigger to prevent
// flood after long outage. Uses same fireWorkflow for dedup.
func (s *Scheduler) Backfill() error {
	if s.stateKV == nil {
		panic("Backfill: stateKV is nil")
	}

	s.mu.RLock()
	snapshot := make(map[string]TriggerDef, len(s.triggers))
	for k, v := range s.triggers {
		snapshot[k] = v
	}
	s.mu.RUnlock()

	var g errgroup.Group
	for _, def := range snapshot {
		if def.Cron == nil || !def.Cron.Backfill {
			continue
		}
		def := def
		g.Go(func() error {
			if err := s.backfillTrigger(def); err != nil {
				return fmt.Errorf("backfill %q: %w", def.ID, err)
			}
			return nil
		})
	}
	return g.Wait()
}

// backfillTrigger replays missed schedules for a single trigger.
func (s *Scheduler) backfillTrigger(def TriggerDef) error {
	if def.ID == "" {
		panic("backfillTrigger: def.ID is empty")
	}
	if def.Cron == nil {
		panic("backfillTrigger: def.Cron is nil")
	}

	lastRun, err := s.loadLastRun(def.ID)
	if err != nil {
		return fmt.Errorf("loadLastRun: %w", err)
	}
	if lastRun.IsZero() {
		return nil
	}

	now := time.Now().UTC().Truncate(time.Minute)
	matches, err := s.findMatches(def, lastRun, now)
	if err != nil {
		return fmt.Errorf("findMatches: %w", err)
	}

	fireCount := len(matches)
	if fireCount > 100 {
		fireCount = 100
	}

	for i := 0; i < fireCount; i++ {
		if err := s.fireWorkflow(def, matches[i]); err != nil {
			return fmt.Errorf("fire %v: %w", matches[i], err)
		}
	}

	return nil
}

// loadLastRun retrieves the last_run_at timestamp from trigger_state KV.
// Returns zero time if key doesn't exist (no previous run).
func (s *Scheduler) loadLastRun(triggerID string) (time.Time, error) {
	if triggerID == "" {
		panic("loadLastRun: triggerID is empty")
	}

	key := fmt.Sprintf("%s.last_run_at", triggerID)
	entry, err := s.stateKV.Get(key)
	if err != nil {
		if err == nats.ErrKeyNotFound {
			return time.Time{}, nil
		}
		return time.Time{}, fmt.Errorf("KV Get: %w", err)
	}

	lastRun, err := time.Parse(time.RFC3339, string(entry.Value()))
	if err != nil {
		return time.Time{}, fmt.Errorf("parse time: %w", err)
	}

	return lastRun, nil
}

// findMatches returns all minute timestamps from start (exclusive) to
// end (inclusive) that match the cron expression. Iterative, no recursion.
// Bounded by maximum 10000 iterations to prevent unbounded loops.
func (s *Scheduler) findMatches(
	def TriggerDef, start, end time.Time,
) ([]time.Time, error) {
	if def.Cron == nil {
		panic("findMatches: def.Cron is nil")
	}

	expr, err := ParseCron(def.Cron.Expression)
	if err != nil {
		return nil, fmt.Errorf("ParseCron: %w", err)
	}

	loc, err := time.LoadLocation(def.Cron.Timezone)
	if err != nil {
		return nil, fmt.Errorf("LoadLocation: %w", err)
	}

	const maxIterations = 10000
	var matches []time.Time
	current := start.Add(time.Minute).Truncate(time.Minute)

	for i := 0; i < maxIterations && !current.After(end); i++ {
		localTime := current.In(loc)
		if expr.Matches(localTime) {
			matches = append(matches, current)
		}
		current = current.Add(time.Minute)
	}

	return matches, nil
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
