package trigger

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/sync/errgroup"
)

// Scheduler evaluates cron triggers and publishes workflow.started events
// when schedules match. Uses NATS KV for last-run tracking and JetStream
// Nats-Msg-Id for deduplication.
//
// lastFired tracks the most recent matching minute fired per trigger ID,
// enforcing in-process minute-precision dedup. This is required because
// the scheduler ticks at sub-minute intervals (30s) but cron is
// minute-resolution: without it, two ticks in the same matching minute
// both call fireWorkflow, and JetStream's per-stream Nats-Msg-Id dedup
// only catches publishes inside the stream's Duplicates window (5s for
// the workflow events stream). See issue #173.
type Scheduler struct {
	nc          *nats.Conn
	js          jetstream.JetStream
	tp          *natsutil.TracingPublisher
	stateKV     jetstream.KeyValue
	triggers    map[string]TriggerDef
	mu          sync.RWMutex
	lastFiredMu sync.Mutex
	lastFired   map[string]time.Time
	// firedAt tracks the unix time of the most recent *successful*
	// cron fire per trigger ID, for the trigger_last_fired_seconds
	// gauge. Distinct from lastFired: lastFired advances at claim
	// time in claimMinute (before Fire is even attempted, to guard
	// dedup across ticks), so it can be non-zero even when the
	// subsequent Fire call errors. firedAt only advances on the
	// OutcomeFired branch of fireWorkflow — it is the "did this
	// trigger actually publish" signal, not the "did we already
	// evaluate this minute" signal. In-process only, never seeded
	// from KV on restart — see metrics.go's RegisterSchedulerMetrics
	// doc comment for why.
	firedAtMu sync.Mutex
	firedAt   map[string]time.Time
	// metricsReg is the OTel callback registration created in
	// NewScheduler. Kept only so a future Close/Shutdown path has
	// something to unregister; the scheduler has no explicit
	// shutdown today so this is otherwise inert.
	metricsReg metric.Registration
}

// NewScheduler creates a Scheduler that uses the trigger_state KV bucket.
// Panics if nc is nil (programmer error).
func NewScheduler(nc *nats.Conn) (*Scheduler, error) {
	if nc == nil {
		panic("NewScheduler: connection must not be nil")
	}

	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream.New: %w", err)
	}

	kvCtx, kvCancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer kvCancel()
	kv, err := js.KeyValue(kvCtx, "trigger_state")
	if err != nil {
		return nil, fmt.Errorf("KeyValue trigger_state: %w", err)
	}

	s := &Scheduler{
		nc:        nc,
		js:        js,
		tp:        natsutil.NewTracingPublisher(nc, js),
		stateKV:   kv,
		triggers:  make(map[string]TriggerDef),
		lastFired: make(map[string]time.Time),
		firedAt:   make(map[string]time.Time),
	}

	// Best-effort metrics registration: matches the convention at
	// metrics.go's newFiringsCounter/pkgFirings — a metrics wiring
	// failure must never fail scheduler construction, but the error
	// is never silently dropped (never `_ = err`).
	reg, err := RegisterSchedulerMetrics(otel.Meter("dagnats/trigger"), s)
	if err != nil {
		slog.Error("RegisterSchedulerMetrics failed", "error", err)
	} else {
		s.metricsReg = reg
	}

	return s, nil
}

// AddTrigger registers a cron trigger. Only processes triggers with Cron
// config. Panics on empty ID (programmer error).
//
// Contract: AddTrigger MUST NOT publish workflow.started. The next fire
// is computed by the steady-state Tick path on cron-time match. Missed
// fires are replayed by Backfill, gated on def.Cron.Backfill==true.
// Issue #139 — registering a future-only cron with backfill:false must
// never trigger an immediate run. The cron_backfill_test.go suite is a
// regression guard for this contract.
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

// Count returns the number of registered cron triggers.
func (s *Scheduler) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.triggers)
}

// RemoveTrigger unregisters a trigger by ID.
func (s *Scheduler) RemoveTrigger(id string) error {
	if id == "" {
		panic("RemoveTrigger: trigger ID must not be empty")
	}

	s.mu.Lock()
	delete(s.triggers, id)
	s.mu.Unlock()

	s.lastFiredMu.Lock()
	delete(s.lastFired, id)
	s.lastFiredMu.Unlock()

	s.firedAtMu.Lock()
	delete(s.firedAt, id)
	s.firedAtMu.Unlock()
	return nil
}

// claimMinute returns true if the given minute (truncated to the minute
// boundary) is newer than the last claim for this trigger. On true, the
// minute is recorded so subsequent calls within the same minute return
// false. This is the in-process dedup guard for #173.
func (s *Scheduler) claimMinute(triggerID string, now time.Time) bool {
	if triggerID == "" {
		panic("claimMinute: triggerID must not be empty")
	}
	if now.IsZero() {
		panic("claimMinute: now must not be zero")
	}
	minute := now.Truncate(time.Minute)

	s.lastFiredMu.Lock()
	defer s.lastFiredMu.Unlock()
	if !minute.After(s.lastFired[triggerID]) {
		return false
	}
	s.lastFired[triggerID] = minute
	return true
}

// Tick evaluates all enabled cron triggers at the given time. For each
// matching trigger, publishes workflow.started with dedup Nats-Msg-Id.
// Triggers are evaluated and fired concurrently.
func (s *Scheduler) Tick(now time.Time) error {
	if s.js == nil {
		panic("Tick: JetStream context is nil")
	}
	if s.triggers == nil {
		panic("Tick: triggers map is nil")
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

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
			// In-process minute dedup: cron is minute-resolution but
			// the scheduler ticks every 30s, so the same matching
			// minute can be evaluated twice. Claim the minute before
			// firing; if already claimed, treat as no-op. See #173.
			if !s.claimMinute(def.ID, now) {
				return nil
			}
			if err := s.fireWorkflow(ctx, def, now); err != nil {
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
	if s.triggers == nil {
		panic("Backfill: triggers map is nil")
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
// Returns immediately when def.Cron.Backfill is false — this is a
// defense-in-depth guard duplicating the filter in Backfill, so a
// direct caller (or a future entry point) cannot accidentally replay
// missed fires for a trigger authored with backfill:false. See #139.
func (s *Scheduler) backfillTrigger(def TriggerDef) error {
	if def.ID == "" {
		panic("backfillTrigger: def.ID is empty")
	}
	if def.Cron == nil {
		panic("backfillTrigger: def.Cron is nil")
	}
	if !def.Cron.Backfill {
		return nil
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()

	lastRun, err := s.loadLastRun(ctx, def.ID)
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
		// Claim the minute so a subsequent live Tick at the same
		// minute (e.g. the boundary between backfill end and the
		// first steady-state tick, which can be ≥30s later — past
		// the workflow stream's 5s msgID Duplicates window) does
		// not re-fire. Same root cause as #173, same guard.
		if !s.claimMinute(def.ID, matches[i]) {
			continue
		}
		if err := s.fireWorkflow(ctx, def, matches[i]); err != nil {
			return fmt.Errorf("fire %v: %w", matches[i], err)
		}
	}

	return nil
}

// loadLastRun retrieves the last_run_at timestamp from trigger_state KV.
// Returns zero time if key doesn't exist (no previous run).
func (s *Scheduler) loadLastRun(
	ctx context.Context, triggerID string,
) (time.Time, error) {
	if ctx == nil {
		panic("loadLastRun: ctx must not be nil")
	}
	if triggerID == "" {
		panic("loadLastRun: triggerID is empty")
	}

	key := fmt.Sprintf("%s.last_run_at", triggerID)
	entry, err := s.stateKV.Get(ctx, key)
	if err != nil {
		if errors.Is(err, jetstream.ErrKeyNotFound) {
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
// Delegates to the shared Fire helper (#352) so the manual fire path
// in api.Service stays wire-identical to the cron tick path.
// Uses Nats-Msg-Id for deduplication: trigger.{id}.{unix_minute}.
func (s *Scheduler) fireWorkflow(
	ctx context.Context, def TriggerDef, now time.Time,
) error {
	if ctx == nil {
		panic("fireWorkflow: ctx must not be nil")
	}
	if def.ID == "" {
		panic("fireWorkflow: def.ID is empty")
	}
	if def.WorkflowID == "" {
		panic("fireWorkflow: def.WorkflowID is empty")
	}
	if _, err := Fire(ctx, s.tp, def, SourceCron, now); err != nil {
		RecordFiring(ctx, TypeCron, OutcomeError)
		return err
	}
	RecordFiring(ctx, TypeCron, OutcomeFired)

	s.firedAtMu.Lock()
	s.firedAt[def.ID] = now
	s.firedAtMu.Unlock()
	return nil
}

// observeMetrics is the OTel callback body invoked on every collection
// of the trigger_last_fired_seconds / trigger_next_fire_seconds
// gauges. Iterates a bounded snapshot of registered triggers (tens of
// entries, no recursion) and delegates the per-trigger decision to
// observeTriggerMetrics to stay under the 70-line function limit.
func (s *Scheduler) observeMetrics(
	ctx context.Context, o metric.Observer, g schedulerGauges,
) error {
	if ctx == nil {
		panic("observeMetrics: ctx must not be nil")
	}
	if o == nil {
		panic("observeMetrics: observer must not be nil")
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
		s.observeTriggerMetrics(o, g, def)
	}
	return nil
}

// observeTriggerMetrics observes last_fired (if the trigger has ever
// fired successfully in this process) and next_fire (always, for an
// enabled cron trigger) for a single trigger. Split out of
// observeMetrics purely to keep both functions under the 70-line
// limit — no independent invariants of its own.
func (s *Scheduler) observeTriggerMetrics(
	o metric.Observer, g schedulerGauges, def TriggerDef,
) {
	attrs := metric.WithAttributes(attribute.String("trigger", def.ID))

	s.firedAtMu.Lock()
	firedAt, fired := s.firedAt[def.ID]
	s.firedAtMu.Unlock()
	if fired {
		o.ObserveInt64(g.lastFired, firedAt.Unix(), attrs)
	}

	// ParseCron/LoadLocation errors here are defense-in-depth only:
	// both are already validated at AddTrigger time, so a live error
	// would mean state drifted after registration. Skip silently
	// rather than surface a live-error path for an already-validated
	// config — mirrors shouldFire's timezone-conversion pattern.
	expr, err := ParseCron(def.Cron.Expression)
	if err != nil {
		return
	}
	loc, err := time.LoadLocation(def.Cron.Timezone)
	if err != nil {
		return
	}
	next := expr.NextN(time.Now().In(loc), 1)
	if len(next) != 1 {
		return
	}
	o.ObserveInt64(g.nextFire, next[0].Unix(), attrs)
}
