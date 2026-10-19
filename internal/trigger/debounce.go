// trigger/debounce.go
// Debounce delays workflow execution until events stop arriving.
// Uses KV for state and SLEEP_TIMERS for durable timers. Stale timers
// self-discard via sequence comparison — no explicit cancel needed.
package trigger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// debounceEntry is the KV value for a pending debounce window.
type debounceEntry struct {
	LastEvent   json.RawMessage `json:"last_event"`
	FirstSeenNs int64           `json:"first_seen_ns"`
	TimerSeq    uint64          `json:"timer_seq"`
}

// Debouncer manages debounce state for subject triggers. Window state
// lives in the debounce_state KV bucket and each window's timer is a
// durable SLEEP_TIMERS message, so a window survives restarts and every
// Debouncer on the same JetStream sees the same state. HandleTimerFire
// starts the run itself, reading the trigger from the triggers bucket, so
// whichever process consumes the timer (the orchestrator's SleepTimer,
// via engine.WithDebounceHandler) can fire it.
type Debouncer struct {
	stateKV   jetstream.KeyValue
	triggerKV jetstream.KeyValue
	tp        *natsutil.TracingPublisher
}

// NewDebouncer creates a Debouncer over the debounce_state and triggers
// buckets (natsutil.SetupAll provisions both). Panics on nil arguments.
func NewDebouncer(
	js jetstream.JetStream, tp *natsutil.TracingPublisher,
) (*Debouncer, error) {
	if js == nil {
		panic("NewDebouncer: js must not be nil")
	}
	if tp == nil {
		panic("NewDebouncer: tp must not be nil")
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	stateKV, err := js.KeyValue(ctx, "debounce_state")
	if err != nil {
		return nil, fmt.Errorf("debounce_state KV: %w", err)
	}
	triggerKV, err := js.KeyValue(ctx, "triggers")
	if err != nil {
		return nil, fmt.Errorf("triggers KV: %w", err)
	}
	return &Debouncer{stateKV: stateKV, triggerKV: triggerKV, tp: tp}, nil
}

// NewDebounceTimerHandler returns the handler a process consuming
// SLEEP_TIMERS passes to engine.WithDebounceHandler, so debounced subject
// triggers start their runs. Every engine process should wire it: the
// sleep-timer consumer is shared, so a timer may reach any of them.
func NewDebounceTimerHandler(nc *nats.Conn) (engine.DebounceHandler, error) {
	if nc == nil {
		panic("NewDebounceTimerHandler: nc must not be nil")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, fmt.Errorf("jetstream.New: %w", err)
	}
	d, err := NewDebouncer(js, natsutil.NewTracingPublisher(nc, js))
	if err != nil {
		return nil, err
	}
	if d.triggerKV == nil {
		panic("NewDebounceTimerHandler: triggers bucket must be open")
	}
	return d.HandleTimerFire, nil
}

// debounceKey computes the KV key for a debounce window. If the
// trigger has a Key config, extracts the value from the event data
// using dot-path. Otherwise uses the trigger ID alone.
func debounceKey(def TriggerDef, data json.RawMessage) string {
	if def.ID == "" {
		panic("debounceKey: def.ID must not be empty")
	}
	if def.Debounce == nil {
		panic("debounceKey: def.Debounce must not be nil")
	}
	if def.Debounce.Key == "" {
		return def.ID
	}
	val, err := dag.ExtractDotPath(def.Debounce.Key, data)
	if err != nil || val == nil {
		return def.ID
	}
	extracted := fmt.Sprintf("%v", val)
	if extracted == "" {
		return def.ID
	}
	return def.ID + "." + extracted
}

// DebounceOrFire handles an incoming event for a debounced trigger.
// Either absorbs the event into the debounce window (returns false)
// or fires immediately due to hard timeout (returns true with the
// event data to use). If debounce is nil, returns true immediately.
func (d *Debouncer) DebounceOrFire(
	ctx context.Context, def TriggerDef, eventData json.RawMessage,
) (fire bool, data json.RawMessage, err error) {
	if ctx == nil {
		panic("DebounceOrFire: ctx must not be nil")
	}
	if def.Debounce == nil {
		return true, eventData, nil
	}

	key := debounceKey(def, eventData)
	now := time.Now()

	// Try to load existing entry
	entry, err := d.stateKV.Get(ctx, key)
	if err != nil && !errors.Is(err, jetstream.ErrKeyNotFound) {
		return false, nil, fmt.Errorf("get debounce_state: %w", err)
	}

	if entry != nil {
		// Existing window — check hard timeout
		var existing debounceEntry
		if err := json.Unmarshal(entry.Value(), &existing); err != nil {
			return false, nil, fmt.Errorf(
				"unmarshal debounce entry: %w", err,
			)
		}
		firstSeen := time.Unix(
			0, existing.FirstSeenNs,
		)
		if def.Debounce.Timeout > 0 &&
			now.Sub(firstSeen) >= def.Debounce.Timeout {
			// Hard timeout — fire with latest event
			if err := d.stateKV.Delete(ctx, key); err != nil {
				return false, nil, fmt.Errorf(
					"delete debounce_state: %w", err,
				)
			}
			return true, eventData, nil
		}

		// Reset the window — update event data and schedule new timer
		return false, nil, d.updateDebounceEntry(
			ctx, key, eventData, existing.FirstSeenNs,
			def, entry.Revision(),
		)
	}

	// New window — create entry and schedule timer
	return false, nil, d.createDebounceEntry(
		ctx, key, eventData, now.UnixNano(), def,
	)
}

// createDebounceEntry creates a new debounce KV entry and schedules
// a timer. This is the ONLY code path that creates entries.
func (d *Debouncer) createDebounceEntry(
	ctx context.Context,
	key string,
	eventData json.RawMessage,
	firstSeenNs int64,
	def TriggerDef,
) error {
	if ctx == nil {
		panic("createDebounceEntry: ctx must not be nil")
	}
	if key == "" {
		panic("createDebounceEntry: key must not be empty")
	}

	// Schedule timer first to get the sequence
	seq, err := engine.ScheduleDebounce(ctx, d.tp, engine.TimerMessage{
		Action:      engine.TimerActionDebounce,
		TriggerID:   def.ID,
		DebounceKey: key,
		DurationMs:  def.Debounce.Period.Milliseconds(),
	})
	if err != nil {
		return fmt.Errorf("schedule debounce timer: %w", err)
	}

	entry := debounceEntry{
		LastEvent:   eventData,
		FirstSeenNs: firstSeenNs,
		TimerSeq:    seq,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal debounce entry: %w", err)
	}
	_, err = d.stateKV.Create(ctx, key, data)
	if err != nil {
		return fmt.Errorf("create debounce_state: %w", err)
	}
	return nil
}

// updateDebounceEntry updates an existing debounce entry with new
// event data and a new timer. Uses optimistic locking via revision.
// Invariant: every write updates the TimerSeq. This is the ONLY
// code path that updates entries.
func (d *Debouncer) updateDebounceEntry(
	ctx context.Context,
	key string,
	eventData json.RawMessage,
	firstSeenNs int64,
	def TriggerDef,
	revision uint64,
) error {
	if ctx == nil {
		panic("updateDebounceEntry: ctx must not be nil")
	}
	if key == "" {
		panic("updateDebounceEntry: key must not be empty")
	}

	// Schedule new timer first to get the sequence
	seq, err := engine.ScheduleDebounce(ctx, d.tp, engine.TimerMessage{
		Action:      engine.TimerActionDebounce,
		TriggerID:   def.ID,
		DebounceKey: key,
		DurationMs:  def.Debounce.Period.Milliseconds(),
	})
	if err != nil {
		return fmt.Errorf("schedule debounce timer: %w", err)
	}

	entry := debounceEntry{
		LastEvent:   eventData,
		FirstSeenNs: firstSeenNs,
		TimerSeq:    seq,
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal debounce entry: %w", err)
	}
	_, err = d.stateKV.Update(ctx, key, data, revision)
	if err != nil {
		return fmt.Errorf("update debounce_state: %w", err)
	}
	return nil
}

// HandleTimerFire is called by the SleepTimer when a debounce timer
// fires. Checks the KV entry's TimerSeq against the timer's stream
// sequence to detect stale timers. If fresh, publishes workflow.started
// with the window's last event and deletes the entry. A trigger deleted
// or disabled while its window was open starts no run.
func (d *Debouncer) HandleTimerFire(
	tm engine.TimerMessage, timerSeq uint64,
) {
	if d.stateKV == nil {
		panic("HandleTimerFire: stateKV must not be nil")
	}
	if tm.DebounceKey == "" || tm.TriggerID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	entry, err := d.stateKV.Get(ctx, tm.DebounceKey)
	if err != nil {
		return // entry gone — already fired or cleaned up
	}
	var de debounceEntry
	if err := json.Unmarshal(entry.Value(), &de); err != nil {
		slog.Warn("debounce: corrupt window entry",
			"key", tm.DebounceKey, "error", err)
		return
	}
	// Stale timer check: if the entry's TimerSeq doesn't match this
	// timer's sequence, a newer timer superseded it — discard.
	if de.TimerSeq != timerSeq {
		return
	}
	if def, live := d.loadLiveTrigger(ctx, tm.TriggerID); live {
		err := publishSubjectFire(ctx, d.tp, def, de.LastEvent)
		if err != nil {
			// Keep the window: the trigger's next event reschedules it.
			slog.Error("debounce: start run failed",
				"trigger_id", def.ID, "error", err)
			return
		}
	}
	if err := d.stateKV.Delete(ctx, tm.DebounceKey); err != nil {
		slog.Warn("debounce: delete window failed",
			"key", tm.DebounceKey, "error", err)
	}
}

// loadLiveTrigger reads triggerID's current def. live is false when it
// was deleted, disabled, or is no longer a debounced subject trigger.
func (d *Debouncer) loadLiveTrigger(
	ctx context.Context, triggerID string,
) (def TriggerDef, live bool) {
	if ctx == nil {
		panic("loadLiveTrigger: ctx must not be nil")
	}
	if triggerID == "" {
		panic("loadLiveTrigger: triggerID must not be empty")
	}
	entry, err := d.triggerKV.Get(ctx, triggerID)
	if err != nil {
		if !errors.Is(err, jetstream.ErrKeyNotFound) {
			slog.Warn("debounce: load trigger failed",
				"trigger_id", triggerID, "error", err)
		}
		return TriggerDef{}, false
	}
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		slog.Warn("debounce: corrupt trigger def",
			"trigger_id", triggerID, "error", err)
		return TriggerDef{}, false
	}
	live = def.Enabled && def.Subject != nil && def.Debounce != nil &&
		def.ID == triggerID && def.WorkflowID != ""
	return def, live
}
