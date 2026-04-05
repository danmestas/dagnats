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
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/engine"
	"github.com/nats-io/nats.go/jetstream"
)

// debounceEntry is the KV value for a pending debounce window.
type debounceEntry struct {
	LastEvent   json.RawMessage `json:"last_event"`
	FirstSeenNs int64           `json:"first_seen_ns"`
	TimerSeq    uint64          `json:"timer_seq"`
}

// Debouncer manages debounce state for triggers. Integrates with the
// SleepTimer for durable timers and a KV bucket for state persistence.
type Debouncer struct {
	js         jetstream.JetStream
	stateKV    jetstream.KeyValue
	sleepTimer *engine.SleepTimer
	onFire     FireHandler
}

// NewDebouncer creates a Debouncer. Panics on nil arguments.
func NewDebouncer(
	js jetstream.JetStream,
	sleepTimer *engine.SleepTimer,
) (*Debouncer, error) {
	if js == nil {
		panic("NewDebouncer: js must not be nil")
	}
	if sleepTimer == nil {
		panic("NewDebouncer: sleepTimer must not be nil")
	}
	stateKV, err := js.KeyValue(
		context.Background(), "debounce_state",
	)
	if err != nil {
		return nil, fmt.Errorf("debounce_state KV: %w", err)
	}
	return &Debouncer{
		js:         js,
		stateKV:    stateKV,
		sleepTimer: sleepTimer,
	}, nil
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
	def TriggerDef, eventData json.RawMessage,
) (fire bool, data json.RawMessage, err error) {
	if def.Debounce == nil {
		return true, eventData, nil
	}

	key := debounceKey(def, eventData)
	now := time.Now()

	// Try to load existing entry
	entry, err := d.stateKV.Get(context.Background(), key)
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
			d.stateKV.Delete(context.Background(), key)
			return true, eventData, nil
		}

		// Reset the window — update event data and schedule new timer
		return false, nil, d.updateDebounceEntry(
			key, eventData, existing.FirstSeenNs,
			def, entry.Revision(),
		)
	}

	// New window — create entry and schedule timer
	return false, nil, d.createDebounceEntry(
		key, eventData, now.UnixNano(), def,
	)
}

// createDebounceEntry creates a new debounce KV entry and schedules
// a timer. This is the ONLY code path that creates entries.
func (d *Debouncer) createDebounceEntry(
	key string,
	eventData json.RawMessage,
	firstSeenNs int64,
	def TriggerDef,
) error {
	if key == "" {
		panic("createDebounceEntry: key must not be empty")
	}

	// Schedule timer first to get the sequence
	seq, err := d.sleepTimer.ScheduleDebounce(engine.TimerMessage{
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
	_, err = d.stateKV.Create(context.Background(), key, data)
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
	key string,
	eventData json.RawMessage,
	firstSeenNs int64,
	def TriggerDef,
	revision uint64,
) error {
	if key == "" {
		panic("updateDebounceEntry: key must not be empty")
	}

	// Schedule new timer first to get the sequence
	seq, err := d.sleepTimer.ScheduleDebounce(engine.TimerMessage{
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
	_, err = d.stateKV.Update(
		context.Background(), key, data, revision,
	)
	if err != nil {
		return fmt.Errorf("update debounce_state: %w", err)
	}
	return nil
}

// HandleTimerFire is called by the SleepTimer when a debounce timer
// fires. Checks the KV entry's TimerSeq against the timer's stream
// sequence to detect stale timers. If fresh, publishes workflow.started
// and deletes the entry.
func (d *Debouncer) HandleTimerFire(
	tm engine.TimerMessage, timerSeq uint64,
) {
	if tm.DebounceKey == "" {
		return
	}
	entry, err := d.stateKV.Get(
		context.Background(), tm.DebounceKey,
	)
	if err != nil {
		return // entry gone — already fired or cleaned up
	}

	var de debounceEntry
	if err := json.Unmarshal(entry.Value(), &de); err != nil {
		return
	}

	// Stale timer check: if the entry's TimerSeq doesn't match this
	// timer's sequence, a newer timer superseded it — discard.
	if de.TimerSeq != timerSeq {
		return
	}

	// Fresh timer — fire the workflow
	d.fireWorkflow(tm.TriggerID, de.LastEvent)
	d.stateKV.Delete(context.Background(), tm.DebounceKey)
}

// fireWorkflow publishes workflow.started with the debounced event
// data as input.
func (d *Debouncer) fireWorkflow(
	triggerID string, eventData json.RawMessage,
) {
	if triggerID == "" {
		panic("fireWorkflow: triggerID must not be empty")
	}

	// Load trigger def to get workflow ID
	// The trigger KV is managed by TriggerService. We need a
	// reference to it. For now, the TriggerID is embedded in the
	// timer message and we need the workflow ID.
	// Use a callback pattern — set by TriggerService at init.
	if d.onFire != nil {
		d.onFire(triggerID, eventData)
	}
}

// FireHandler is called when a debounce window completes and the
// workflow should start. TriggerService sets this to wire up the
// actual workflow start logic.
type FireHandler func(triggerID string, eventData json.RawMessage)

// onFire is the callback for workflow firing.
var _ FireHandler // type check

// SetOnFire sets the callback invoked when a debounce timer fires
// and the workflow should start. Must be called before any timers fire.
func (d *Debouncer) SetOnFire(fn FireHandler) {
	if fn == nil {
		panic("SetOnFire: fn must not be nil")
	}
	d.onFire = fn
}
