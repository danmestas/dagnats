// trigger/scheduler_test.go
// Methodology: Integration tests with embedded NATS. Each test creates
// an isolated scheduler with real KV storage to validate dedup, timezone,
// and state tracking. Bounded timeouts prevent hanging tests.
package trigger

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestSchedulerTickFiresMatchingTriggers(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	scheduler, sub := setupSchedulerWithEveryMinuteTrigger(t, nc, js)
	testTime := time.Date(2026, 3, 31, 12, 30, 0, 0, time.UTC)

	// Tick at a matching minute
	err = scheduler.Tick(testTime)
	if err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	// Positive: should fire one workflow.started event
	verifyWorkflowStartedEvent(t, sub)

	// Negative: ticking again at same minute should not fire (dedup)
	err = scheduler.Tick(testTime)
	if err != nil {
		t.Fatalf("second Tick failed: %v", err)
	}

	// Should timeout (no duplicate)
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("expected timeout on duplicate tick, got message")
	}
}

// TestSchedulerTickDoesNotDoubleFireWithinSameMinute regresses #173.
// The scheduler ticks every 30s but cron is minute-resolution, so two
// ticks 30s apart in a matching minute used to fire twice. JetStream
// stream-level msgID dedup masks this only when publishes are <5s apart
// (the workflow stream's Duplicates window). To prove the bug at the
// scheduler level — independent of JetStream stream config — this test
// uses a CORE NATS subscriber on `history.>`, which sees every publish
// regardless of stream dedup.
func TestSchedulerTickDoesNotDoubleFireWithinSameMinute(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	def := TriggerDef{
		ID:         "every3-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "*/3 * * * *",
			Timezone:   "UTC",
			Backfill:   false,
		},
	}
	if err := scheduler.AddTrigger(def); err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	// Core NATS sub: bypasses stream-level dedup so we see real publish
	// behavior. Any double-publish from Tick is observable here.
	sub, err := nc.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	// Two ticks in the same matching minute (12:24:00 and 12:24:30).
	// `*/3 * * * *` matches minute 24, so Matches() returns true for both.
	tick1 := time.Date(2026, 3, 31, 12, 24, 0, 0, time.UTC)
	tick2 := tick1.Add(30 * time.Second)

	if err := scheduler.Tick(tick1); err != nil {
		t.Fatalf("first Tick failed: %v", err)
	}
	// Positive: first tick fires exactly one event.
	msg1, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("expected first event: %v", err)
	}
	if msg1 == nil {
		t.Fatalf("expected non-nil first message")
	}

	if err := scheduler.Tick(tick2); err != nil {
		t.Fatalf("second Tick failed: %v", err)
	}
	// Negative: second tick (same minute) must not produce a second publish.
	msg2, err := sub.NextMsg(1500 * time.Millisecond)
	if err == nil {
		t.Errorf(
			"expected no second event within same matching minute, "+
				"got %s", msg2.Subject)
	}
}

// TestSchedulerTickFiresInNextMatchingMinute confirms the in-process
// dedup is per-minute, not permanent: a later matching minute must
// produce a new run.
func TestSchedulerTickFiresInNextMatchingMinute(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	def := TriggerDef{
		ID:         "every3-next",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "*/3 * * * *",
			Timezone:   "UTC",
			Backfill:   false,
		},
	}
	if err := scheduler.AddTrigger(def); err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	sub, err := nc.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	tick1 := time.Date(2026, 3, 31, 12, 24, 0, 0, time.UTC)
	tick2 := time.Date(2026, 3, 31, 12, 27, 0, 0, time.UTC)

	if err := scheduler.Tick(tick1); err != nil {
		t.Fatalf("first Tick failed: %v", err)
	}
	msg1, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("expected first event: %v", err)
	}

	if err := scheduler.Tick(tick2); err != nil {
		t.Fatalf("second Tick failed: %v", err)
	}
	msg2, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("expected second event in next matching minute: %v", err)
	}

	// Positive: distinct subjects (runID embedded) prove separate fires.
	if msg1.Subject == msg2.Subject {
		t.Errorf("expected distinct subjects, got %q twice", msg1.Subject)
	}
}

// TestSchedulerBackfillThenLiveTickNoDoubleFire regresses the same
// failure class as #173 at the backfill→live-tick boundary. Backfill
// replays the most recent missed minute; if the next live Tick lands
// in that same minute (and the workflow stream's 5s msgID dedup window
// has elapsed because backfill ran ≥30s earlier), the minute would
// fire twice. The in-process claimMinute guard prevents this.
func TestSchedulerBackfillThenLiveTickNoDoubleFire(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	def := TriggerDef{
		ID:         "boundary-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
			Backfill:   true,
		},
	}
	if err := scheduler.AddTrigger(def); err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	// Seed last_run_at to 1 minute ago so Backfill replays the most
	// recent missed minute, which we will then re-tick live.
	lastRun := time.Now().UTC().Add(-1 * time.Minute).Truncate(time.Minute)
	_, err = scheduler.stateKV.Put(
		context.Background(),
		"boundary-trigger.last_run_at",
		[]byte(lastRun.Format(time.RFC3339)))
	if err != nil {
		t.Fatalf("KV Put failed: %v", err)
	}

	sub, err := nc.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}
	defer func() { _ = sub.Unsubscribe() }()

	if err := scheduler.Backfill(); err != nil {
		t.Fatalf("Backfill failed: %v", err)
	}
	// Drain the backfilled events, keeping the minute the last one
	// claimed. That minute comes from the published envelope, NOT from
	// a second time.Now() read: Backfill claims the minute containing
	// its own time.Now(), and this drain takes ~500ms, so a wall-clock
	// read here lands in the NEXT minute whenever the drain straddles a
	// real minute boundary (~1% of runs). A live tick in a genuinely
	// new minute SHOULD fire, so that made the assertion below flaky
	// rather than wrong. Sourcing the tick time from the event keeps
	// the test on the behavior it means to pin.
	backfilled := 0
	var backfilledMinute time.Time
	for {
		msg, err := sub.NextMsg(500 * time.Millisecond)
		if err != nil {
			break
		}
		backfilled++
		backfilledMinute = triggerFireMinute(t, msg)
	}
	if backfilled == 0 {
		t.Fatalf("expected at least one backfilled event")
	}
	if backfilledMinute.IsZero() {
		t.Fatalf("expected non-zero minute on backfilled event")
	}

	// Live tick at exactly the most recent backfilled minute. Without
	// the claimMinute guard in backfill, this would publish again.
	if err := scheduler.Tick(backfilledMinute); err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	// Negative: no live event for an already-claimed minute.
	msg, err := sub.NextMsg(1500 * time.Millisecond)
	if err == nil {
		t.Errorf(
			"expected no live event for backfilled minute, got %s",
			msg.Subject)
	}
}

// triggerFireMinute reports the cron minute a workflow.started event was
// fired for, read from its TriggerEnvelope payload. Tests derive tick
// times from this rather than from time.Now() so an assertion about a
// specific minute cannot be decided by where a real minute boundary
// happens to fall during the test run.
func triggerFireMinute(t *testing.T, msg *nats.Msg) time.Time {
	t.Helper()

	var evt protocol.Event
	if err := json.Unmarshal(msg.Data, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if evt.Type != protocol.EventWorkflowStarted {
		t.Fatalf("expected workflow.started, got %s", evt.Type)
	}

	var envelope TriggerEnvelope
	if err := json.Unmarshal(evt.Payload, &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if envelope.Timestamp.IsZero() {
		t.Fatalf("expected non-zero envelope timestamp")
	}

	return envelope.Timestamp.UTC()
}

func setupSchedulerWithEveryMinuteTrigger(
	t *testing.T, nc *nats.Conn, js nats.JetStreamContext,
) (*Scheduler, *nats.Subscription) {
	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	triggerDef := TriggerDef{
		ID:         "test-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
			Backfill:   false,
		},
	}
	err = scheduler.AddTrigger(triggerDef)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	return scheduler, sub
}

func verifyWorkflowStartedEvent(
	t *testing.T, sub *nats.Subscription,
) {
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("expected workflow.started event, got timeout")
	}

	var evt protocol.Event
	err = json.Unmarshal(msg.Data, &evt)
	if err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if evt.Type != protocol.EventWorkflowStarted {
		t.Errorf("expected workflow.started, got %s", evt.Type)
	}

	msgID := msg.Header.Get("Nats-Msg-Id")
	if msgID == "" {
		t.Errorf("expected Nats-Msg-Id header, got empty")
	}
}

func TestSchedulerDeduplicationAcrossMinutes(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	triggerDef := TriggerDef{
		ID:         "test-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "*/5 * * * *",
			Timezone:   "UTC",
		},
	}
	err = scheduler.AddTrigger(triggerDef)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	// Tick at 12:30 (matches */5)
	time1 := time.Date(2026, 3, 31, 12, 30, 0, 0, time.UTC)
	err = scheduler.Tick(time1)
	if err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	msg1, err := sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("expected first event")
	}
	msgID1 := msg1.Header.Get("Nats-Msg-Id")

	// Tick at 12:35 (next match)
	time2 := time.Date(2026, 3, 31, 12, 35, 0, 0, time.UTC)
	err = scheduler.Tick(time2)
	if err != nil {
		t.Fatalf("second Tick failed: %v", err)
	}

	msg2, err := sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("expected second event")
	}
	msgID2 := msg2.Header.Get("Nats-Msg-Id")

	// Different msg IDs
	if msgID1 == msgID2 {
		t.Errorf("expected different dedup IDs, got same: %s", msgID1)
	}
}

func TestSchedulerTimezoneSupport(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	// Trigger at 9 AM America/New_York
	triggerDef := TriggerDef{
		ID:         "tz-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "0 9 * * *",
			Timezone:   "America/New_York",
		},
	}
	err = scheduler.AddTrigger(triggerDef)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	// 9 AM NYC is 14:00 UTC (during DST)
	// Using March 31, 2026 which is after DST starts
	loc, _ := time.LoadLocation("America/New_York")
	nycTime := time.Date(2026, 3, 31, 9, 0, 0, 0, loc)
	utcTime := nycTime.UTC()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	err = scheduler.Tick(utcTime)
	if err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("expected event at NYC time")
	}

	// Should NOT fire at 9 AM UTC
	utcNine := time.Date(2026, 3, 31, 9, 0, 0, 0, time.UTC)
	err = scheduler.Tick(utcNine)
	if err != nil {
		t.Fatalf("Tick at UTC 9 failed: %v", err)
	}

	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("should not fire at UTC 9 AM")
	}
}

func TestSchedulerDisabledTrigger(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	triggerDef := TriggerDef{
		ID:         "disabled-trigger",
		WorkflowID: "test-workflow",
		Enabled:    false,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
		},
	}
	err = scheduler.AddTrigger(triggerDef)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	testTime := time.Date(2026, 3, 31, 12, 30, 0, 0, time.UTC)
	err = scheduler.Tick(testTime)
	if err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	// Should not fire
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("disabled trigger should not fire")
	}
}

func TestSchedulerRemoveTrigger(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	triggerDef := TriggerDef{
		ID:         "remove-me",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
		},
	}
	err = scheduler.AddTrigger(triggerDef)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	err = scheduler.RemoveTrigger("remove-me")
	if err != nil {
		t.Fatalf("RemoveTrigger failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}
	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	testTime := time.Date(2026, 3, 31, 12, 30, 0, 0, time.UTC)
	err = scheduler.Tick(testTime)
	if err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	// Should not fire
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("removed trigger should not fire")
	}
}

func TestSchedulerStartAutoTick(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	// Add trigger that fires every minute
	triggerDef := TriggerDef{
		ID:         "auto-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
		},
	}
	err = scheduler.AddTrigger(triggerDef)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	// Start the scheduler with 100ms tick interval (for testing)
	ctx, cancel := context.WithCancel(context.Background())
	doneChan := make(chan struct{})
	go func() {
		scheduler.Start(ctx, 100*time.Millisecond)
		close(doneChan)
	}()

	// Wait for at least one tick cycle
	time.Sleep(300 * time.Millisecond)

	// Stop scheduler
	cancel()

	// Wait for shutdown
	select {
	case <-doneChan:
		// Success
	case <-time.After(2 * time.Second):
		t.Errorf("scheduler did not stop within timeout")
	}
}

func TestSchedulerBackfillMissedRuns(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	triggerDef := TriggerDef{
		ID:         "backfill-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
			Backfill:   true,
		},
	}
	err = scheduler.AddTrigger(triggerDef)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	// Set last_run_at to 3 minutes ago in trigger_state KV
	lastRun := time.Now().UTC().Add(-3 * time.Minute).Truncate(time.Minute)
	lastRunBytes := []byte(lastRun.Format(time.RFC3339))
	_, err = scheduler.stateKV.Put(context.Background(), "backfill-trigger.last_run_at", lastRunBytes)
	if err != nil {
		t.Fatalf("KV Put failed: %v", err)
	}

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	err = scheduler.Backfill()
	if err != nil {
		t.Fatalf("Backfill failed: %v", err)
	}

	// Positive: should fire 3 workflow.started events (one per missed minute)
	eventCount := 0
	for i := 0; i < 3; i++ {
		_, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("expected event %d, got timeout", i+1)
		}
		eventCount++
	}
	if eventCount != 3 {
		t.Errorf("expected 3 backfilled events, got %d", eventCount)
	}

	// Negative: no additional events
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("expected no more events after backfill")
	}
}

func TestSchedulerBackfillCapsAt100(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	triggerDef := TriggerDef{
		ID:         "cap-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
			Backfill:   true,
		},
	}
	err = scheduler.AddTrigger(triggerDef)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	// Set last_run_at to 200 minutes ago
	lastRun := time.Now().UTC().Add(-200 * time.Minute).Truncate(time.Minute)
	lastRunBytes := []byte(lastRun.Format(time.RFC3339))
	_, err = scheduler.stateKV.Put(context.Background(), "cap-trigger.last_run_at", lastRunBytes)
	if err != nil {
		t.Fatalf("KV Put failed: %v", err)
	}

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	err = scheduler.Backfill()
	if err != nil {
		t.Fatalf("Backfill failed: %v", err)
	}

	// Positive: should fire exactly 100 events (not 200)
	eventCount := 0
	for i := 0; i < 100; i++ {
		_, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("expected event %d, got timeout", i+1)
		}
		eventCount++
	}
	if eventCount != 100 {
		t.Errorf("expected 100 backfilled events, got %d", eventCount)
	}

	// Negative: no 101st event
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("expected exactly 100 events, got more")
	}
}

func TestSchedulerShouldFireMatchesTimezone(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	def := TriggerDef{
		ID:         "sf-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "30 10 * * *",
			Timezone:   "America/Denver",
		},
	}
	err = scheduler.AddTrigger(def)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	// 10:30 Denver (MDT = UTC-6) = 16:30 UTC
	loc, _ := time.LoadLocation("America/Denver")
	denverTime := time.Date(2026, 6, 15, 10, 30, 0, 0, loc)
	utcTime := denverTime.UTC()

	// Positive: fires at correct UTC equivalent
	err = scheduler.Tick(utcTime)
	if err != nil {
		t.Fatalf("Tick failed: %v", err)
	}

	// Negative: does not fire at wrong hour
	wrongHour := time.Date(2026, 6, 15, 10, 30, 0, 0, time.UTC)
	js, _ := nc.JetStream()
	sub, _ := js.SubscribeSync("history.>")
	// Drain the first event
	sub.NextMsg(1 * time.Second)

	err = scheduler.Tick(wrongHour)
	if err != nil {
		t.Fatalf("Tick at wrong hour failed: %v", err)
	}

	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("should not fire at 10:30 UTC for Denver trigger")
	}
}

func TestSchedulerStartStopsOnSignal(t *testing.T) {
	// Methodology: start the scheduler in a goroutine, let a few ticks
	// fire, cancel the context, verify Start returns within a bounded
	// window. The bounded `<-time.After` arm is the negative-space
	// guard — without it, a Start that loops forever would hang the
	// test rather than fail. (An earlier "is doneChan closed?" double-
	// select was structurally vacuous: the only path past the first
	// select required doneChan to be closed, so the second's default
	// arm was unreachable. Verifying "no ticks after Start returns"
	// would require a tick-counter test seam in the production
	// Scheduler — that's scope creep for this test's purpose.)
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	doneChan := make(chan struct{})
	go func() {
		scheduler.Start(ctx, 50*time.Millisecond)
		close(doneChan)
	}()

	// Let a few ticks happen.
	time.Sleep(200 * time.Millisecond)
	cancel()

	// Start must return within a bounded window after cancel. The
	// time.After arm is the negative-space guard against an infinite
	// loop in Start — without it the test would hang.
	select {
	case <-doneChan:
	case <-time.After(2 * time.Second):
		t.Fatalf("Start did not return after stop signal")
	}
}

func TestSchedulerAddTriggerRejectsNilCron(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	def := TriggerDef{
		ID:         "no-cron",
		WorkflowID: "wf",
		Enabled:    true,
		// No Cron config
	}

	// Positive: returns error for nil cron
	err = scheduler.AddTrigger(def)
	if err == nil {
		t.Fatalf("expected error for nil cron")
	}

	// Negative: trigger not added
	scheduler.mu.RLock()
	_, exists := scheduler.triggers["no-cron"]
	scheduler.mu.RUnlock()
	if exists {
		t.Fatalf("trigger should not be registered")
	}
}

func TestSchedulerTickReturnsErrorForBadTimezone(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	def := TriggerDef{
		ID:         "bad-tz",
		WorkflowID: "wf",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "Invalid/Timezone",
		},
	}
	err = scheduler.AddTrigger(def)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	// Positive: Tick returns error for bad timezone
	testTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	err = scheduler.Tick(testTime)
	if err == nil {
		t.Fatalf("expected error for invalid timezone")
	}

	// Negative: error message mentions the trigger ID
	if !containsStr(err.Error(), "bad-tz") {
		t.Fatalf("error = %q, should mention trigger ID", err)
	}
}

func TestSchedulerTickReturnsErrorForBadCronExpr(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	def := TriggerDef{
		ID:         "bad-expr",
		WorkflowID: "wf",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "invalid",
			Timezone:   "UTC",
		},
	}
	err = scheduler.AddTrigger(def)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	// Positive: Tick returns error for invalid expression
	testTime := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	err = scheduler.Tick(testTime)
	if err == nil {
		t.Fatalf("expected error for invalid cron expression")
	}

	// Negative: error message mentions the trigger ID
	if !containsStr(err.Error(), "bad-expr") {
		t.Fatalf("error = %q, should mention trigger ID", err)
	}
}

// containsStr checks if substr is in s (avoids importing strings).
func containsStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func TestSchedulerBackfillNoLastRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, _ := nc.JetStream()

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	def := TriggerDef{
		ID:         "no-last-run",
		WorkflowID: "wf",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
			Backfill:   true,
		},
	}
	err = scheduler.AddTrigger(def)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	sub, _ := js.SubscribeSync("history.>")

	// Positive: backfill succeeds with no error
	err = scheduler.Backfill()
	if err != nil {
		t.Fatalf("Backfill failed: %v", err)
	}

	// Negative: no events fired (zero time means skip)
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("no events expected when no last_run_at exists")
	}
}

func TestSchedulerBackfillCorruptedLastRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	def := TriggerDef{
		ID:         "corrupt-trigger",
		WorkflowID: "wf",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
			Backfill:   true,
		},
	}
	err = scheduler.AddTrigger(def)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	// Store corrupted timestamp
	_, err = scheduler.stateKV.Put(
		context.Background(),
		"corrupt-trigger.last_run_at", []byte("not-a-time"))
	if err != nil {
		t.Fatalf("KV Put failed: %v", err)
	}

	// Positive: Backfill returns error for corrupted time
	err = scheduler.Backfill()
	if err == nil {
		t.Fatalf("expected error for corrupted last_run_at")
	}

	// Negative: error mentions the trigger
	if !containsStr(err.Error(), "corrupt-trigger") {
		t.Fatalf("error = %q, should mention trigger", err)
	}
}

func TestSchedulerBackfillBadTimezone(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	def := TriggerDef{
		ID:         "bad-tz-backfill",
		WorkflowID: "wf",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "Fake/Zone",
			Backfill:   true,
		},
	}
	err = scheduler.AddTrigger(def)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	lastRun := time.Now().UTC().Add(-2 * time.Minute).Truncate(time.Minute)
	_, err = scheduler.stateKV.Put(
		context.Background(),
		"bad-tz-backfill.last_run_at",
		[]byte(lastRun.Format(time.RFC3339)))
	if err != nil {
		t.Fatalf("KV Put failed: %v", err)
	}

	// Positive: returns error for invalid timezone
	err = scheduler.Backfill()
	if err == nil {
		t.Fatalf("expected error for invalid timezone in backfill")
	}

	// Negative: error mentions the trigger
	if !containsStr(err.Error(), "bad-tz-backfill") {
		t.Fatalf("error = %q", err)
	}
}

func TestSchedulerBackfillSkipsNoBackfillTriggers(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "trigger_state"}))
	if err != nil {
		t.Fatalf("setup failed: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream failed: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler failed: %v", err)
	}

	triggerDef := TriggerDef{
		ID:         "no-backfill-trigger",
		WorkflowID: "test-workflow",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
			Backfill:   false,
		},
	}
	err = scheduler.AddTrigger(triggerDef)
	if err != nil {
		t.Fatalf("AddTrigger failed: %v", err)
	}

	// Set old last_run_at
	lastRun := time.Now().UTC().Add(-5 * time.Minute).Truncate(time.Minute)
	lastRunBytes := []byte(lastRun.Format(time.RFC3339))
	_, err = scheduler.stateKV.Put(context.Background(), "no-backfill-trigger.last_run_at", lastRunBytes)
	if err != nil {
		t.Fatalf("KV Put failed: %v", err)
	}

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("Subscribe failed: %v", err)
	}

	err = scheduler.Backfill()
	if err != nil {
		t.Fatalf("Backfill failed: %v", err)
	}

	// Positive: no events (backfill disabled)
	_, err = sub.NextMsg(500 * time.Millisecond)
	if err == nil {
		t.Errorf("expected no backfill events when Backfill=false")
	}

	// Negative: verify no state changes
	entry, err := scheduler.stateKV.Get(context.Background(), "no-backfill-trigger.last_run_at")
	if err != nil {
		t.Fatalf("KV Get failed: %v", err)
	}
	if string(entry.Value()) != string(lastRunBytes) {
		t.Errorf("expected last_run_at unchanged when Backfill=false")
	}
}
