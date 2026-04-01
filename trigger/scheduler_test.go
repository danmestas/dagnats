// trigger/scheduler_test.go
// Methodology: Integration tests with embedded NATS. Each test creates
// an isolated scheduler with real KV storage to validate dedup, timezone,
// and state tracking. Bounded timeouts prevent hanging tests.
package trigger

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
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
	stopChan := make(chan struct{})
	doneChan := make(chan struct{})
	go func() {
		scheduler.Start(100*time.Millisecond, stopChan)
		close(doneChan)
	}()

	// Wait for at least one tick cycle
	time.Sleep(300 * time.Millisecond)

	// Stop scheduler
	close(stopChan)

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
	_, err = scheduler.stateKV.Put("backfill-trigger.last_run_at", lastRunBytes)
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
	_, err = scheduler.stateKV.Put("cap-trigger.last_run_at", lastRunBytes)
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
	_, err = scheduler.stateKV.Put("no-backfill-trigger.last_run_at", lastRunBytes)
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
	entry, err := scheduler.stateKV.Get("no-backfill-trigger.last_run_at")
	if err != nil {
		t.Fatalf("KV Get failed: %v", err)
	}
	if string(entry.Value()) != string(lastRunBytes) {
		t.Errorf("expected last_run_at unchanged when Backfill=false")
	}
}
