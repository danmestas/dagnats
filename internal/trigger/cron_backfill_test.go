package trigger

// Methodology: integration test with embedded NATS that exercises the
// register-time path for cron triggers. Verifies that registering a
// trigger with Backfill=false does NOT fire any run on registration —
// the next fire must come from the steady-state Tick path. Negative
// space: registering with Backfill=true should still backfill missed
// fires when last_run_at is set, proving the guard does not over-correct.
//
// Issue: #139 — cron triggers fire immediately on workflow registration
// even when backfill:false.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
)

// TestRegisterDoesNotFireWhenBackfillFalse registers a Sunday-noon cron
// on a Friday and asserts no workflow.started event appears within 2s.
func TestRegisterDoesNotFireWhenBackfillFalse(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	trigKV, err := js.KeyValue("triggers")
	if err != nil {
		t.Fatalf("KeyValue triggers: %v", err)
	}

	sub, err := js.SubscribeSync("history.>",
		nats.AckExplicit(), nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Register a Sunday-noon cron with backfill=false. Registration
	// happens "on a Friday" — i.e., a moment that does not match the
	// cron — and the service should NOT fire on register.
	def := TriggerDef{
		ID:         "sunday-noon",
		WorkflowID: "weekly-job",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "0 12 * * 0",
			Timezone:   "UTC",
			Backfill:   false,
		},
	}
	defData, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if _, err := trigKV.Put("sunday-noon", defData); err != nil {
		t.Fatalf("KV Put: %v", err)
	}

	// Positive: trigger landed in the service.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if svc.TriggerCount() >= 1 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if svc.TriggerCount() < 1 {
		t.Fatalf(
			"trigger not loaded into service (count=%d)",
			svc.TriggerCount(),
		)
	}

	// Negative: no workflow.started event within 2s of registration.
	msg, err := sub.NextMsg(2 * time.Second)
	if err == nil {
		t.Fatalf(
			"unexpected fire on register: subject=%s data=%q",
			msg.Subject, string(msg.Data),
		)
	}
}

// TestBackfillTriggerSkipsWhenBackfillFalse exercises the
// defense-in-depth guard in backfillTrigger directly. Even with a
// non-empty last_run_at seeded in trigger_state KV, a trigger with
// Backfill=false must not replay any missed fires.
func TestBackfillTriggerSkipsWhenBackfillFalse(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	def := TriggerDef{
		ID:         "no-bf",
		WorkflowID: "wf",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
			Backfill:   false,
		},
	}
	if err := scheduler.AddTrigger(def); err != nil {
		t.Fatalf("AddTrigger: %v", err)
	}

	// Seed last_run_at — would yield missed fires if backfill were on.
	lastRun := time.Now().UTC().
		Add(-3 * time.Minute).Truncate(time.Minute)
	_, err = scheduler.stateKV.Put(
		context.Background(),
		"no-bf.last_run_at",
		[]byte(lastRun.Format(time.RFC3339)),
	)
	if err != nil {
		t.Fatalf("KV Put: %v", err)
	}

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	// Direct call to backfillTrigger must early-exit on !Backfill.
	if err := scheduler.backfillTrigger(def); err != nil {
		t.Fatalf("backfillTrigger: %v", err)
	}

	// Negative: no events from the direct call.
	if msg, err := sub.NextMsg(500 * time.Millisecond); err == nil {
		t.Fatalf(
			"unexpected fire from backfillTrigger with "+
				"Backfill=false: subject=%s",
			msg.Subject,
		)
	}
}

// TestRegisterWithBackfillTrueStillBackfills proves the !Backfill guard
// does not over-correct: with Backfill=true and a non-empty last_run_at,
// missed fires must still replay.
func TestRegisterWithBackfillTrueStillBackfills(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}

	def := TriggerDef{
		ID:         "every-minute-bf",
		WorkflowID: "wf",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
			Backfill:   true,
		},
	}
	if err := scheduler.AddTrigger(def); err != nil {
		t.Fatalf("AddTrigger: %v", err)
	}

	// Seed last_run_at so backfill has something to replay.
	lastRun := time.Now().UTC().
		Add(-2 * time.Minute).Truncate(time.Minute)
	lastRunBytes := []byte(lastRun.Format(time.RFC3339))
	_, err = scheduler.stateKV.Put(
		context.Background(),
		"every-minute-bf.last_run_at",
		lastRunBytes,
	)
	if err != nil {
		t.Fatalf("KV Put last_run_at: %v", err)
	}

	sub, err := js.SubscribeSync("history.>")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}

	if err := scheduler.Backfill(); err != nil {
		t.Fatalf("Backfill: %v", err)
	}

	// Positive: at least one missed-fire replays.
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf(
			"expected backfilled event, got timeout: %v", err,
		)
	}
}
