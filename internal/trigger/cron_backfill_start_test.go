package trigger

// Methodology: integration tests against a real embedded NATS server for
// #784 item 3, cron.backfill having no effect. They drive the production
// entry points only: a cron fire must record <id>.last_run_at in
// trigger_state (the mark Backfill replays from), and
// TriggerService.Start must replay the minutes missed since that mark for
// backfill:true triggers and for no others.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestCronFireRecordsLastRun: a successful tick records the fired minute,
// and a trigger that did not fire records nothing.
func TestCronFireRecordsLastRun(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	scheduler, err := NewScheduler(nc)
	if err != nil {
		t.Fatalf("NewScheduler: %v", err)
	}
	every := TriggerDef{ID: "every", WorkflowID: "wf", Enabled: true,
		Cron: &CronConfig{Expression: "* * * * *", Timezone: "UTC"}}
	never := TriggerDef{ID: "never", WorkflowID: "wf", Enabled: true,
		Cron: &CronConfig{Expression: "0 0 29 2 *", Timezone: "UTC"}}
	for _, def := range []TriggerDef{every, never} {
		if err := scheduler.AddTrigger(def); err != nil {
			t.Fatalf("AddTrigger %s: %v", def.ID, err)
		}
	}
	now := time.Date(2026, 3, 2, 10, 7, 30, 0, time.UTC)
	if err := scheduler.Tick(now); err != nil {
		t.Fatalf("Tick: %v", err)
	}

	ctx := context.Background()
	got, err := scheduler.loadLastRun(ctx, "every")
	if err != nil {
		t.Fatalf("loadLastRun: %v", err)
	}
	if want := now.Truncate(time.Minute); !got.Equal(want) {
		t.Errorf("every.last_run_at = %v, want %v", got, want)
	}
	if _, err := scheduler.stateKV.Get(ctx, "never.last_run_at"); err == nil {
		t.Error("a trigger that did not fire must not record last_run_at")
	}
}

// TestServiceStartBackfillsMissedFires: with last_run_at three minutes
// back, Start replays the three missed minutes of a backfill:true trigger
// and none of an otherwise identical backfill:false one.
func TestServiceStartBackfillsMissedFires(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	lastRun := time.Now().UTC().Add(-3 * time.Minute).Truncate(time.Minute)
	for id, backfill := range map[string]bool{"bf-on": true, "bf-off": false} {
		seedCronTrigger(ctx, t, js, id, "wf-"+id, backfill, lastRun)
	}

	sub, err := nc.SubscribeSync("history.*")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	svc, err := NewTriggerService(nc, "1.0.0")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	fired := countStartedByWorkflow(t, sub, 3, 5*time.Second)
	if fired["wf-bf-on"] < 3 {
		t.Errorf("backfill:true fires = %d, want >= 3 (missed minutes)",
			fired["wf-bf-on"])
	}
	if fired["wf-bf-off"] > 1 {
		t.Errorf("backfill:false fires = %d, want at most the live minute",
			fired["wf-bf-off"])
	}
}

// seedCronTrigger stores an every-minute cron trigger and its last_run_at.
func seedCronTrigger(
	ctx context.Context, t *testing.T, js jetstream.JetStream,
	id, workflowID string, backfill bool, lastRun time.Time,
) {
	t.Helper()
	def := TriggerDef{ID: id, WorkflowID: workflowID, Enabled: true,
		Cron: &CronConfig{
			Expression: "* * * * *", Timezone: "UTC", Backfill: backfill,
		}}
	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal %s: %v", id, err)
	}
	triggers, err := js.KeyValue(ctx, "triggers")
	if err != nil {
		t.Fatalf("triggers KV: %v", err)
	}
	if _, err := triggers.Put(ctx, id, data); err != nil {
		t.Fatalf("put trigger %s: %v", id, err)
	}
	state, err := js.KeyValue(ctx, "trigger_state")
	if err != nil {
		t.Fatalf("trigger_state KV: %v", err)
	}
	stamp := []byte(lastRun.Format(time.RFC3339))
	if _, err := state.Put(ctx, id+".last_run_at", stamp); err != nil {
		t.Fatalf("put last_run_at %s: %v", id, err)
	}
}

// countStartedByWorkflow collects workflow.started events until want have
// arrived and then a quiet second passes, or until budget runs out, and
// counts them by the envelope's workflow ID.
func countStartedByWorkflow(
	t *testing.T, sub *nats.Subscription, want int, budget time.Duration,
) map[string]int {
	t.Helper()
	counts := map[string]int{}
	deadline := time.Now().Add(budget)
	const eventsMax = 1000
	for total := 0; total < eventsMax && time.Now().Before(deadline); total++ {
		wait := time.Until(deadline)
		if total >= want {
			wait = time.Second
		}
		msg, err := sub.NextMsg(wait)
		if err != nil {
			break
		}
		counts[startedWorkflowID(t, msg.Data)]++
	}
	return counts
}

// startedWorkflowID decodes a workflow.started event's trigger envelope;
// any other history event counts under "".
func startedWorkflowID(t *testing.T, data []byte) string {
	t.Helper()
	var evt protocol.Event
	if err := json.Unmarshal(data, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if evt.Type != protocol.EventWorkflowStarted {
		return ""
	}
	var env TriggerEnvelope
	if err := json.Unmarshal(evt.Payload, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	return env.WorkflowID
}
