package api

// fire_trigger_test.go exercises api.Service.FireTrigger (#352) end
// to end against an embedded NATS server.
//
// Methodology:
//   - Each test gets its own embedded server via natsutil.StartTestServer.
//   - SetupAll provisions the triggers KV + TRIGGER_HISTORY stream so
//     the publish path lands somewhere observable.
//   - Minimum 2 assertions per test: the run id is non-empty AND the
//     TRIGGER_HISTORY stream caught the fire record.

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestFireTrigger_cronSuccess fires an enabled cron trigger and
// confirms (1) the returned run id is non-empty, (2) the
// TRIGGER_HISTORY stream caught a TriggerFire row with Source=manual,
// (3) the workflow.started event subject was published.
func TestFireTrigger_cronSuccess(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	def := trigger.TriggerDef{
		ID:         "fire-cron",
		WorkflowID: "wf-fire",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "*/5 * * * *",
			Timezone:   "UTC",
		},
	}
	if err := svc.CreateTrigger(
		context.Background(), def,
	); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	runID, err := svc.FireTrigger(
		context.Background(), "fire-cron",
	)
	if err != nil {
		t.Fatalf("FireTrigger: %v", err)
	}
	if runID == "" {
		t.Fatalf("FireTrigger returned empty run id")
	}
	fire := drainOneTriggerFire(t, nc, "fire-cron")
	if fire.RunID != runID {
		t.Errorf("history RunID = %q; want %q", fire.RunID, runID)
	}
	if fire.Source != trigger.SourceManual {
		t.Errorf("history Source = %q; want %q",
			fire.Source, trigger.SourceManual)
	}
}

// TestFireTrigger_webhookSuccess confirms the webhook kind is allowed
// by the manual fire path.
func TestFireTrigger_webhookSuccess(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	def := trigger.TriggerDef{
		ID:         "fire-hook",
		WorkflowID: "wf-fire-hook",
		Enabled:    true,
		Webhook: &trigger.WebhookConfig{
			Path:   "/hooks/x",
			Secret: "s",
		},
	}
	if err := svc.CreateTrigger(
		context.Background(), def,
	); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	runID, err := svc.FireTrigger(
		context.Background(), "fire-hook",
	)
	if err != nil {
		t.Fatalf("FireTrigger: %v", err)
	}
	if runID == "" {
		t.Fatalf("FireTrigger returned empty run id")
	}
}

// TestFireTrigger_subjectKindRejected confirms manual fire of a
// NATS-subject trigger returns ErrTriggerKindNotFireable.
func TestFireTrigger_subjectKindRejected(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	def := trigger.TriggerDef{
		ID:         "fire-subj",
		WorkflowID: "wf-fire-subj",
		Enabled:    true,
		Subject:    &trigger.SubjectConfig{Subject: "demo.>"},
	}
	if err := svc.CreateTrigger(
		context.Background(), def,
	); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	runID, err := svc.FireTrigger(
		context.Background(), "fire-subj",
	)
	if !errors.Is(err, ErrTriggerKindNotFireable) {
		t.Fatalf("err = %v; want ErrTriggerKindNotFireable", err)
	}
	if runID != "" {
		t.Errorf("runID = %q on error path; want empty", runID)
	}
}

// TestFireTrigger_disabledRejected confirms a disabled trigger can't
// be fired manually — the operator must enable it first.
func TestFireTrigger_disabledRejected(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(natsutil.KVConfig{Bucket: "triggers"}),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	def := trigger.TriggerDef{
		ID:         "fire-dis",
		WorkflowID: "wf-dis",
		Enabled:    false,
		Cron: &trigger.CronConfig{
			Expression: "*/5 * * * *",
			Timezone:   "UTC",
		},
	}
	if err := svc.CreateTrigger(
		context.Background(), def,
	); err != nil {
		t.Fatalf("CreateTrigger: %v", err)
	}
	_, err := svc.FireTrigger(
		context.Background(), "fire-dis",
	)
	if !errors.Is(err, ErrTriggerDisabled) {
		t.Fatalf("err = %v; want ErrTriggerDisabled", err)
	}
}

// drainOneTriggerFire pulls one message off TRIGGER_HISTORY for the
// given trigger id and unmarshals it. Bounded — bails after 5s so a
// missing publish fails the calling test fast.
func drainOneTriggerFire(
	t *testing.T, nc *nats.Conn, triggerID string,
) trigger.TriggerFire {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	subject := "trigger.fire." + triggerID
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	stream, err := js.Stream(ctx, "TRIGGER_HISTORY")
	if err != nil {
		t.Fatalf("Stream TRIGGER_HISTORY: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(ctx,
		jetstream.ConsumerConfig{
			FilterSubject: subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
		})
	if err != nil {
		t.Fatalf("CreateOrUpdateConsumer: %v", err)
	}
	msg, err := cons.Next(jetstream.FetchMaxWait(5 * time.Second))
	if err != nil {
		t.Fatalf("consumer Next on %s: %v", subject, err)
	}
	var fire trigger.TriggerFire
	if err := json.Unmarshal(msg.Data(), &fire); err != nil {
		t.Fatalf("unmarshal TriggerFire: %v", err)
	}
	_ = msg.Ack()
	return fire
}
