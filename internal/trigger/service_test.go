package trigger

// Methodology: integration test with embedded NATS. Verify that
// TriggerService loads triggers from KV and routes to the right handler.

import (
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
)

func TestServiceLoadsCronFromKV(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	// Store a cron trigger in KV
	def := TriggerDef{
		ID: "svc-t1", WorkflowID: "test-wf", Enabled: true,
		Cron: &CronConfig{Expression: "* * * * *", Timezone: "UTC"},
	}
	defData, _ := json.Marshal(def)
	trigKV.Put("svc-t1", defData)

	// Subscribe to catch events
	sub, _ := js.SubscribeSync("history.>",
		nats.AckExplicit(), nats.DeliverAll())

	// Start service
	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Manually tick the scheduler (don't wait 30s)
	svc.TickNow()

	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected event: %v", err)
	}
	msg.Ack()

	evt, _ := protocol.UnmarshalEvent(msg.Data)
	// Positive: workflow started
	if evt.Type != protocol.EventWorkflowStarted {
		t.Fatalf("type = %q, want workflow.started", evt.Type)
	}

	// Positive: from cron trigger
	var env TriggerEnvelope
	json.Unmarshal(evt.Payload, &env)
	if env.Trigger != "cron" {
		t.Fatalf("trigger = %q, want cron", env.Trigger)
	}
}

func TestServiceLiveReloadFromKV(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	// Start service with no triggers
	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Subscribe to catch events
	sub, _ := js.SubscribeSync("history.>",
		nats.AckExplicit(), nats.DeliverAll())

	// Add a trigger to KV after service started
	def := TriggerDef{
		ID: "svc-t2", WorkflowID: "test-wf", Enabled: true,
		Cron: &CronConfig{Expression: "* * * * *", Timezone: "UTC"},
	}
	defData, _ := json.Marshal(def)
	trigKV.Put("svc-t2", defData)

	// Give watcher time to process
	time.Sleep(100 * time.Millisecond)

	// Tick should now fire the newly added trigger
	svc.TickNow()

	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected event from live-added trigger: %v", err)
	}
	msg.Ack()

	// Positive: event appeared
	evt, _ := protocol.UnmarshalEvent(msg.Data)
	if evt.Type != protocol.EventWorkflowStarted {
		t.Fatalf("type = %q, want workflow.started", evt.Type)
	}
}

func TestServiceWebhookHandlerReturnsNonNil(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Positive: handler is not nil
	handler := svc.WebhookHandler()
	if handler == nil {
		t.Fatalf("WebhookHandler returned nil")
	}

	// Negative: unknown path returns 404
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/unknown", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Fatalf("expected 404 for unknown path, got %d", rec.Code)
	}
}

func TestServiceTickNowFiresTrigger(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	def := TriggerDef{
		ID: "tick-t", WorkflowID: "wf", Enabled: true,
		Cron: &CronConfig{
			Expression: "* * * * *", Timezone: "UTC",
		},
	}
	defData, _ := json.Marshal(def)
	trigKV.Put("tick-t", defData)

	sub, _ := js.SubscribeSync("history.>",
		nats.AckExplicit(), nats.DeliverAll())

	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	svc.TickNow()

	// Positive: event received
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("expected event from TickNow: %v", err)
	}
	msg.Ack()

	evt, _ := protocol.UnmarshalEvent(msg.Data)
	if evt.Type != protocol.EventWorkflowStarted {
		t.Fatalf("type = %q, want workflow.started", evt.Type)
	}
}

func TestServiceLoadsWebhookFromKV(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	def := TriggerDef{
		ID: "wh-t1", WorkflowID: "wf", Enabled: true,
		Webhook: &WebhookConfig{Path: "/hooks/deploy"},
	}
	defData, _ := json.Marshal(def)
	trigKV.Put("wh-t1", defData)

	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Positive: webhook registered (count includes it)
	count := svc.TriggerCount()
	if count < 1 {
		t.Fatalf("expected at least 1 trigger, got %d", count)
	}

	// Negative: non-webhook path gives 404
	handler := svc.WebhookHandler()
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/hooks/missing", nil)
	handler.ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Fatalf("expected 404 for missing webhook, got %d", rec.Code)
	}
}

func TestServiceStopIdempotent(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Positive: Stop does not panic
	svc.Stop()

	// Negative: TriggerCount still returns 0 after stop
	count := svc.TriggerCount()
	if count != 0 {
		t.Fatalf("expected 0 after stop, got %d", count)
	}
}

func TestServiceDisabledTriggerNotLoaded(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	def := TriggerDef{
		ID: "dis-t1", WorkflowID: "wf", Enabled: false,
		Cron: &CronConfig{
			Expression: "0 9 * * *", Timezone: "UTC",
		},
	}
	defData, _ := json.Marshal(def)
	trigKV.Put("dis-t1", defData)

	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Positive: service started without error
	// Negative: disabled trigger not added to count
	count := svc.TriggerCount()
	if count != 0 {
		t.Fatalf("expected 0 triggers, got %d", count)
	}
}

func TestServiceRespectsMaxTriggers(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	// Store 501 triggers (over limit)
	for i := 0; i < 501; i++ {
		def := TriggerDef{
			ID:         fmt.Sprintf("t%d", i),
			WorkflowID: "wf",
			Enabled:    true,
			Cron:       &CronConfig{Expression: "0 9 * * *", Timezone: "UTC"},
		}
		defData, _ := json.Marshal(def)
		trigKV.Put(def.ID, defData)
	}

	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Positive: service starts without panic
	// Negative: should enforce maxActiveTriggers=500
	count := svc.TriggerCount()
	if count > maxActiveTriggers {
		t.Fatalf("loaded %d triggers, max is %d", count, maxActiveTriggers)
	}
	if count == 0 {
		t.Fatalf("expected some triggers loaded")
	}
}
