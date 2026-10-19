package trigger

// Methodology: integration test against a real embedded NATS server for
// #784 item 4, subject-trigger debounce having no effect. The trigger is
// registered through the triggers KV and events arrive on its subject, as
// in production; the debounce timer is consumed by a real SLEEP_TIMERS
// consumer with the debounce handler wired the way the orchestrator wires
// it. Positive space: one run, carrying the last event. Negative space: no
// run while events keep arriving.

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestSubjectTriggerDebounceCollapsesBurst: three events inside the
// debounce period start no run until the period passes quietly, then
// start exactly one, with the last event's data.
func TestSubjectTriggerDebounceCollapsesBurst(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	startDebounceTimers(t, nc)
	svc, err := NewTriggerService(nc, "1.0.0")
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()
	putDebouncedSubjectTrigger(t, nc, time.Second)
	waitTriggerCount(t, svc, 1)

	sub, err := nc.SubscribeSync("history.*")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	for i := 1; i <= 3; i++ {
		data := []byte(`{"n":` + string(rune('0'+i)) + `}`)
		if err := nc.Publish("orders.created", data); err != nil {
			t.Fatalf("publish %d: %v", i, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	if msg, err := sub.NextMsg(500 * time.Millisecond); err == nil {
		t.Fatalf("run started inside the debounce period: %s", msg.Data)
	}

	msg, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("no run after the debounce period: %v", err)
	}
	if got := debouncedEventData(t, msg.Data); got != `{"n":3}` {
		t.Errorf("run input = %s, want the last event {\"n\":3}", got)
	}
	if extra, err := sub.NextMsg(1500 * time.Millisecond); err == nil {
		t.Fatalf("burst started a second run: %s", extra.Data)
	}
}

// startDebounceTimers runs a SLEEP_TIMERS consumer with the debounce
// handler wired, standing in for the orchestrator's.
func startDebounceTimers(t *testing.T, nc *nats.Conn) {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	tp := natsutil.NewTracingPublisher(nc, js)
	debouncer, err := NewDebouncer(js, tp)
	if err != nil {
		t.Fatalf("NewDebouncer: %v", err)
	}
	timers := engine.NewSleepTimer(nc, js, tp)
	timers.OnDebounce(debouncer.HandleTimerFire)
	if err := timers.Start(); err != nil {
		t.Fatalf("SleepTimer.Start: %v", err)
	}
	t.Cleanup(timers.Stop)
}

// putDebouncedSubjectTrigger registers a subject trigger on
// orders.created debounced by period.
func putDebouncedSubjectTrigger(
	t *testing.T, nc *nats.Conn, period time.Duration,
) {
	t.Helper()
	def := TriggerDef{
		ID: "orders", WorkflowID: "wf-orders", Enabled: true,
		Subject:  &SubjectConfig{Subject: "orders.created"},
		Debounce: &DebounceConfig{Period: period},
	}
	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	kv, err := js.KeyValue(ctx, "triggers")
	if err != nil {
		t.Fatalf("triggers KV: %v", err)
	}
	if _, err := kv.Put(ctx, def.ID, data); err != nil {
		t.Fatalf("put trigger: %v", err)
	}
}

// waitTriggerCount waits (bounded) for the service to load want triggers.
func waitTriggerCount(t *testing.T, svc *TriggerService, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for svc.TriggerCount() < want && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := svc.TriggerCount(); got != want {
		t.Fatalf("trigger count = %d, want %d", got, want)
	}
}

// debouncedEventData returns the trigger envelope data of a
// workflow.started history event.
func debouncedEventData(t *testing.T, raw []byte) string {
	t.Helper()
	var evt protocol.Event
	if err := json.Unmarshal(raw, &evt); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if evt.Type != protocol.EventWorkflowStarted {
		t.Fatalf("event type = %s, want workflow.started", evt.Type)
	}
	var env TriggerEnvelope
	if err := json.Unmarshal(evt.Payload, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.WorkflowID != "wf-orders" {
		t.Fatalf("workflow = %q, want wf-orders", env.WorkflowID)
	}
	return string(env.Data)
}
