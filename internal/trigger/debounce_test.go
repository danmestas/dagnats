// trigger/debounce_test.go
// Tests for debounce logic. Methodology: real NATS for KV and
// SLEEP_TIMERS. Verify debounce absorbs rapid events, fires on
// quiet period, and respects hard timeout.
package trigger

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/engine"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestDebounceAbsorbsEvents(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := jetstream.New(nc)

	d, err := NewDebouncer(js, natsutil.NewTracingPublisher(nc, js))
	if err != nil {
		t.Fatalf("NewDebouncer: %v", err)
	}

	def := TriggerDef{
		ID: "t1", WorkflowID: "wf",
		Subject:  &SubjectConfig{Subject: "test.>"},
		Debounce: &DebounceConfig{Period: 5 * time.Second},
	}

	// Positive: first event is absorbed (not fired)
	fire, _, err := d.DebounceOrFire(
		context.Background(), def, json.RawMessage(`{"v":1}`),
	)
	if err != nil {
		t.Fatalf("event 1: %v", err)
	}
	if fire {
		t.Fatal("first event should be absorbed, not fired")
	}

	// Positive: second event is also absorbed (resets window)
	fire, _, err = d.DebounceOrFire(
		context.Background(), def, json.RawMessage(`{"v":2}`),
	)
	if err != nil {
		t.Fatalf("event 2: %v", err)
	}
	if fire {
		t.Fatal("second event should be absorbed")
	}

	// Negative: entry stores latest event data
	entry, err := d.stateKV.Get(context.Background(), "t1")
	if err != nil {
		t.Fatalf("get state: %v", err)
	}
	var de debounceEntry
	json.Unmarshal(entry.Value(), &de)
	if string(de.LastEvent) != `{"v":2}` {
		t.Fatalf("LastEvent = %s, want {\"v\":2}",
			string(de.LastEvent))
	}
}

func TestDebounceFiresOnHardTimeout(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := jetstream.New(nc)

	d, err := NewDebouncer(js, natsutil.NewTracingPublisher(nc, js))
	if err != nil {
		t.Fatalf("NewDebouncer: %v", err)
	}

	def := TriggerDef{
		ID: "t2", WorkflowID: "wf",
		Subject: &SubjectConfig{Subject: "test.>"},
		Debounce: &DebounceConfig{
			Period:  5 * time.Second,
			Timeout: 100 * time.Millisecond,
		},
	}

	// First event — creates window
	fire, _, err := d.DebounceOrFire(
		context.Background(), def, json.RawMessage(`{"v":1}`),
	)
	if err != nil {
		t.Fatalf("event 1: %v", err)
	}
	if fire {
		t.Fatal("first event should be absorbed")
	}

	// Wait past the hard timeout
	time.Sleep(150 * time.Millisecond)

	// Next event should fire immediately (timeout exceeded)
	fire, data, err := d.DebounceOrFire(
		context.Background(), def, json.RawMessage(`{"v":2}`),
	)
	if err != nil {
		t.Fatalf("event 2: %v", err)
	}
	// Positive: fires due to hard timeout
	if !fire {
		t.Fatal("expected fire after hard timeout")
	}
	if string(data) != `{"v":2}` {
		t.Fatalf("data = %s, want {\"v\":2}", string(data))
	}
}

func TestDebounceNoConfig(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, _ := jetstream.New(nc)

	d, err := NewDebouncer(js, natsutil.NewTracingPublisher(nc, js))
	if err != nil {
		t.Fatalf("NewDebouncer: %v", err)
	}

	def := TriggerDef{
		ID: "t3", WorkflowID: "wf",
		Subject: &SubjectConfig{Subject: "test.>"},
	}

	// Positive: no debounce config fires immediately
	fire, data, err := d.DebounceOrFire(
		context.Background(), def, json.RawMessage(`{"v":1}`),
	)
	if err != nil {
		t.Fatalf("DebounceOrFire: %v", err)
	}
	if !fire {
		t.Fatal("expected immediate fire without debounce config")
	}
	if string(data) != `{"v":1}` {
		t.Fatalf("data = %s", string(data))
	}
}

func TestDebounceKeyExtraction(t *testing.T) {
	def := TriggerDef{
		ID: "t4", WorkflowID: "wf",
		Subject:  &SubjectConfig{Subject: "test.>"},
		Debounce: &DebounceConfig{Period: 5 * time.Second, Key: "id"},
	}

	// Positive: key extracted from data
	key := debounceKey(def, json.RawMessage(`{"id":"abc"}`))
	if key != "t4.abc" {
		t.Fatalf("key = %q, want t4.abc", key)
	}

	// Negative: missing key falls back to trigger ID
	key = debounceKey(def, json.RawMessage(`{"other":"val"}`))
	if key != "t4" {
		t.Fatalf("key = %q, want t4", key)
	}
}

func TestHandleTimerFireStaleRejection(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	d, err := NewDebouncer(js, natsutil.NewTracingPublisher(nc, js))
	if err != nil {
		t.Fatalf("NewDebouncer: %v", err)
	}
	ctx := context.Background()
	def := TriggerDef{
		ID: "t5", WorkflowID: "wf", Enabled: true,
		Subject:  &SubjectConfig{Subject: "test.>"},
		Debounce: &DebounceConfig{Period: time.Second},
	}
	defData, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal def: %v", err)
	}
	if _, err := d.triggerKV.Put(ctx, "t5", defData); err != nil {
		t.Fatalf("put trigger: %v", err)
	}
	entry, err := json.Marshal(debounceEntry{
		LastEvent:   json.RawMessage(`{"v":1}`),
		FirstSeenNs: time.Now().UnixNano(),
		TimerSeq:    99,
	})
	if err != nil {
		t.Fatalf("marshal entry: %v", err)
	}
	if _, err := d.stateKV.Create(ctx, "t5", entry); err != nil {
		t.Fatalf("create entry: %v", err)
	}
	sub, err := nc.SubscribeSync("history.*")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	timer := engine.TimerMessage{TriggerID: "t5", DebounceKey: "t5"}

	// Negative: a superseded timer (seq 50 != 99) starts no run.
	d.HandleTimerFire(timer, 50)
	if msg, err := sub.NextMsg(300 * time.Millisecond); err == nil {
		t.Fatalf("stale timer started a run: %s", msg.Data)
	}

	// Positive: the current timer starts one and closes the window.
	d.HandleTimerFire(timer, 99)
	if _, err := sub.NextMsg(2 * time.Second); err != nil {
		t.Fatalf("fresh timer started no run: %v", err)
	}
	if _, err := d.stateKV.Get(ctx, "t5"); err == nil {
		t.Fatal("window entry should be deleted after firing")
	}
}

// TestHandleTimerFireSkipsDisabledTrigger: a trigger disabled while its
// window was open starts no run, and the window is still closed.
func TestHandleTimerFireSkipsDisabledTrigger(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	d, err := NewDebouncer(js, natsutil.NewTracingPublisher(nc, js))
	if err != nil {
		t.Fatalf("NewDebouncer: %v", err)
	}
	ctx := context.Background()
	def := TriggerDef{
		ID: "t6", WorkflowID: "wf", Enabled: false,
		Subject:  &SubjectConfig{Subject: "test.>"},
		Debounce: &DebounceConfig{Period: time.Second},
	}
	defData, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal def: %v", err)
	}
	if _, err := d.triggerKV.Put(ctx, "t6", defData); err != nil {
		t.Fatalf("put trigger: %v", err)
	}
	entry, err := json.Marshal(debounceEntry{TimerSeq: 7})
	if err != nil {
		t.Fatalf("marshal entry: %v", err)
	}
	if _, err := d.stateKV.Create(ctx, "t6", entry); err != nil {
		t.Fatalf("create entry: %v", err)
	}
	sub, err := nc.SubscribeSync("history.*")
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	d.HandleTimerFire(engine.TimerMessage{TriggerID: "t6", DebounceKey: "t6"}, 7)
	if msg, err := sub.NextMsg(300 * time.Millisecond); err == nil {
		t.Fatalf("disabled trigger started a run: %s", msg.Data)
	}
	if _, err := d.stateKV.Get(ctx, "t6"); err == nil {
		t.Fatal("window entry should be deleted for a disabled trigger")
	}
}
