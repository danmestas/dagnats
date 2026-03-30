// observe/simple/log_collector_test.go
// Tests for LogCollector. Methodology: verify Info/Error calls publish
// correct JSON LogRecords to NATS. Uses real embedded NATS.
package simple

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

func TestLogCollectorInfo(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	lc := NewLogCollector(js, "test-svc")
	sub, err := js.SubscribeSync("telemetry.logs.>", nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	lc.Info("step completed", observe.String("step_id", "s1"))
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var rec LogRecord
	if err := json.Unmarshal(msg.Data, &rec); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if rec.Level != "info" {
		t.Fatalf("Level = %q, want info", rec.Level)
	}
	if rec.Message != "step completed" {
		t.Fatalf("Message = %q, want step completed", rec.Message)
	}
}

func TestLogCollectorError(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	lc := NewLogCollector(js, "test-svc")
	sub, err := js.SubscribeSync("telemetry.logs.>", nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	lc.Error("failed", fmt.Errorf("boom"))
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var rec LogRecord
	if err := json.Unmarshal(msg.Data, &rec); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if rec.Level != "error" {
		t.Fatalf("Level = %q, want error", rec.Level)
	}
	if rec.Error != "boom" {
		t.Fatalf("Error = %q, want boom", rec.Error)
	}
}

func TestLogCollectorWith(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("Setup: %v", err)
	}
	lc := NewLogCollector(js, "test-svc")
	child := lc.With(observe.String("trace_id", "t1"))
	sub, err := js.SubscribeSync("telemetry.logs.>", nats.DeliverAll())
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	child.Info("correlated log")
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}
	var rec LogRecord
	if err := json.Unmarshal(msg.Data, &rec); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if rec.Fields["trace_id"] != "t1" {
		t.Fatalf("trace_id field = %v, want t1", rec.Fields["trace_id"])
	}
	if rec.Message != "correlated log" {
		t.Fatalf("Message = %q, want correlated log", rec.Message)
	}
}
