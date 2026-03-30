// observe/simple/monitor_test.go
// Tests for StorageMonitor. Methodology: real embedded NATS with tiny
// stream to trigger threshold. Asserts advisory content and subject.
package simple

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/nats-io/nats.go"
)

func TestStorageMonitorPublishesAdvisory(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TELEMETRY",
		Subjects: []string{"telemetry.>"},
		MaxBytes: 1024,
		Storage:  nats.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("AddStream: %v", err)
	}
	sub, err := nc.SubscribeSync("alerts.storage.>")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	bigPayload := make([]byte, 900)
	if _, err := js.Publish("telemetry.spans.test.r1", bigPayload); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mon := NewStorageMonitor(nc, 100*time.Millisecond, 0.8)
	go mon.Start(ctx)
	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("no advisory received: %v", err)
	}
	if !bytes.Contains(msg.Data, []byte("TELEMETRY")) {
		t.Fatal("advisory should mention TELEMETRY stream")
	}
	cancel()
}

func TestStorageMonitorNilPanic(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("NewStorageMonitor with nil nc should panic")
		}
	}()
	NewStorageMonitor(nil, time.Second, 0.8)
}
