// publisher_test.go
// Tests the shared NATS JetStream publisher used by all exporters.
// Uses real embedded NATS — no mocks.
package natsexporter

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startNATS(t *testing.T) (*server.Server, *nats.Conn) {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		Port:      -1,
		JetStream: true,
		StoreDir:  t.TempDir(),
	})
	if err != nil {
		t.Fatalf("start nats: %v", err)
	}
	ns.Start()
	if !ns.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats not ready")
	}
	nc, err := nats.Connect(ns.ClientURL())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		nc.Close()
		ns.Shutdown()
	})
	return ns, nc
}

func setupStream(
	t *testing.T, nc *nats.Conn,
) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	_, err = js.CreateOrUpdateStream(
		context.Background(),
		jetstream.StreamConfig{
			Name:       "TELEMETRY",
			Subjects:   []string{"telemetry.>"},
			Storage:    jetstream.MemoryStorage,
			Duplicates: 5 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return js
}

func TestPublisher_Publish(t *testing.T) {
	_, nc := startNATS(t)
	js := setupStream(t, nc)

	pub := NewPublisher(js)
	data := []byte(`{"test":"value"}`)
	msgID := "trace1.span1"
	subject := "telemetry.spans.test.run1"

	err := pub.Publish(
		context.Background(), subject, data, msgID,
	)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Verify message arrived on stream.
	cons, err := js.CreateOrUpdateConsumer(
		context.Background(), "TELEMETRY",
		jetstream.ConsumerConfig{
			FilterSubject: subject,
		},
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	msgs, err := cons.Fetch(1,
		jetstream.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	count := 0
	for msg := range msgs.Messages() {
		count++
		if string(msg.Data()) != string(data) {
			t.Errorf("data = %s, want %s",
				msg.Data(), data)
		}
	}
	if count != 1 {
		t.Errorf("count = %d, want 1", count)
	}
}

func TestPublisher_Dedup(t *testing.T) {
	_, nc := startNATS(t)
	js := setupStream(t, nc)

	pub := NewPublisher(js)
	data := []byte(`{"dup":"test"}`)
	msgID := "trace1.span1"
	subject := "telemetry.spans.test.run1"

	// Publish same msg ID twice.
	err := pub.Publish(
		context.Background(), subject, data, msgID,
	)
	if err != nil {
		t.Fatalf("first publish: %v", err)
	}
	err = pub.Publish(
		context.Background(), subject, data, msgID,
	)
	if err != nil {
		t.Fatalf("second publish: %v", err)
	}

	// Only one message should exist.
	info, err := js.Stream(
		context.Background(), "TELEMETRY",
	)
	if err != nil {
		t.Fatalf("stream info: %v", err)
	}
	si, err := info.Info(context.Background())
	if err != nil {
		t.Fatalf("info: %v", err)
	}
	if si.State.Msgs != 1 {
		t.Errorf("msgs = %d, want 1 (dedup failed)",
			si.State.Msgs)
	}
}
