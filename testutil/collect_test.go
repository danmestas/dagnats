// collect_test.go verifies the telemetry collection helpers
// work against a real embedded NATS server with JetStream.
// No mocks — the helpers must parse real JSON from the
// TELEMETRY stream.
package testutil

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startNATS(t *testing.T) *nats.Conn {
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
	return nc
}

func setupStream(t *testing.T, nc *nats.Conn) jetstream.JetStream {
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

func TestCollectSpans_ReceivesPublishedJSON(t *testing.T) {
	nc := startNATS(t)
	js := setupStream(t, nc)

	// Publish a span-shaped JSON message to the stream.
	payload := map[string]any{
		"name":    "test.span",
		"traceId": "abc123",
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, err = js.Publish(
		context.Background(),
		"telemetry.spans.test.run1",
		data,
	)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	spans := CollectSpans(t, nc, 2*time.Second)

	// Assertion 1: at least one record collected.
	if len(spans) == 0 {
		t.Fatal("expected at least one span record")
	}
	// Assertion 2: name field matches published data.
	name, ok := spans[0]["name"].(string)
	if !ok || name != "test.span" {
		t.Errorf("name = %v, want test.span", spans[0]["name"])
	}
}

func TestCollectLogs_ReceivesPublishedJSON(t *testing.T) {
	nc := startNATS(t)
	js := setupStream(t, nc)

	payload := map[string]any{
		"severity": "info",
		"body":     "hello from test",
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, err = js.Publish(
		context.Background(),
		"telemetry.logs.test.info",
		data,
	)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	logs := CollectLogs(t, nc, 2*time.Second)

	// Assertion 1: at least one record collected.
	if len(logs) == 0 {
		t.Fatal("expected at least one log record")
	}
	// Assertion 2: body field matches published data.
	body, ok := logs[0]["body"].(string)
	if !ok || body != "hello from test" {
		t.Errorf(
			"body = %v, want 'hello from test'",
			logs[0]["body"],
		)
	}
}

func TestCollectSpans_EmptyOnNoMessages(t *testing.T) {
	nc := startNATS(t)
	setupStream(t, nc)

	spans := CollectSpans(t, nc, 1*time.Second)

	// Assertion 1: returns empty or nil slice (not a panic).
	if spans == nil {
		spans = []map[string]any{}
	}
	// Assertion 2: zero records when nothing published.
	if len(spans) != 0 {
		t.Errorf("span count = %d, want 0", len(spans))
	}
}
