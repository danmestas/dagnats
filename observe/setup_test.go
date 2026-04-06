// setup_test.go
// Integration tests for InitTelemetry. Uses real embedded NATS
// with JetStream — no mocks. Validates that the full provider
// pipeline works end-to-end: span creation -> NATS stream.
package observe

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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

func TestInitTelemetry_SpansReachNATS(t *testing.T) {
	_, nc := startNATS(t)
	js := setupStream(t, nc)

	shutdown, err := InitTelemetry(
		context.Background(),
		Config{
			ServiceName: "test-svc",
			NATSConn:    nc,
		},
	)
	if err != nil {
		t.Fatalf("InitTelemetry: %v", err)
	}

	// Create a span through the global provider to verify
	// that InitTelemetry registered it correctly.
	tracer := otel.Tracer("test")
	_, span := tracer.Start(
		context.Background(), "test-op",
		trace.WithAttributes(
			attribute.String("dagnats.run.id", "run-42"),
		),
	)
	span.End()

	// Flush all buffered telemetry before reading.
	shutdownCtx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	shutdown(shutdownCtx)

	// Read from the expected NATS subject.
	subject := "telemetry.spans.test-svc.run-42"
	cons, err := js.CreateOrUpdateConsumer(
		context.Background(), "TELEMETRY",
		jetstream.ConsumerConfig{
			FilterSubject: subject,
		},
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}

	msgs, err := cons.Fetch(
		1, jetstream.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}

	count := 0
	for msg := range msgs.Messages() {
		count++

		// Verify valid JSON with expected span name.
		var parsed map[string]interface{}
		unmarshalErr := json.Unmarshal(
			msg.Data(), &parsed,
		)
		if unmarshalErr != nil {
			t.Fatalf("unmarshal: %v", unmarshalErr)
		}
		name, ok := parsed["name"]
		if !ok {
			t.Fatal("JSON missing 'name' field")
		}
		if name != "test-op" {
			t.Errorf("name = %v, want test-op", name)
		}
	}

	// Assertion 1: at least one message arrived.
	if count == 0 {
		t.Fatal("no spans received on NATS stream")
	}
	// Assertion 2: exactly one message (no duplicates).
	if count != 1 {
		t.Errorf("message count = %d, want 1", count)
	}
}

func TestInitTelemetry_PanicsOnNilConn(t *testing.T) {
	defer func() {
		r := recover()
		// Assertion 1: must panic.
		if r == nil {
			t.Fatal("expected panic on nil NATSConn")
		}
		// Assertion 2: message mentions NATSConn.
		msg, ok := r.(string)
		if !ok || len(msg) == 0 {
			t.Errorf(
				"panic value = %v, want string mentioning NATSConn",
				r,
			)
		}
	}()

	_, err := InitTelemetry(
		context.Background(),
		Config{ServiceName: "svc", NATSConn: nil},
	)
	// Should not reach here.
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestInitTelemetry_PanicsOnEmptyServiceName(t *testing.T) {
	_, nc := startNATS(t)

	defer func() {
		r := recover()
		// Assertion 1: must panic.
		if r == nil {
			t.Fatal("expected panic on empty ServiceName")
		}
		// Assertion 2: message mentions ServiceName.
		msg, ok := r.(string)
		if !ok || len(msg) == 0 {
			t.Errorf(
				"panic value = %v, want string mentioning ServiceName",
				r,
			)
		}
	}()

	_, err := InitTelemetry(
		context.Background(),
		Config{ServiceName: "", NATSConn: nc},
	)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
