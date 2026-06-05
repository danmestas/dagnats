// setup_test.go
// Integration tests for InitTelemetry. Uses real embedded NATS
// with JetStream — no mocks. Validates that the full provider
// pipeline works end-to-end: span creation -> NATS stream.
package observe

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
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

func TestInitTelemetry_OTLPHonorsPerSignalEndpointEnv(t *testing.T) {
	// Before #184: dagnats called WithEndpoint(generic) on every
	// OTLP exporter, which overrode the SDK's default behavior of
	// honoring per-signal endpoint env vars. After #184: dagnats
	// passes no explicit endpoint or transport-security option,
	// the SDK reads env vars itself, and OTEL_EXPORTER_OTLP_TRACES_ENDPOINT
	// takes precedence over the generic OTEL_EXPORTER_OTLP_ENDPOINT.
	// genericTraces counts only /v1/traces requests to the
	// generic endpoint; the SDK also sends metrics + logs there
	// when per-signal env vars are not set, and we explicitly
	// don't want to assert on those — only the trace path.
	var genericTraces, perSignalTraces atomic.Int32
	genericSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/v1/traces" {
				genericTraces.Add(1)
			}
			w.WriteHeader(http.StatusOK)
		},
	))
	defer genericSrv.Close()
	tracesSrv := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			perSignalTraces.Add(1)
			w.WriteHeader(http.StatusOK)
		},
	))
	defer tracesSrv.Close()

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", genericSrv.URL)
	t.Setenv(
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", tracesSrv.URL,
	)

	_, nc := startNATS(t)
	shutdown, err := InitTelemetry(
		context.Background(),
		Config{
			ServiceName:  "test-svc",
			NATSConn:     nc,
			OTLPEndpoint: genericSrv.URL,
		},
	)
	if err != nil {
		t.Fatalf("InitTelemetry: %v", err)
	}

	tracer := otel.Tracer("test")
	_, span := tracer.Start(
		context.Background(), "per-signal-test",
	)
	span.End()

	flushCtx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	shutdown(flushCtx)

	// Positive: per-signal traces endpoint received the span.
	if perSignalTraces.Load() == 0 {
		t.Errorf(
			"expected per-signal traces endpoint to receive "+
				"spans; generic /v1/traces=%d "+
				"per-signal=%d",
			genericTraces.Load(), perSignalTraces.Load(),
		)
	}
	// Negative: generic endpoint should not receive /v1/traces
	// requests when per-signal env var is set (SDK precedence
	// rule). Metric/log requests to the generic endpoint are
	// expected and not counted here.
	if genericTraces.Load() > 0 {
		t.Errorf(
			"generic endpoint received %d /v1/traces "+
				"requests; per-signal traces endpoint "+
				"should take precedence",
			genericTraces.Load(),
		)
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

// attrValue is a test helper to extract string value for a resource key.
// Returns "" if absent.
func attrValue(r *resource.Resource, key string) string {
	if r == nil {
		return ""
	}
	for _, kv := range r.Attributes() {
		if string(kv.Key) == key {
			return kv.Value.AsString()
		}
	}
	return ""
}

// TestBuildResource_HonorsEnvAndPrecedence exercises the OTEL_RESOURCE_ATTRIBUTES
// and OTEL_SERVICE_NAME support added to fix #367. Verifies precedence
// cfg.Resource > env > builtins, and that env attrs reach the resource
// (which then flows to OTLP exporters for SigNoz etc).
func TestBuildResource_HonorsEnvAndPrecedence(t *testing.T) {
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "deployment.environment=prd,foo=bar")
	t.Setenv("OTEL_SERVICE_NAME", "env-override")

	cfg := Config{
		ServiceName: "cfg-svc",
		Resource: map[string]string{
			"deployment.environment": "cfg-wins",
			"extra":                  "from-cfg",
		},
	}

	res, err := buildResource(context.Background(), cfg)
	if err != nil {
		t.Fatalf("buildResource: %v", err)
	}

	// Assertion 1: cfg.Resource wins over env for same key.
	if got := attrValue(res, "deployment.environment"); got != "cfg-wins" {
		t.Errorf("deployment.environment = %q, want cfg-wins (cfg > env)", got)
	}

	// Assertion 2: env-only attr is still present.
	if got := attrValue(res, "foo"); got != "bar" {
		t.Errorf("foo = %q, want bar from env", got)
	}

	// Assertion 3: cfg-only extra is present.
	if got := attrValue(res, "extra"); got != "from-cfg" {
		t.Errorf("extra = %q, want from-cfg", got)
	}

	// Assertion 4: our cfg.ServiceName wins for service.name over OTEL_SERVICE_NAME
	// (builtins after fromEnv in detector order).
	if got := attrValue(res, "service.name"); got != "cfg-svc" {
		t.Errorf("service.name = %q, want cfg-svc (forced > env)", got)
	}

	// Assertion 5: a built-in like host.name is still present.
	if got := attrValue(res, "host.name"); got == "" {
		t.Error("expected host.name built-in to be present")
	}
}
