// nats_pump_test.go drives the aggregator's NATS pump against a real
// embedded JetStream server. Same pattern as the engine integration
// tests: no mocks, bounded waits, isolated NATS per test.
//
// Methodology:
//   - Start an embedded NATS server with JetStream.
//   - Provision the TELEMETRY stream the natsexporter publishes onto.
//   - Push a hand-crafted metric record onto telemetry.metrics.X.Y.
//   - Verify the pump ingests it and Snapshot reflects the point.
//   - 5s bounded wait on every assertion.
//   - Minimum 2 assertions per test (snapshot + subscribe).
package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func startEmbeddedNATS(t *testing.T) (*server.Server, *nats.Conn) {
	t.Helper()
	ns, err := server.NewServer(&server.Options{
		Port: -1, JetStream: true, StoreDir: t.TempDir(),
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

func setupTelemetryStream(t *testing.T, js jetstream.JetStream) {
	t.Helper()
	_, err := js.CreateOrUpdateStream(
		context.Background(),
		jetstream.StreamConfig{
			Name:     "TELEMETRY",
			Subjects: []string{"telemetry.>"},
			Storage:  jetstream.MemoryStorage,
			MaxAge:   1 * time.Hour,
		},
	)
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
}

func publishMetricRecord(
	t *testing.T, js jetstream.JetStream,
	name string, value int64,
) {
	t.Helper()
	data := json.RawMessage(fmt.Sprintf(
		`{"DataPoints":[{"Int":%d}],"Temporality":1,"IsMonotonic":true}`, value,
	))
	rec := metricRecord{
		Name:        name,
		ServiceName: "test-svc",
		Data:        data,
		Timestamp:   time.Now().UTC().Format(time.RFC3339Nano),
	}
	body, err := json.Marshal(rec)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	subj := "telemetry.metrics.test-svc." + name
	if _, err := js.Publish(context.Background(), subj, body); err != nil {
		t.Fatalf("publish: %v", err)
	}
}

func TestPump_IngestsPublishedRecord(t *testing.T) {
	_, nc := startEmbeddedNATS(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	setupTelemetryStream(t, js)
	publishMetricRecord(t, js, "before_pump_counter", 3)

	agg := NewAggregator(silentLogger())
	defer agg.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stop, err := agg.StartPump(ctx, js)
	if err != nil {
		t.Fatalf("StartPump: %v", err)
	}
	defer stop()

	if !waitForSeries(agg, "before_pump_counter", 2*time.Second) {
		t.Fatal("pump did not ingest pre-existing record within 2s")
	}
	got, ok := agg.Snapshot("before_pump_counter")
	if !ok {
		t.Fatal("Snapshot reported missing series")
	}
	if got.Kind != KindCounter {
		t.Fatalf("Kind = %q, want counter", got.Kind)
	}
	if got.Latest().Value != 3 {
		t.Fatalf("Latest.Value = %v, want 3", got.Latest().Value)
	}
}

func TestPump_LiveRecordsTriggerSubscribers(t *testing.T) {
	_, nc := startEmbeddedNATS(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	setupTelemetryStream(t, js)

	agg := NewAggregator(silentLogger())
	defer agg.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stop, err := agg.StartPump(ctx, js)
	if err != nil {
		t.Fatalf("StartPump: %v", err)
	}
	defer stop()

	ch, cancelSub := agg.Subscribe("live_gauge")
	defer cancelSub()

	publishMetricRecord(t, js, "live_gauge", 11)

	select {
	case u := <-ch:
		if u.Name != "live_gauge" {
			t.Fatalf("Name = %q", u.Name)
		}
		if u.Point.Value != 11 {
			t.Fatalf("Value = %v, want 11", u.Point.Value)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("subscriber did not receive live update")
	}
}

// TestPump_RestartAgainstSameStream is the restart-resilience
// regression: a second StartPump against the same TELEMETRY stream
// must succeed (an ephemeral consumer has no immutable durable state
// to conflict on) and resume ingesting. The old durable code returned
// nats error 10012 "start time can not be updated" on the second call,
// which silently disabled the aggregator across engine restarts.
func TestPump_RestartAgainstSameStream(t *testing.T) {
	_, nc := startEmbeddedNATS(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream: %v", err)
	}
	setupTelemetryStream(t, js)

	agg := NewAggregator(silentLogger())
	defer agg.Close()

	// First pump lifecycle: start, then stop cleanly.
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	stop1, err := agg.StartPump(ctx1, js)
	if err != nil {
		t.Fatalf("first StartPump: %v", err)
	}
	stop1()

	// No durable should linger after startup — the legacy durable is
	// best-effort deleted, and the ephemeral consumer carries no name.
	if _, err := js.Consumer(
		context.Background(), "TELEMETRY", PumpConsumerName,
	); err == nil {
		t.Fatalf("legacy durable %q still present after startup", PumpConsumerName)
	}

	// Second pump lifecycle against the SAME stream: must NOT error.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	stop2, err := agg.StartPump(ctx2, js)
	if err != nil {
		t.Fatalf("second StartPump returned error (restart regression): %v", err)
	}
	defer stop2()

	// And the restarted pump still ingests freshly published records.
	publishMetricRecord(t, js, "after_restart_counter", 7)
	if !waitForSeries(agg, "after_restart_counter", 2*time.Second) {
		t.Fatal("restarted pump did not ingest record within 2s")
	}
	got, ok := agg.Snapshot("after_restart_counter")
	if !ok {
		t.Fatal("Snapshot reported missing series after restart")
	}
	if got.Latest().Value != 7 {
		t.Fatalf("Latest.Value = %v, want 7", got.Latest().Value)
	}
}

// waitForSeries polls Snapshot until the named series exists or the
// deadline expires. Returns true on success. Bounded loop count.
func waitForSeries(
	a *Aggregator, name string, max time.Duration,
) bool {
	deadline := time.Now().Add(max)
	const maxIter = 200
	for i := 0; i < maxIter; i++ {
		if _, ok := a.Snapshot(name); ok {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(25 * time.Millisecond)
	}
	return false
}
