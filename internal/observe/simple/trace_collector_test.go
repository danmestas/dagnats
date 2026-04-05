// observe/simple/trace_collector_test.go
// Tests for TraceCollector. Methodology: mix of unit tests (LiveSpan with a
// bare channel, no NATS) and integration tests (real embedded NATS server per
// test). Each test uses its own NATS server to avoid cross-test contamination.
// Assertions cover both positive (field correctness) and negative (no
// unexpected values) space with a minimum of 2 assertions per test.
package simple

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
	"github.com/nats-io/nats.go"
)

func TestLiveSpanProducesSpanRecord(t *testing.T) {
	records := make(chan SpanRecord, 8)
	metrics := observe.NewNoopMetrics()

	ctx := context.Background()
	span := newLiveSpan(ctx, "test-op", "engine",
		records, metrics, nil)
	span.SetAttributes(observe.StringAttr("key1", "val1"))
	span.SetStatus(observe.StatusOK, "all good")
	span.End()

	select {
	case rec := <-records:
		// Positive: fields are populated correctly.
		if rec.TraceID == "" {
			t.Fatal("TraceID must not be empty")
		}
		if len(rec.TraceID) != 32 {
			t.Errorf("TraceID length = %d, want 32", len(rec.TraceID))
		}
		if rec.DurationMS < 0 {
			t.Errorf("DurationMS = %d, want >= 0", rec.DurationMS)
		}
		if rec.Name != "test-op" {
			t.Errorf("Name = %q, want test-op", rec.Name)
		}
		if rec.Status != "ok" {
			t.Errorf("Status = %q, want ok", rec.Status)
		}
		// Negative: attributes key matches.
		v, ok := rec.Attributes["key1"]
		if !ok {
			t.Fatal("Attributes missing key1")
		}
		if v != "val1" {
			t.Errorf("Attributes[key1] = %v, want val1", v)
		}
		// Negative: no parent for root span.
		if rec.ParentID != "" {
			t.Errorf("ParentID = %q, want empty for root span",
				rec.ParentID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for SpanRecord")
	}
}

func TestTraceCollectorPublishesToNATS(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("SetupTelemetryStream: %v", err)
	}

	sub, err := js.SubscribeSync("telemetry.spans.>",
		nats.DeliverAll())
	if err != nil {
		t.Fatalf("SubscribeSync: %v", err)
	}
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	metrics := observe.NewNoopMetrics()
	tc := NewTraceCollector(js, "engine", metrics)
	t.Cleanup(func() { tc.Flush() })

	ctx := context.Background()
	_, span := tc.Start(ctx, "build-step",
		observe.WithAttributes(
			observe.StringAttr("run_id", "run-abc-123"),
		),
	)
	span.End()

	msg, err := sub.NextMsg(3 * time.Second)
	if err != nil {
		t.Fatalf("NextMsg: %v", err)
	}

	// Positive: subject contains run_id.
	wantSubject := "telemetry.spans.engine.run-abc-123"
	if msg.Subject != wantSubject {
		t.Errorf("Subject = %q, want %q", msg.Subject, wantSubject)
	}

	var rec SpanRecord
	if err := json.Unmarshal(msg.Data, &rec); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	// Positive: span name matches.
	if rec.Name != "build-step" {
		t.Errorf("Name = %q, want build-step", rec.Name)
	}
	// Negative: service matches.
	if rec.Service != "engine" {
		t.Errorf("Service = %q, want engine", rec.Service)
	}
}

func TestTraceCollectorParentSpanLinking(t *testing.T) {
	records := make(chan SpanRecord, 8)
	metrics := observe.NewNoopMetrics()

	ctx := context.Background()
	parentSpan := newLiveSpan(ctx, "parent-op", "engine",
		records, metrics, nil)
	parentCtx := context.WithValue(ctx, spanContextKey{}, parentSpan)

	childSpan := newLiveSpan(parentCtx, "child-op", "engine",
		records, metrics, nil)
	childSpan.End()
	parentSpan.End()

	// Drain both records.
	var childRec, parentRec SpanRecord
	for i := 0; i < 2; i++ {
		select {
		case rec := <-records:
			if rec.Name == "child-op" {
				childRec = rec
			} else {
				parentRec = rec
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for SpanRecord")
		}
	}

	// Positive: child inherits parent's traceID.
	if childRec.TraceID != parentRec.TraceID {
		t.Errorf("child TraceID = %q, parent TraceID = %q, want equal",
			childRec.TraceID, parentRec.TraceID)
	}
	// Positive: child's parentID is parent's spanID.
	if childRec.ParentID != parentRec.SpanID {
		t.Errorf("child ParentID = %q, parent SpanID = %q, want equal",
			childRec.ParentID, parentRec.SpanID)
	}
	// Negative: parent has no parentID (root span).
	if parentRec.ParentID != "" {
		t.Errorf("parent ParentID = %q, want empty", parentRec.ParentID)
	}
}

func TestTraceCollectorDedup(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}
	if err := natsutil.SetupTelemetryStream(js); err != nil {
		t.Fatalf("SetupTelemetryStream: %v", err)
	}

	rec := SpanRecord{
		TraceID:    "aaaa1111bbbb2222cccc3333dddd4444",
		SpanID:     "eeee5555ffff6666",
		Name:       "dedup-test",
		Service:    "engine",
		Kind:       "internal",
		StartTime:  time.Now().UTC(),
		EndTime:    time.Now().UTC(),
		DurationMS: 1,
		Status:     "ok",
	}

	// Publish the same record twice with the same Nats-Msg-Id.
	publishSpanRecord(js, rec)
	publishSpanRecord(js, rec)

	// Allow time for JetStream to process both publishes.
	time.Sleep(200 * time.Millisecond)

	streamInfo, err := js.StreamInfo("TELEMETRY")
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}

	// Positive: dedup means only 1 message stored.
	if streamInfo.State.Msgs != 1 {
		t.Errorf("stream message count = %d, want 1 (dedup failed)",
			streamInfo.State.Msgs)
	}
	// Negative: not zero messages.
	if streamInfo.State.Msgs == 0 {
		t.Error("stream message count = 0, want at least 1")
	}
}
