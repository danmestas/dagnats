// dispatchspan_test.go
// Verifies that bridge span volume is proportional to work done, and
// that the per-task dispatch span joins the engine's enqueue trace
// rather than rooting a fresh one (issue #531; regression guard for
// #527/#528).
//
// Methodology: real NATS server, real bridge, real HTTP roundtrip.
// The Bridge's tracer field is swapped for one backed by a
// per-test synchronous tracetest.InMemoryExporter (spans visible the
// instant span.End() returns) so no global tracer provider is
// mutated. The global propagator IS set, matching the precedent in
// bridge/tracecontext_test.go:51 —
// precedent: internal/engine/task_publisher_test.go:269. Trace-chain
// assertions read the raw *nats.Msg headers off WORKFLOW_HISTORY,
// precedent: bridge/tracecontext_test.go.
package bridge

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/protocol"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// storeBudgetBytes caps the JetStream store these tests reserve. They
// move a handful of small messages; the 64 MiB ceiling keeps them
// runnable on a host without 10 GiB of free disk.
const storeBudgetBytes int64 = 64 << 20

// recordBridgeSpans swaps b's tracer for one exporting synchronously
// into a fresh in-memory buffer, leaving the global provider alone.
func recordBridgeSpans(
	t *testing.T, b *Bridge,
) *tracetest.InMemoryExporter {
	t.Helper()
	if b == nil {
		t.Fatalf("recordBridgeSpans: b must not be nil")
	}
	exporter := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)
	t.Cleanup(func() {
		if err := provider.Shutdown(context.Background()); err != nil {
			t.Logf("provider shutdown: %v", err)
		}
	})
	b.tracer = provider.Tracer("test")
	return exporter
}

// countSpansNamed returns how many recorded spans carry name.
func countSpansNamed(
	spans tracetest.SpanStubs, name string,
) int {
	count := 0
	for _, s := range spans {
		if s.Name == name {
			count++
		}
	}
	return count
}

// postPoll issues one poll request and returns the decoded tasks.
// timeoutMs is bounded by the caller so no test wait is open-ended.
func postPoll(
	t *testing.T, ts *httptest.Server,
	taskTypes string, maxTasks int, timeoutMs int,
) []pollResponse {
	t.Helper()
	body := fmt.Sprintf(
		`{"task_types":["%s"],"max_tasks":%d,"timeout_ms":%d}`,
		taskTypes, maxTasks, timeoutMs,
	)
	resp, err := http.Post(
		ts.URL+"/v1/tasks/poll",
		"application/json",
		strings.NewReader(body),
	)
	if err != nil {
		t.Fatalf("poll failed: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("poll status: got %d, want 200", resp.StatusCode)
	}
	var tasks []pollResponse
	if err := json.NewDecoder(resp.Body).Decode(&tasks); err != nil {
		t.Fatalf("decode poll response: %v", err)
	}
	return tasks
}

// TestPollSpanVolumeIsWorkProportional pins the core of #531: an idle
// poll must cost zero spans, and a poll that hands out N tasks must
// cost exactly N bridge.dispatch spans — never a per-request span.
func TestPollSpanVolumeIsWorkProportional(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	// Bounded store budget: these tests need a few KB, and the default
	// 10 GiB budget makes stream creation fail on a host with less
	// free disk than that.
	err := natsutil.SetupAll(nc, natsutil.WithStoreBudget(storeBudgetBytes))
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := newTestBridge(t, nc)
	exporter := recordBridgeSpans(t, b)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// Negative space: an empty poll is pure waiting, not work. Uses a
	// task type of its own so the idle poll's ephemeral consumer
	// cannot interfere with the dispatch poll below (consumer churn is
	// issue #532, out of scope here).
	empty := postPoll(t, ts, "quiet", 5, 300)
	if len(empty) != 0 {
		t.Fatalf("expected 0 tasks from idle poll, got %d", len(empty))
	}
	if got := len(exporter.GetSpans()); got != 0 {
		t.Fatalf(
			"idle poll emitted %d spans, want 0: %v",
			got, exporter.GetSpans(),
		)
	}

	// Positive space: N dispatched tasks yield exactly N spans.
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	const taskCount = 3
	for i := 0; i < taskCount; i++ {
		payload := protocol.TaskPayload{
			RunID:  fmt.Sprintf("run-vol-%d", i),
			StepID: "step-vol",
			Input:  json.RawMessage(`{"x":1}`),
		}
		data, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		_, err = js.Publish(
			context.Background(),
			"task.echo."+payload.RunID, data,
		)
		if err != nil {
			t.Fatalf("publish task: %v", err)
		}
	}

	tasks := postPoll(t, ts, "echo", taskCount, 3000)
	if len(tasks) != taskCount {
		t.Fatalf("expected %d tasks, got %d", taskCount, len(tasks))
	}
	spans := exporter.GetSpans()
	if got := countSpansNamed(spans, "bridge.dispatch"); got != taskCount {
		t.Fatalf(
			"got %d bridge.dispatch spans, want %d (all: %d)",
			got, taskCount, len(spans),
		)
	}
	if got := countSpansNamed(spans, "bridge.poll"); got != 0 {
		t.Fatalf("bridge.poll span still emitted %d times", got)
	}
}

// TestDispatchSpanJoinsInboundTaskTrace is the #527/#528 regression
// guard. The trace ID that reaches the published step.started must be
// the one the ENGINE injected on the inbound task message — asserting
// only that it matches the dispatch span's own trace ID would pass
// even if bridge.dispatch were an orphan root.
func TestDispatchSpanJoinsInboundTaskTrace(t *testing.T) {
	otel.SetTextMapPropagator(
		propagation.NewCompositeTextMapPropagator(
			propagation.TraceContext{},
			propagation.Baggage{},
		),
	)

	_, nc := natsutil.StartTestServer(t)
	// Bounded store budget: these tests need a few KB, and the default
	// 10 GiB budget makes stream creation fail on a host with less
	// free disk than that.
	err := natsutil.SetupAll(nc, natsutil.WithStoreBudget(storeBudgetBytes))
	if err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	b := newTestBridge(t, nc)
	recordBridgeSpans(t, b)
	ts := httptest.NewServer(b.Handler())
	defer ts.Close()

	// Stand in for the engine's enqueueTask injection: a task message
	// carrying a known W3C traceparent.
	const inboundTP = "00-4bf92f3577b34da6a3ce929d0e0e4736-" +
		"00f067aa0ba902b7-01"
	wantTraceID := w3cTraceID(t, inboundTP)

	const runID = "run-chain"
	payload := protocol.TaskPayload{
		RunID:  runID,
		StepID: "step-chain",
		Input:  json.RawMessage(`{"x":1}`),
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	hdr := nats.Header{}
	hdr.Set("traceparent", inboundTP)
	_, err = js.PublishMsg(context.Background(), &nats.Msg{
		Subject: "task.echo." + runID,
		Data:    data,
		Header:  hdr,
	})
	if err != nil {
		t.Fatalf("publish task msg: %v", err)
	}

	tasks := postPoll(t, ts, "echo", 1, 3000)
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task, got %d", len(tasks))
	}

	started := fetchHistoryMsg(
		t, nc, runID, protocol.EventStepStarted, 2*time.Second,
	)
	gotTP := started.Headers().Get("traceparent")
	if gotTP == "" {
		t.Fatalf(
			"step.started carries no traceparent; headers=%v",
			started.Headers(),
		)
	}
	if gotTraceID := w3cTraceID(t, gotTP); gotTraceID != wantTraceID {
		t.Fatalf(
			"step.started trace_id=%s, want inbound task trace_id=%s",
			gotTraceID, wantTraceID,
		)
	}
}

// fetchHistoryMsg returns the raw history message of wantType for a
// run. Raw, not protocol.Event: headers are the live stitching
// carrier and Unmarshal would drop them. Bounded by
// historyEventScanMax and timeout.
func fetchHistoryMsg(
	t *testing.T,
	nc *nats.Conn,
	runID string,
	wantType protocol.EventType,
	timeout time.Duration,
) jetstream.Msg {
	t.Helper()
	if nc == nil {
		t.Fatalf("fetchHistoryMsg: nc must not be nil")
	}
	if runID == "" {
		t.Fatalf("fetchHistoryMsg: runID must not be empty")
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx := context.Background()
	stream, err := js.Stream(ctx, "WORKFLOW_HISTORY")
	if err != nil {
		t.Fatalf("Stream: %v", err)
	}
	cons, err := stream.CreateOrUpdateConsumer(
		ctx, jetstream.ConsumerConfig{
			FilterSubject:     "history." + runID,
			AckPolicy:         jetstream.AckNonePolicy,
			DeliverPolicy:     jetstream.DeliverAllPolicy,
			InactiveThreshold: timeout,
		},
	)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	fetched, err := cons.Fetch(
		historyEventScanMax, jetstream.FetchMaxWait(timeout),
	)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	for msg := range fetched.Messages() {
		var evt protocol.Event
		if err := json.Unmarshal(msg.Data(), &evt); err != nil {
			t.Fatalf("unmarshal scanned event: %v", err)
		}
		if evt.Type == wantType {
			return msg
		}
	}
	t.Fatalf("no %s message for run %s", wantType, runID)
	return nil
}
