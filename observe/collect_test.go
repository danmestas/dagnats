// collect_test.go holds the TELEMETRY-stream collection helpers used
// by this package's integration tests, plus their own unit tests.
//
// These live here (as test-only code in the sole consuming package)
// rather than in dagnatstest or spanread deliberately: dagnatstest
// transitively imports internal/api, which imports observe, so an
// observe test cannot import dagnatstest without a cycle; and spanread
// returns proto tracepb.Span types filtered by run ID with no log
// reader, which cannot serve the log round-trip test below. Records are
// returned as map[string]any so tests assert on wire field names
// without coupling to proto types.
package observe

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const collectBufMax = 1000

// CollectSpans reads from the TELEMETRY stream's telemetry.spans.>
// subjects and returns parsed JSON records. Uses a JetStream consumer
// so it reads messages already on the stream (not just new ones).
// Panics on nil nc or zero timeout (programmer errors).
func CollectSpans(
	t *testing.T,
	nc *nats.Conn,
	timeout time.Duration,
) []map[string]any {
	t.Helper()
	if nc == nil {
		panic("CollectSpans: nc must not be nil")
	}
	if timeout <= 0 {
		panic("CollectSpans: timeout must be positive")
	}
	return collectFromStream(
		t, nc, "telemetry.spans.>", timeout,
	)
}

// CollectLogs reads from the TELEMETRY stream's telemetry.logs.>
// subjects and returns parsed JSON records. Panics on nil nc or zero
// timeout (programmer errors).
func CollectLogs(
	t *testing.T,
	nc *nats.Conn,
	timeout time.Duration,
) []map[string]any {
	t.Helper()
	if nc == nil {
		panic("CollectLogs: nc must not be nil")
	}
	if timeout <= 0 {
		panic("CollectLogs: timeout must be positive")
	}
	return collectFromStream(
		t, nc, "telemetry.logs.>", timeout,
	)
}

// collectFromStream creates an ephemeral consumer on the TELEMETRY
// stream filtered by subject, fetches messages until timeout, and
// parses each as JSON.
func collectFromStream(
	t *testing.T,
	nc *nats.Conn,
	filterSubject string,
	timeout time.Duration,
) []map[string]any {
	t.Helper()
	if filterSubject == "" {
		panic(
			"collectFromStream: filterSubject must not be empty",
		)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("collectFromStream: jetstream: %v", err)
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), timeout,
	)
	defer cancel()

	cons, err := js.CreateOrUpdateConsumer(
		ctx, "TELEMETRY",
		jetstream.ConsumerConfig{
			FilterSubject:     filterSubject,
			DeliverPolicy:     jetstream.DeliverAllPolicy,
			AckPolicy:         jetstream.AckNonePolicy,
			InactiveThreshold: timeout,
		},
	)
	if err != nil {
		t.Fatalf(
			"collectFromStream: consumer %s: %v",
			filterSubject, err,
		)
	}

	var records []map[string]any

	// Fetch in a loop with short waits until context expires.
	for {
		if ctx.Err() != nil {
			break
		}
		msgs, fetchErr := cons.Fetch(
			100,
			jetstream.FetchMaxWait(500*time.Millisecond),
		)
		if fetchErr != nil {
			break
		}
		gotAny := false
		for msg := range msgs.Messages() {
			gotAny = true
			var rec map[string]any
			if jsonErr := json.Unmarshal(
				msg.Data(), &rec,
			); jsonErr != nil {
				t.Errorf(
					"collectFromStream: unmarshal: %v",
					jsonErr,
				)
				continue
			}
			records = append(records, rec)
			if len(records) >= collectBufMax {
				return records
			}
		}
		// If we got no messages this round, we've drained
		// the stream for this filter.
		if !gotAny {
			break
		}
	}

	return records
}

func TestCollectSpans_ReceivesPublishedJSON(t *testing.T) {
	_, nc := startNATS(t)
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
	_, nc := startNATS(t)
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
	_, nc := startNATS(t)
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
