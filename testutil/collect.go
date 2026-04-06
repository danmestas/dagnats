// Package testutil provides helpers for collecting OTel telemetry
// from the NATS TELEMETRY stream in integration tests. Records
// are returned as map[string]any for flexible test assertions
// without coupling to proto types.
package testutil

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const collectBufMax = 1000

// CollectSpans reads from the TELEMETRY stream's
// telemetry.spans.> subjects and returns parsed JSON records.
// Uses a JetStream consumer so it reads messages already on
// the stream (not just new ones). Panics on nil nc or zero
// timeout (programmer errors).
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

// CollectLogs reads from the TELEMETRY stream's
// telemetry.logs.> subjects and returns parsed JSON records.
// Panics on nil nc or zero timeout (programmer errors).
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

// collectFromStream creates an ephemeral ordered consumer on
// the TELEMETRY stream filtered by subject, fetches messages
// until timeout, and parses each as JSON.
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
