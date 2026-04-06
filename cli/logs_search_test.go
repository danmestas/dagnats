// cli/logs_search_test.go
// Tests for the logs search command that replays historical logs from the
// NATS TELEMETRY stream. Methodology: unit tests for flag parsing and
// text matching; integration test with embedded NATS confirms end-to-end
// search with time-based filtering and text search.
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestParseSearchFlags(t *testing.T) {
	t.Run("defaults", func(t *testing.T) {
		flags := parseSearchFlags([]string{})

		// Positive: defaults should be set
		if flags.since != 30*time.Minute {
			t.Fatalf(
				"expected since=30m, got %v", flags.since,
			)
		}
		if flags.limit != 100 {
			t.Fatalf("expected limit=100, got %d", flags.limit)
		}

		// Negative: optional filters should be empty
		if flags.level != "" {
			t.Fatal("level should be empty by default")
		}
		if flags.service != "" {
			t.Fatal("service should be empty by default")
		}
	})

	t.Run("all flags", func(t *testing.T) {
		flags := parseSearchFlags([]string{
			"--level=error",
			"--service=engine",
			"--since=1h",
			"--limit=50",
			"--search=timeout",
			"--json",
		})

		// Positive: all flags parsed
		if flags.level != "error" {
			t.Fatalf("expected level=error, got %q", flags.level)
		}
		if flags.service != "engine" {
			t.Fatalf(
				"expected service=engine, got %q",
				flags.service,
			)
		}
		if flags.since != time.Hour {
			t.Fatalf("expected since=1h, got %v", flags.since)
		}
		if flags.limit != 50 {
			t.Fatalf("expected limit=50, got %d", flags.limit)
		}
		if flags.search != "timeout" {
			t.Fatalf(
				"expected search=timeout, got %q",
				flags.search,
			)
		}
		if !flags.jsonOut {
			t.Fatal("expected jsonOut=true")
		}

		// Negative: since must not be zero
		if flags.since <= 0 {
			t.Fatal("since must be positive")
		}
	})
}

func TestMatchesSearch(t *testing.T) {
	rec := LogRecord{
		Severity:    "error",
		Body:        "Connection timeout to database",
		ServiceName: "engine",
		Timestamp:   time.Now().Format(time.RFC3339Nano),
	}

	t.Run("empty search matches all", func(t *testing.T) {
		// Positive: empty search returns true
		if !matchesSearch(rec, "") {
			t.Fatal("empty search should match")
		}

		// Negative: verify non-empty mismatch
		if matchesSearch(rec, "xyz-no-match") {
			t.Fatal("should not match unrelated text")
		}
	})

	t.Run("case insensitive match", func(t *testing.T) {
		// Positive: lowercase search matches mixed-case body
		if !matchesSearch(rec, "timeout") {
			t.Fatal("should match 'timeout' case-insensitively")
		}

		// Positive: uppercase search matches
		if !matchesSearch(rec, "connection") {
			t.Fatal("should match 'connection'")
		}
	})
}

func TestPrintSearchResults(t *testing.T) {
	oldNoColor := os.Getenv("NO_COLOR")
	os.Setenv("NO_COLOR", "1")
	defer os.Setenv("NO_COLOR", oldNoColor)

	records := []LogRecord{
		{
			Severity:    "error",
			Body:        "task failed",
			ServiceName: "worker",
			Timestamp:   "2025-06-15T14:30:45.000000000Z",
		},
		{
			Severity:    "info",
			Body:        "task started",
			ServiceName: "engine",
			Timestamp:   "2025-06-15T14:30:44.000000000Z",
		},
	}

	t.Run("human format", func(t *testing.T) {
		var buf bytes.Buffer
		printLogSearchResults(&buf, records, false)

		output := buf.String()

		// Positive: contains both records
		if !strings.Contains(output, "task failed") {
			t.Fatal("should contain first record body")
		}
		if !strings.Contains(output, "task started") {
			t.Fatal("should contain second record body")
		}

		// Negative: not JSON format
		if strings.HasPrefix(output, "[") {
			t.Fatal("human format should not start with [")
		}
	})

	t.Run("json format", func(t *testing.T) {
		var buf bytes.Buffer
		printLogSearchResults(&buf, records, true)

		// Positive: valid JSON array
		var parsed []LogRecord
		if err := json.Unmarshal(buf.Bytes(), &parsed); err != nil {
			t.Fatalf("should be valid JSON: %v", err)
		}
		if len(parsed) != 2 {
			t.Fatalf("expected 2 records, got %d", len(parsed))
		}

		// Negative: should not be empty
		if buf.Len() == 0 {
			t.Fatal("JSON output should not be empty")
		}
	})
}

func TestCollectSearchRecords(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Publish mixed logs: 3 errors, 2 info
	publishSearchTestLogs(t, jsLegacy)

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	t.Run("collects all without text filter", func(t *testing.T) {
		cons := createSearchConsumer(t, js, "telemetry.logs.>")
		results := collectSearchRecords(cons, 100, "")

		// Positive: should find all 5 messages
		if len(results) != 5 {
			t.Fatalf("expected 5 results, got %d", len(results))
		}

		// Negative: should not exceed published count
		if len(results) > 5 {
			t.Fatal("should not exceed published count")
		}
	})

	t.Run("text filter narrows results", func(t *testing.T) {
		cons := createSearchConsumer(t, js, "telemetry.logs.>")
		results := collectSearchRecords(cons, 100, "timeout")

		// Positive: only timeout messages match
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}

		// Negative: none should lack 'timeout' in body
		for _, rec := range results {
			lower := strings.ToLower(rec.Body)
			if !strings.Contains(lower, "timeout") {
				t.Fatalf(
					"result should contain 'timeout': %q",
					rec.Body,
				)
			}
		}
	})

	t.Run("limit bounds results", func(t *testing.T) {
		cons := createSearchConsumer(t, js, "telemetry.logs.>")
		results := collectSearchRecords(cons, 2, "")

		// Positive: respects limit
		if len(results) != 2 {
			t.Fatalf("expected 2 results, got %d", len(results))
		}

		// Negative: must not exceed limit
		if len(results) > 2 {
			t.Fatal("should not exceed limit")
		}
	})
}

// publishSearchTestLogs publishes 5 test log records with varied
// severity and body content for search testing.
func publishSearchTestLogs(
	t *testing.T, js nats.JetStreamContext,
) {
	t.Helper()

	logs := []struct {
		subject string
		rec     LogRecord
	}{
		{
			"telemetry.logs.engine.error",
			LogRecord{
				Severity:    "error",
				Body:        "connection timeout",
				ServiceName: "engine",
				Timestamp: time.Now().UTC().Format(
					time.RFC3339Nano),
			},
		},
		{
			"telemetry.logs.worker.info",
			LogRecord{
				Severity:    "info",
				Body:        "task completed",
				ServiceName: "worker",
				Timestamp: time.Now().UTC().Format(
					time.RFC3339Nano),
			},
		},
		{
			"telemetry.logs.engine.error",
			LogRecord{
				Severity:    "error",
				Body:        "request timeout",
				ServiceName: "engine",
				Timestamp: time.Now().UTC().Format(
					time.RFC3339Nano),
			},
		},
		{
			"telemetry.logs.api.info",
			LogRecord{
				Severity:    "info",
				Body:        "server started",
				ServiceName: "api",
				Timestamp: time.Now().UTC().Format(
					time.RFC3339Nano),
			},
		},
		{
			"telemetry.logs.worker.error",
			LogRecord{
				Severity:    "error",
				Body:        "task failed permanently",
				ServiceName: "worker",
				Timestamp: time.Now().UTC().Format(
					time.RFC3339Nano),
			},
		},
	}

	for i, l := range logs {
		data, err := json.Marshal(l.rec)
		if err != nil {
			t.Fatalf("marshal log %d: %v", i, err)
		}
		_, err = js.Publish(l.subject, data)
		if err != nil {
			t.Fatalf("publish log %d: %v", i, err)
		}
	}
}

// createSearchConsumer creates an ordered consumer for search tests
// with DeliverAll policy (since test data was just published).
func createSearchConsumer(
	t *testing.T, js jetstream.JetStream, subject string,
) jetstream.Consumer {
	t.Helper()

	cons, err := js.OrderedConsumer(
		context.Background(), "TELEMETRY",
		jetstream.OrderedConsumerConfig{
			FilterSubjects: []string{subject},
			DeliverPolicy:  jetstream.DeliverAllPolicy,
		},
	)
	if err != nil {
		t.Fatalf("ordered consumer: %v", err)
	}
	return cons
}

func TestFetchSearchResultsIntegration(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	_ = srv
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}

	jsLegacy, err := nc.JetStream()
	if err != nil {
		t.Fatalf("JetStream: %v", err)
	}

	// Publish logs with known timestamps
	for i := 0; i < 3; i++ {
		rec := LogRecord{
			Severity:    "error",
			Body:        "search-test-" + strconv.Itoa(i),
			ServiceName: "searchsvc",
			Timestamp: time.Now().UTC().Format(
				time.RFC3339Nano),
		}
		data, marshalErr := json.Marshal(rec)
		if marshalErr != nil {
			t.Fatalf("marshal: %v", marshalErr)
		}
		_, pubErr := jsLegacy.Publish(
			"telemetry.logs.searchsvc.error", data,
		)
		if pubErr != nil {
			t.Fatalf("publish: %v", pubErr)
		}
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Search with start time 1 hour ago (should find all)
	startTime := time.Now().Add(-time.Hour)
	results := fetchSearchResults(
		js,
		"telemetry.logs.searchsvc.error",
		startTime,
		100,
		"",
	)

	// Positive: should find all 3 records
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Positive: all should be from searchsvc
	for _, rec := range results {
		if rec.ServiceName != "searchsvc" {
			t.Fatalf(
				"expected service=searchsvc, got %q",
				rec.ServiceName,
			)
		}
	}

	// Negative: future start time should find none
	futureStart := time.Now().Add(time.Hour)
	empty := fetchSearchResults(
		js,
		"telemetry.logs.searchsvc.error",
		futureStart,
		100,
		"",
	)
	if len(empty) != 0 {
		t.Fatalf("expected 0 results for future, got %d",
			len(empty))
	}
}
