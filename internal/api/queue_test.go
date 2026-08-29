// api/queue_test.go
// Tests for GET /v1/queue (#632). Methodology: real embedded NATS with
// TASK_QUEUES set up via natsutil.SetupAll, publish raw task.* messages
// directly (no consumer, so they stay pending -- work-queue semantics),
// then drive MountV1's handler via httptest and assert on the JSON
// shape.
package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestRESTV1QueueDepth(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx := t.Context()
	for i := 0; i < 3; i++ {
		if _, err := js.Publish(ctx, "task.build", []byte("{}")); err != nil {
			t.Fatalf("publish task.build: %v", err)
		}
	}
	if _, err := js.Publish(ctx, "task.test", []byte("{}")); err != nil {
		t.Fatalf("publish task.test: %v", err)
	}

	svc := NewService(nc)
	mux := http.NewServeMux()
	MountV1(mux, svc)
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/queue")
	if err != nil {
		t.Fatalf("GET /v1/queue: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var out queueDepthResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// Positive: two groups, sorted by task_type, correct pending counts.
	if len(out.Groups) != 2 {
		t.Fatalf("len(groups) = %d, want 2: %+v", len(out.Groups), out.Groups)
	}
	if out.Groups[0].TaskType != "build" || out.Groups[0].Pending != 3 {
		t.Fatalf("groups[0] = %+v, want build/3", out.Groups[0])
	}
	if out.Groups[1].TaskType != "test" || out.Groups[1].Pending != 1 {
		t.Fatalf("groups[1] = %+v, want test/1", out.Groups[1])
	}
	// Negative: oldest_wait_ms must be present (direct-get succeeded)
	// and non-negative, not silently omitted.
	if out.Groups[0].OldestWaitMs == nil || *out.Groups[0].OldestWaitMs < 0 {
		t.Fatalf("groups[0].oldest_wait_ms = %v, want a present, non-negative value",
			out.Groups[0].OldestWaitMs)
	}
	if out.SnapshotAt.IsZero() {
		t.Fatal("snapshot_at is zero, want a real timestamp")
	}
}

func TestRESTV1QueueDepthOldestWaitIncreasesOverTime(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx := t.Context()
	if _, err := js.Publish(ctx, "task.build", []byte("{}")); err != nil {
		t.Fatalf("publish task.build: %v", err)
	}

	svc := NewService(nc)
	mux := http.NewServeMux()
	MountV1(mux, svc)
	server := httptest.NewServer(mux)
	defer server.Close()

	first := getQueueDepth(t, server.URL)
	// Bounded sleep: long enough for a millisecond-resolution wait to
	// visibly grow, short enough to keep the test fast.
	time.Sleep(50 * time.Millisecond)
	second := getQueueDepth(t, server.URL)

	if len(first.Groups) != 1 || len(second.Groups) != 1 {
		t.Fatalf("want exactly one group both times, got %d then %d",
			len(first.Groups), len(second.Groups))
	}
	// Positive: the wait grew across the two calls.
	if *second.Groups[0].OldestWaitMs <= *first.Groups[0].OldestWaitMs {
		t.Fatalf("oldest_wait_ms did not increase: first=%d second=%d",
			*first.Groups[0].OldestWaitMs, *second.Groups[0].OldestWaitMs)
	}
	// Negative: pending count must stay stable (no consumer drained it).
	if second.Groups[0].Pending != 1 {
		t.Fatalf("pending = %d, want 1 (unchanged)", second.Groups[0].Pending)
	}
}

func TestRESTV1QueueDepthEmpty(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)
	mux := http.NewServeMux()
	MountV1(mux, svc)
	server := httptest.NewServer(mux)
	defer server.Close()

	resp, err := http.Get(server.URL + "/v1/queue")
	if err != nil {
		t.Fatalf("GET /v1/queue: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	rawBody := make([]byte, 4096)
	n, _ := resp.Body.Read(rawBody)
	body := string(rawBody[:n])
	// Positive: no pending tasks -> groups:[].
	if !jsonContains(body, `"groups":[]`) {
		t.Fatalf("body = %q, want groups:[]", body)
	}
	// Negative: must not report truncated when there is nothing to
	// truncate.
	if jsonContains(body, `"truncated"`) {
		t.Fatalf("body = %q, want no truncated key", body)
	}
}

func TestRESTV1QueueDepthTruncatesAt256Subjects(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx := t.Context()
	const subjectCount = 300
	for i := 0; i < subjectCount; i++ {
		subject := "task." + taskTypeLabel(i)
		if _, err := js.Publish(ctx, subject, []byte("{}")); err != nil {
			t.Fatalf("publish %s: %v", subject, err)
		}
	}

	svc := NewService(nc)
	mux := http.NewServeMux()
	MountV1(mux, svc)
	server := httptest.NewServer(mux)
	defer server.Close()

	out := getQueueDepth(t, server.URL)
	// Positive: truncated at the documented bound.
	if !out.Truncated {
		t.Fatal("truncated = false, want true for 300 subjects")
	}
	// Negative: exactly 256 groups, not 300 and not fewer.
	if len(out.Groups) != 256 {
		t.Fatalf("len(groups) = %d, want 256", len(out.Groups))
	}
}

// getQueueDepth GETs /v1/queue and decodes the response, failing the
// test on any error.
func getQueueDepth(t *testing.T, baseURL string) queueDepthResponse {
	t.Helper()
	resp, err := http.Get(baseURL + "/v1/queue")
	if err != nil {
		t.Fatalf("GET /v1/queue: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var out queueDepthResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decode: %v", err)
	}
	return out
}

// taskTypeLabel produces a deterministic, sortable 3-digit task-type
// suffix so 300 distinct subjects sort predictably in the truncation
// test.
func taskTypeLabel(i int) string {
	digits := "0123456789"
	return string([]byte{
		digits[i/100%10], digits[i/10%10], digits[i%10],
	})
}
