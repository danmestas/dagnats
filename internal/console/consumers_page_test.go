// consumers_page_test.go exercises the /console/consumers page and the
// pure consumerRowFrom mapper without standing up NATS.
//
// Methodology:
//   - The page test reuses the fakeDataSource + mountWithFake helpers
//     from pages_test.go. Seeding fake.consumers drives the render so
//     the row/empty-state logic gets coverage without a JetStream
//     consumer existing. Assertions look for stable substrings the
//     template emits (positive space) and confirm fabricated rows are
//     absent (negative space).
//   - consumerRowFrom is a pure function over *jetstream.ConsumerInfo,
//     so its test builds info structs in-memory and asserts the derived
//     Lag / Stalled / AckPolicy / AckWait / MaxDeliver fields directly.
//   - Each page test creates its own console.Mount with the fake; tests
//     never share state.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestServePageConsumers_rendersRows(t *testing.T) {
	fake := newFakeDS()
	fake.consumers = []ConsumerRow{
		{
			Stream: "TASK_QUEUES", Name: "wkr-img",
			Filter: "task.image-pipeline.>", AckPolicy: "explicit",
			NumPending: 4, NumWaiting: 0, Lag: 4,
			AckWait: "60s", MaxDeliver: "-1", Stalled: true,
		},
		{
			Stream: "WORKFLOW_HISTORY", Name: "orchestrator",
			AckPolicy: "explicit", NumWaiting: 1, Lag: 0,
		},
	}
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/consumers", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"wkr-img", "orchestrator", "TASK_QUEUES",
		// html/template escapes the trailing ">" in the filter
		// subject; assert the escaped form the template actually emits.
		"task.image-pipeline.&gt;",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("body missing %q", want)
		}
	}
	// Negative space: a consumer never seeded must not appear.
	if strings.Contains(body, "wkr-phantom") {
		t.Errorf("body contains a fabricated consumer row")
	}
}

func TestServePageConsumers_emptyState(t *testing.T) {
	fake := newFakeDS()
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/consumers", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "no consumers") {
		t.Errorf("empty state missing the no-consumers notice")
	}
	// Negative space: no fabricated row id leaks into the empty page.
	if strings.Contains(body, "consumer-row-") {
		t.Errorf("empty page rendered a consumer row")
	}
}

func TestConsumerRowFrom_computesLagAndStalled(t *testing.T) {
	stalled := &jetstream.ConsumerInfo{
		Name:       "wkr-img",
		Delivered:  jetstream.SequenceInfo{Stream: 96},
		AckFloor:   jetstream.SequenceInfo{Stream: 92},
		NumPending: 4,
		NumWaiting: 0,
		Config: jetstream.ConsumerConfig{
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       60 * time.Second,
			MaxDeliver:    -1,
			FilterSubject: "task.image-pipeline.>",
		},
	}
	row := consumerRowFrom("TASK_QUEUES", stalled)
	if row.Stream != "TASK_QUEUES" {
		t.Errorf("Stream: got %q, want TASK_QUEUES", row.Stream)
	}
	if row.Lag != 4 {
		t.Errorf("Lag: got %d, want 4", row.Lag)
	}
	if !row.Stalled {
		t.Errorf("Stalled: got false, want true")
	}
	if row.AckPolicy != "explicit" {
		t.Errorf("AckPolicy: got %q, want explicit", row.AckPolicy)
	}
	if row.AckWait != "1m0s" {
		t.Errorf("AckWait: got %q, want 1m0s", row.AckWait)
	}
	if row.MaxDeliver != "-1" {
		t.Errorf("MaxDeliver: got %q, want -1", row.MaxDeliver)
	}
	if row.Filter != "task.image-pipeline.>" {
		t.Errorf("Filter: got %q, want task.image-pipeline.>", row.Filter)
	}

	// Non-stalled: pending drained, so no stall even with no pulls.
	healthy := &jetstream.ConsumerInfo{
		Name:       "wkr-img",
		Delivered:  jetstream.SequenceInfo{Stream: 96},
		AckFloor:   jetstream.SequenceInfo{Stream: 96},
		NumPending: 0,
		NumWaiting: 0,
		Config: jetstream.ConsumerConfig{
			AckPolicy: jetstream.AckExplicitPolicy,
		},
	}
	healthyRow := consumerRowFrom("TASK_QUEUES", healthy)
	if healthyRow.Lag != 0 {
		t.Errorf("healthy Lag: got %d, want 0", healthyRow.Lag)
	}
	if healthyRow.Stalled {
		t.Errorf("healthy Stalled: got true, want false")
	}
	if healthyRow.AckWait != "—" {
		t.Errorf("healthy AckWait: got %q, want —", healthyRow.AckWait)
	}

	// AckNone policy: lag still computed; Stalled false when no pending.
	ackNone := &jetstream.ConsumerInfo{
		Name:       "fire-and-forget",
		Delivered:  jetstream.SequenceInfo{Stream: 10},
		AckFloor:   jetstream.SequenceInfo{Stream: 7},
		NumPending: 0,
		NumWaiting: 0,
		Config: jetstream.ConsumerConfig{
			AckPolicy: jetstream.AckNonePolicy,
		},
	}
	noneRow := consumerRowFrom("EVENTS", ackNone)
	if noneRow.Lag != 3 {
		t.Errorf("ackNone Lag: got %d, want 3", noneRow.Lag)
	}
	if noneRow.Stalled {
		t.Errorf("ackNone Stalled: got true, want false")
	}
	if noneRow.AckPolicy != "none" {
		t.Errorf("ackNone AckPolicy: got %q, want none", noneRow.AckPolicy)
	}
}
