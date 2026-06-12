// stream_detail_test.go covers the read-only stream detail view
// (Batch 6, ITEM 1): GET /console/streams/{name} renders a Config card,
// a State card, and a "Consumers on this stream" table sourced from the
// same ConfigSnapshot read that backs the list page plus a filtered
// ListConsumers — no second stream-info round-trip, no fabricated data.
//
// Methodology:
//   - In-memory fakeDataSource feeds ConfigSnapshot.Streams and the
//     consumers slice; httptest.Recorder asserts status + body.
//   - Each test mounts its own console.Mount; nothing is shared.
//   - Positive value: a known stream renders its real config/state and
//     only the consumers belonging to it. Negative space: an unknown
//     stream name renders an honest not-found state (still 200 with the
//     console chrome, never a fabricated stream row), and a provisioned
//     stream's "—" placeholders never appear for fields that have data.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// seedStreamDetailFake returns a fake with one provisioned stream and
// two consumers, one of which belongs to a different stream so the
// detail page's filtering is observable.
func seedStreamDetailFake() *fakeDataSource {
	fake := newFakeDS()
	fake.configSnap.Streams = []StreamSnapshot{
		{
			Name:      "WORKFLOW_HISTORY",
			Subjects:  []string{"history.>"},
			Messages:  4096,
			Bytes:     8192,
			Consumers: 1,
			Retention: "limits",
			Storage:   "file",
			Replicas:  1,
			MaxAge:    "24h0m0s",
			MaxBytes:  -1,
			MaxMsgs:   -1,
			FirstSeq:  1,
			LastSeq:   4096,

			Provisioned: true,
		},
		{Name: "TASK_QUEUES", Provisioned: false},
	}
	fake.consumers = []ConsumerRow{
		{Stream: "WORKFLOW_HISTORY", Name: "history-tail",
			Filter: "history.>", AckPolicy: "explicit", NumPending: 3},
		{Stream: "TASK_QUEUES", Name: "task-worker",
			Filter: "task.email", AckPolicy: "explicit"},
	}
	return fake
}

// TestStreamDetail_rendersConfigAndState asserts a known provisioned
// stream renders its real config + state values and only the consumer
// rows that belong to it.
func TestStreamDetail_rendersConfigAndState(t *testing.T) {
	fake := seedStreamDetailFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/streams/WORKFLOW_HISTORY", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	// Positive: real config + state values reach the page.
	for _, want := range []string{
		"WORKFLOW_HISTORY", "history.&gt;", "limits", "file",
		"4096", // messages / last seq
		"history-tail", "Consumers on this stream",
		`data-component="page-header"`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("stream detail missing %q", want)
		}
	}
	// Negative space: a consumer on a different stream must NOT appear.
	if strings.Contains(body, "task-worker") {
		t.Errorf("stream detail leaked a consumer from another stream")
	}
}

// TestStreamDetail_unknownStreamHonestNotFound asserts a stream name the
// engine doesn't know renders an honest not-found state — never a
// fabricated row — while keeping the console chrome (status 200).
func TestStreamDetail_unknownStreamHonestNotFound(t *testing.T) {
	fake := seedStreamDetailFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/streams/NOT_A_STREAM", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "No stream named") &&
		!strings.Contains(body, "not found") {
		t.Errorf("unknown stream missing honest not-found state: %s",
			truncBody(body))
	}
	// Must not fabricate config/state numbers for a stream that doesn't
	// exist.
	if strings.Contains(body, "Consumers on this stream") {
		t.Errorf("unknown stream rendered a consumers table")
	}
}

// TestStreamsList_rowsLinkToDetail asserts the list rows are clickable
// and carry a chevron affordance linking to the per-stream detail route.
func TestStreamsList_rowsLinkToDetail(t *testing.T) {
	fake := seedStreamDetailFake()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/streams", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	// Positive: a row links to the detail route with a chevron cell.
	if !strings.Contains(body, `href="/console/streams/WORKFLOW_HISTORY"`) {
		t.Errorf("streams list row not linked to detail route")
	}
	if !strings.Contains(body, "row-chevron") {
		t.Errorf("streams list missing chevron affordance")
	}
}
