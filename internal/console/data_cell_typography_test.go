// data_cell_typography_test.go pins remediation theme T3 — the
// data-cell typography + semantic coloring that matches the MagicPath
// prototype: IoskeleyMono (the --font-mono face) on every data table
// cell, and honest red/amber emphasis only where a real value signals a
// problem.
//
// Methodology:
//   - CSS check reads the embedded app.css and asserts the shared rule
//     `.console-table tbody td { font-family: var(--font-mono); }`
//     exists so all data cells render in the mono face, and that the
//     status color helpers (.status-failed red, .status-pending amber)
//     are still defined to back the semantic cell coloring. Pure string
//     assertions, no NATS.
//   - Template checks render the consumers + task-types pages through
//     the fakeDataSource + mountWithFake helpers and assert the
//     conditional semantic class is emitted ONLY when the seeded value
//     warrants it (positive space) and absent on a nominal row (negative
//     space). This is the honesty contract: color tracks the real value.
package console

import (
	"io/fs"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestAppCSS_dataCellsRenderMono(t *testing.T) {
	body, err := fs.ReadFile(assetsFS, "assets/app.css")
	if err != nil {
		t.Fatalf("read app.css: %v", err)
	}
	css := normalizeWhitespace(string(body))
	// The deep fix: every data table cell inherits the mono face so IDs,
	// counts, durations, timestamps, and statuses all render in
	// IoskeleyMono — not the proportional sans the cells used before.
	want := ".console-table tbody td { font-family: var(--font-mono); }"
	if !strings.Contains(css, want) {
		t.Errorf("app.css missing the data-cell mono rule %q", want)
	}
	// Definition-list value cells (Server identity + capacity, dlq/trigger
	// detail) are data too — they must render mono via .console-deflist dd,
	// not only <table> cells (the gap that left the Server page in sans).
	wantDL := ".console-deflist dd { margin: 0; font-family: var(--font-mono); }"
	if !strings.Contains(css, wantDL) {
		t.Errorf("app.css missing the deflist data-cell mono rule %q", wantDL)
	}
	// The semantic color helpers must stay defined — the templates color
	// failure/alarm cells by reusing these, not by inventing parallel
	// classes.
	for _, sub := range []string{
		".status-failed { color: var(--status-failed); }",
		".status-pending { color: var(--status-pending); }",
	} {
		if !strings.Contains(css, sub) {
			t.Errorf("app.css missing semantic color helper %q", sub)
		}
	}
}

// normalizeWhitespace collapses runs of spaces/newlines/tabs to a single
// space so the CSS assertions above are insensitive to the exact
// formatting of the rule in the file.
func normalizeWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// rowContaining returns the <tr>…</tr> fragment that contains marker, so a
// test can assert on a single rendered table row in isolation — used to
// prove a NOMINAL row carries no semantic color class (the honesty
// invariant: an unconditional-coloring bug must fail the test).
func rowContaining(t *testing.T, html, marker string) string {
	t.Helper()
	i := strings.Index(html, marker)
	if i < 0 {
		t.Fatalf("marker %q not found in rendered body", marker)
	}
	start := strings.LastIndex(html[:i], "<tr")
	end := strings.Index(html[i:], "</tr>")
	if start < 0 || end < 0 {
		t.Fatalf("could not isolate the <tr> row for %q", marker)
	}
	return html[start : i+end]
}

func TestServePageConsumers_colorsAlarmCellsHonestly(t *testing.T) {
	fake := newFakeDS()
	fake.consumers = []ConsumerRow{
		// Explicit-ack work consumer with a real backlog: pending + lag
		// must turn red, redelivered amber.
		{
			Stream: "TASK_QUEUES", Name: "wkr-busy",
			AckPolicy: "explicit", NumPending: 7, NumRedelivered: 3,
			NumWaiting: 0, Lag: 5, Stalled: true,
		},
		// Nominal consumer: drained, no redelivery — nothing colored.
		{
			Stream: "WORKFLOW_HISTORY", Name: "wkr-idle",
			AckPolicy: "explicit", NumPending: 0, NumRedelivered: 0,
			NumWaiting: 1, Lag: 0,
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

	// Positive space: the backlogged work consumer emits the failure +
	// warning helpers on its pending / lag / redelivered cells.
	if c := strings.Count(body, `class="status-failed"`); c < 2 {
		t.Errorf("expected pending+lag red on the backlogged consumer; "+
			"status-failed count = %d, want >= 2", c)
	}
	if !strings.Contains(body, `class="status-pending"`) {
		t.Errorf("expected redelivered amber on the backlogged consumer")
	}

	// Negative space (honesty): the nominal drained consumer (wkr-idle,
	// all zeros) must carry NO semantic color — isolate its row and assert
	// no status-* class. This pins the invariant: an unconditional coloring
	// that falsely reddened a healthy 0 must fail here.
	idle := rowContaining(t, body, "wkr-idle")
	if strings.Contains(idle, "status-failed") || strings.Contains(idle, "status-pending") {
		t.Errorf("nominal consumer wkr-idle must not carry semantic color: %s", idle)
	}
}

func TestServePageTaskTypes_colorsFailRateHonestly(t *testing.T) {
	fake := newFakeDS()
	fake.taskTypeRows = []TaskTypeRow{
		// Real failure rate > 0 → fail-rate cell turns red.
		{
			TaskType: "billing::charge", Service: "billing",
			OwnerWorkerIDs:    []string{"w1"},
			RecentInvocations: 40, AvgDurationMS: 120, FailureRate: 12.5,
		},
		// Not measured (-1) and clean (0) rows must stay nominal.
		{
			TaskType: "email", Service: defaultServiceGroup,
			OwnerWorkerIDs:    []string{"w2"},
			RecentInvocations: -1, AvgDurationMS: -1, FailureRate: -1,
		},
		{
			TaskType: "billing::refund", Service: "billing",
			OwnerWorkerIDs:    []string{"w1"},
			RecentInvocations: 10, AvgDurationMS: 90, FailureRate: 0,
		},
	}
	handler := mountWithFake(t, fake)

	req := httptest.NewRequest(http.MethodGet, "/console/task-types", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status: got %d, want %d", rec.Code, http.StatusOK)
	}
	body := rec.Body.String()

	// A task type with a real failure rate > 0 turns its fail-rate cell
	// red; the not-measured (-1) and zero-fail rows stay nominal.
	if !strings.Contains(body, `class="status-failed"`) {
		t.Errorf("expected fail-rate cell red on the failing task type")
	}

	// Negative space (honesty): the not-measured (email, -1) and zero-fail
	// (billing::refund) rows must stay nominal — no red fail-rate cell.
	for _, nominal := range []string{"email", "billing::refund"} {
		if row := rowContaining(t, body, nominal); strings.Contains(row, "status-failed") {
			t.Errorf("nominal task type %q must not carry a red fail-rate cell: %s", nominal, row)
		}
	}
}
