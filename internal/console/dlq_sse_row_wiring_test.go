// dlq_sse_row_wiring_test.go drives a headless Chrome via agent-browser
// against a live httptest.Server to prove the DLQ confirm-modal wiring
// survives rows that arrive after the initial page load.
//
// Methodology:
//   - serveSSEDLQ (streams_extra.go) patches new/replaced rows into
//     #dlq-tbody after the page has already rendered — new dead
//     letters land via the KV watch, retried/discarded rows swap via
//     the in-process bus. dlq_action_modal.html's confirm-modal JS
//     used to wire every [data-action-confirm] button with
//     document.querySelectorAll(...).forEach(addEventListener) once,
//     at load time, so any row inserted afterward had a Retry/Discard
//     button with zero click listeners — a silently dead button.
//   - This test reproduces that shape without needing a live SSE
//     round-trip: it inserts a second row into #dlq-tbody via raw DOM
//     (insertAdjacentHTML), mirroring dlq_row.html's markup exactly.
//     The wiring bug is about listener coverage, not the transport,
//     so a direct DOM insert exercises the same code path an SSE
//     patch would.
//   - Skip cleanly when agent-browser / Chrome unavailable, same
//     pattern as the sibling browser smoke tests in this package.
//   - Bounded: 30s deadline. Minimum 2 assertions: the modal actually
//     opens (data-open="true") AND its prompt reflects the inserted
//     row's seq, not the seeded row's — proving the picker matched
//     the right hidden form rather than falling back to the first one
//     on the page.
package console

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/api"
)

func TestBrowser_dlqRowInsertedAfterLoadDiscardWorks(t *testing.T) {
	skipIfBrowserUnavailable(t)
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{{
		DeadLetter: api.DeadLetter{
			Sequence: 501, Subject: "dead.task.seeded", RunID: "r-seeded",
			Error: "seeded failure",
		},
	}}
	h := mountWithFake(t, fake)
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	ctx, cancel := context.WithTimeout(
		context.Background(), 30*time.Second,
	)
	defer cancel()

	runAgentBrowser(t, ctx, "open", srv.URL+"/console/dlq")
	t.Cleanup(func() {
		brCtx, brCancel := context.WithTimeout(
			context.Background(), 5*time.Second,
		)
		defer brCancel()
		runAgentBrowserAllowFail(t, brCtx, "close")
	})
	time.Sleep(750 * time.Millisecond)

	// Insert row #999 into #dlq-tbody after the page (and its modal-
	// wiring script) has already run — the exact shape a live SSE
	// patch produces. Markup mirrors dlq_row.html's hidden form +
	// Discard button precisely.
	const insertJS = `(function () {
  var tbody = document.getElementById("dlq-tbody");
  tbody.insertAdjacentHTML("afterbegin",
    '<tr id="dlq-row-999">' +
    '<td><a href="/console/dlq/999">#999</a></td>' +
    '<td>late-arrival failure</td>' +
    '<td>—</td><td>—</td><td>—</td><td>1</td>' +
    '<td class="dlq-actions">' +
    '<form method="POST" action="/console/dlq/999/discard" ' +
    'data-confirm-action="discard" data-confirm-seq="999" ' +
    'data-confirm-reason="late-arrival failure" ' +
    'data-confirm-target="entry #999" style="display:none;">' +
    '<input type="hidden" name="csrf_token" value="test-token"></form>' +
    '<button type="button" class="btn btn-sm btn-heavy btn-destructive" ' +
    'data-dlq-action="discard" data-dlq-seq="999" ' +
    'data-action-confirm="discard" data-confirm-seq="999">Discard</button>' +
    '</td></tr>');
})()`
	runAgentBrowser(t, ctx, "eval", insertJS)

	// Click the inserted row's Discard button — never present at
	// initial load, so the old per-element addEventListener binding
	// would never have reached it.
	runAgentBrowser(t, ctx, "eval",
		`document.querySelector(`+
			`'[data-dlq-action="discard"][data-dlq-seq="999"]').click()`)
	time.Sleep(100 * time.Millisecond)

	// Assertion 1: the confirm modal actually opened. This is false
	// (attribute absent) on the pre-fix load-time-binding code, since
	// the click never reaches a listener.
	open := evalString(t, ctx,
		`document.getElementById("dlq-confirm-modal")`+
			`.getAttribute("data-open")`)
	if open != "true" {
		t.Fatalf("modal data-open=%q after clicking a post-load row's "+
			"Discard button, want %q — SSE-inserted row wiring regressed",
			open, "true")
	}

	// Assertion 2: the modal picked the NEW row's form (seq 999), not
	// the seeded row's (seq 501) — proves the picker's exact-seq match
	// against the freshly-inserted form, not an accidental fallback.
	prompt := evalString(t, ctx,
		`document.getElementById("dlq-confirm-prompt").textContent`)
	if !strings.Contains(prompt, "entry #999") {
		t.Errorf("modal prompt=%q, want it to reference %q",
			prompt, "entry #999")
	}
}
