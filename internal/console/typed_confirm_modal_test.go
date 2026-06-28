// typed_confirm_modal_test.go pins the consolidation of destructive-
// action confirmation onto ONE shared typed-confirm modal, reused by
// both the DLQ Retry/Discard flow and trigger delete.
//
// Methodology:
//   - In-memory fakeDataSource feeds the trigger detail + DLQ renders.
//   - httptest.Recorder asserts status + body substrings (literal-key
//     HTML assertions on the wire output).
//   - Each test mounts its own console.Mount; nothing is shared.
//   - Positive value (the data-driven confirm attributes are present)
//     AND negative space (no window.confirm IIFE survives) are asserted.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
)

// TestTriggerDetail_deleteUsesSharedTypedConfirm pins D1: the trigger
// delete control drives the shared typed-confirm modal purely via data
// attributes (confirm word DELETE, the POST url, the CSRF token, a
// display target) — NO window.confirm IIFE, and the shared modal markup
// is present on the page.
func TestTriggerDetail_deleteUsesSharedTypedConfirm(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-1", "alpha", "cron"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/cron-1", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()

	// Positive: the delete button carries the data-driven confirm
	// contract the shared modal reads.
	for _, sub := range []string{
		`data-action-confirm="delete"`,
		`data-confirm-word="DELETE"`,
		`data-confirm-url="/console/triggers/cron-1/delete"`,
		`data-confirm-target="trigger cron-1"`,
		`data-confirm-token=`,
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("trigger detail missing %q on delete control", sub)
		}
	}

	// Negative space: the window.confirm delete IIFE must be gone.
	if strings.Contains(body, "window.confirm(") {
		t.Errorf("trigger detail still ships a window.confirm() delete path")
	}
}

// TestTriggerDetail_includesSharedModalMarkup pins D1: the shared
// typed-confirm modal partial is included on the trigger detail page so
// the data-driven delete control has a modal to drive.
func TestTriggerDetail_includesSharedModalMarkup(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-1", "alpha", "cron"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/cron-1", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `id="dlq-confirm-modal"`) {
		t.Errorf("trigger detail missing shared typed-confirm modal markup")
	}
	if !strings.Contains(body, `id="dlq-confirm-input"`) {
		t.Errorf("trigger detail missing shared modal typed-confirm input")
	}
}

// TestTriggerDetail_modalGatesConfirmDisabled pins D2: the shared modal
// JS HTML-disables the confirm button until the exact word is typed —
// the constraint reflected in the browser model, not just a JS branch.
// We assert the open logic sets goBtn.disabled and the input listener
// re-enables it on an exact match.
func TestTriggerDetail_modalGatesConfirmDisabled(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-1", "alpha", "cron"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/cron-1", nil))
	body := rr.Body.String()
	if !strings.Contains(body, "goBtn.disabled = true") {
		t.Errorf("shared modal does not disable confirm button on open")
	}
	if !strings.Contains(body, "goBtn.disabled = (input.value !== expected)") {
		t.Errorf("shared modal does not gate confirm enable on exact-match input")
	}
}

// TestTriggersList_actionHeaderLabel pins m3: the trigger list action
// column's accessible label is "Actions" (the cell holds Fire AND Edit),
// not the misleading "Fire".
func TestTriggersList_actionHeaderLabel(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-1", "alpha", "cron"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `<span class="sr-only">Actions</span>`) {
		t.Errorf("trigger list action header label is not %q", "Actions")
	}
	if strings.Contains(body, `<span class="sr-only">Fire</span>`) {
		t.Errorf("trigger list still labels the action column %q", "Fire")
	}
}

// TestDLQ_typedConfirmUnchanged is the regression guard: the DLQ
// detail + list still render the shared typed-confirm modal with the
// retry/discard confirm words and per-row hidden forms intact, so the
// consolidation does not regress the working DLQ flow.
func TestDLQ_typedConfirmUnchanged(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{
		sampleDeadLetter(301, "task timed out"),
	}
	h := mountWithFake(t, fake)

	for _, path := range []string{"/console/dlq/301", "/console/dlq"} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("GET %s: status = %d, want 200", path, rr.Code)
		}
		body := rr.Body.String()
		for _, sub := range []string{
			`id="dlq-confirm-modal"`,
			`data-confirm-action="retry"`,
			`data-confirm-action="discard"`,
		} {
			if !strings.Contains(body, sub) {
				t.Errorf("GET %s: DLQ modal regressed, missing %q", path, sub)
			}
		}
	}
}
