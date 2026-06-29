// trigger_delete_confirm_test.go pins that the trigger-delete confirm
// modal does NOT leak DLQ-specific copy.
//
// Methodology:
//   - The trigger detail page reuses the shared dlq-action-modal for its
//     typed-confirm delete. That modal previously hardcoded the static
//     label "Reason this entry is on the DLQ:", which is wrong wording
//     for a TRIGGER delete — a Norman conceptual-model violation (the
//     operator is deleting a trigger, not triaging a dead letter).
//   - Render the trigger detail page through the handler and assert the
//     served markup carries no "DLQ" wording (negative space), while the
//     trigger-appropriate delete affordance is still present (positive).
//   - Own Mount; nothing shared.
package console

import (
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/trigger"
)

// TestTriggerDeleteConfirm_noDLQCopy asserts the trigger detail page's
// confirm modal shows no leaked DLQ wording. RED before the fix (the
// shared modal hardcoded "Reason this entry is on the DLQ:"), GREEN
// after the static DLQ label is removed in favour of JS-driven,
// reason-only copy.
func TestTriggerDeleteConfirm_noDLQCopy(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-del", "alpha", "cron"),
	}
	body := getPage(t, fake, "/console/triggers/cron-del")

	// Scope to the confirm modal's visible card. The page-wide markup
	// legitimately contains "DLQ" (the global nav has a DLQ link) and the
	// shared modal's <script> contains DLQ logic (it's the DLQ modal,
	// reused by triggers). The leak the audit flagged was the STATIC
	// visible label "Reason this entry is on the DLQ:" inside the modal
	// card — a real DOM element the operator sees on a TRIGGER delete.
	card := modalCardMarkup(t, body)
	if strings.Contains(card, "DLQ") {
		t.Errorf("trigger delete confirm modal must not leak visible DLQ copy; got\n%s", card)
	}
	// Positive control: the trigger-delete affordance is still wired.
	if !strings.Contains(body, `data-confirm-url="/console/triggers/cron-del/delete"`) {
		t.Errorf("trigger detail missing the delete confirm wiring")
	}
}

// modalCardMarkup returns the visible confirm-modal card markup: from
// the modal's opening div up to (excluding) the first <script>. That
// span is exactly the user-visible DOM of the typed-confirm modal.
func modalCardMarkup(t *testing.T, html string) string {
	t.Helper()
	open := strings.Index(html, `id="dlq-confirm-modal"`)
	if open < 0 {
		t.Fatalf("trigger detail page missing the confirm modal markup")
	}
	rest := html[open:]
	if script := strings.Index(rest, "<script"); script >= 0 {
		rest = rest[:script]
	}
	return rest
}
