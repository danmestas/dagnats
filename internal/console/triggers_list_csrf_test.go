// triggers_list_csrf_test.go verifies the trigger modal on the triggers
// LIST page renders a non-empty CSRF token under a real auth mode, and
// that a create POST carrying that token passes CSRF verification.
//
// Methodology:
//   - mountWithFakeAuth(AuthForwarded) so the CSRF middleware fires and
//     csrfTokenFor resolves a real (non-loopback) actor.
//   - Positive + negative space: the rendered data-csrf is non-empty AND
//     equals the token the create POST then submits successfully.
package console

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/trigger"
)

// extractDataCSRF pulls the data-csrf="..." value out of the rendered
// trigger modal. Returns "" when the attribute is absent or empty.
func extractDataCSRF(t *testing.T, body string) string {
	t.Helper()
	const marker = `data-csrf="`
	idx := strings.Index(body, marker)
	if idx < 0 {
		return ""
	}
	rest := body[idx+len(marker):]
	end := strings.IndexByte(rest, '"')
	if end < 0 {
		return ""
	}
	return rest[:end]
}

// TestTriggersList_modalCSRFTokenNonEmpty asserts the triggers-list page
// renders the trigger modal with a non-empty data-csrf under forwarded
// auth. An empty token here is the bug the interaction audit flagged:
// the Add POST would send an empty X-CSRF-Token and fail verification.
func TestTriggersList_modalCSRFTokenNonEmpty(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-1", "alpha", "cron"),
	}
	h := mountWithFakeAuth(t, fake, AuthForwarded)
	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/console/triggers", nil)
	req.Header.Set("X-Forwarded-User", "alice")
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	token := extractDataCSRF(t, rr.Body.String())
	if token == "" {
		t.Fatalf("trigger modal data-csrf is empty; Add cannot save")
	}
	want := CSRFTokenForActor(Actor{User: "alice", Source: AuthForwarded})
	if token != want {
		t.Errorf("data-csrf = %q, want actor HMAC %q", token, want)
	}
}

// TestTriggersList_modalTokenPassesCreatePOST closes the loop: the token
// the page rendered is the token a create POST submits, and the POST
// clears CSRF (does not 403). Mirrors the working DLQ-retry CSRF tests.
func TestTriggersList_modalTokenPassesCreatePOST(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeAuth(t, fake, AuthForwarded)

	rr := httptest.NewRecorder()
	getReq := httptest.NewRequest(http.MethodGet, "/console/triggers", nil)
	getReq.Header.Set("X-Forwarded-User", "alice")
	h.ServeHTTP(rr, getReq)
	if rr.Code != http.StatusOK {
		t.Fatalf("GET triggers status = %d, want 200", rr.Code)
	}
	token := extractDataCSRF(t, rr.Body.String())
	if token == "" {
		t.Fatalf("rendered data-csrf is empty; cannot exercise POST")
	}

	form := strings.NewReader(
		"id=new-cron&workflow_id=alpha&type=cron&config=*/5 * * * *&enabled=on")
	postRR := httptest.NewRecorder()
	postReq := httptest.NewRequest(http.MethodPost, "/console/triggers", form)
	postReq.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	postReq.Header.Set("X-Forwarded-User", "alice")
	postReq.Header.Set("X-CSRF-Token", token)
	h.ServeHTTP(postRR, postReq)
	if postRR.Code == http.StatusForbidden {
		t.Fatalf("create POST got 403 (CSRF rejected the rendered token); body=%s",
			postRR.Body.String())
	}
	if strings.Contains(postRR.Body.String(), "csrf_invalid") {
		t.Errorf("create POST body shows csrf_invalid: %s", postRR.Body.String())
	}
	if len(fake.createTriggerCalls) != 1 {
		t.Errorf("CreateTrigger calls = %d; want 1 (POST should reach handler)",
			len(fake.createTriggerCalls))
	}
}
