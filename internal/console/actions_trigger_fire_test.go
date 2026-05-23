package console

// actions_trigger_fire_test.go exercises POST
// /console/triggers/{id}/fire (#352).
//
// Methodology:
//   - Pure handler tests against fakeDataSource (no NATS).
//   - mountWithFakeRO drives read-only / write-mode toggles; tests
//     opt the limiter into deterministic time via cfg.fireLimit
//     by hitting the handler then reaching into cfg via the test
//     seam exposed through fakeDataSource only when needed.
//   - Minimum 2 assertions per test (state machine + boundary).

import (
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	dnapi "github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
)

// TestTriggerFire_successCron fires an enabled cron trigger and
// asserts the response carries the run id + the success audit row.
func TestTriggerFire_successCron(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-fire-A", "alpha", "cron"),
	}
	fake.fireTriggerRunID = "run-fire-1"
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/triggers/cron-fire-A/fire", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s",
			rr.Code, rr.Body.String())
	}
	if len(fake.fireTriggerCalls) != 1 ||
		fake.fireTriggerCalls[0] != "cron-fire-A" {
		t.Fatalf("FireTrigger calls = %v, want one cron-fire-A",
			fake.fireTriggerCalls)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		`"action":"fire"`,
		`"trigger_id":"cron-fire-A"`,
		`"run_id":"run-fire-1"`,
		`"href":"/console/runs/run-fire-1"`,
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("missing %q in body: %s", sub, body)
		}
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Action != string(ActionTriggerFireManual) ||
		fake.auditEvents[0].Outcome != string(OutcomeSuccess) {
		t.Fatalf("audit events = %+v; want one success",
			fake.auditEvents)
	}
}

// TestTriggerFire_successWebhook also tests the webhook path so the
// kind allow-list isn't accidentally cron-only.
func TestTriggerFire_successWebhook(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("hook-fire-A", "alpha", "webhook"),
	}
	fake.fireTriggerRunID = "run-hook-1"
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/triggers/hook-fire-A/fire", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if len(fake.fireTriggerCalls) != 1 {
		t.Fatalf("FireTrigger calls = %d; want 1",
			len(fake.fireTriggerCalls))
	}
}

// TestTriggerFire_readOnly returns 405 and records denied audit.
func TestTriggerFire_readOnly(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-ro", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/triggers/cron-ro/fire", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405; body=%s",
			rr.Code, rr.Body.String())
	}
	if len(fake.fireTriggerCalls) != 0 {
		t.Errorf("FireTrigger called in read-only mode: %v",
			fake.fireTriggerCalls)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != string(OutcomeDenied) {
		t.Fatalf("audit events = %+v; want one denied",
			fake.auditEvents)
	}
}

// TestTriggerFire_methodCheck rejects GET on the fire endpoint.
func TestTriggerFire_methodCheck(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-meth", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/cron-meth/fire", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	if len(fake.fireTriggerCalls) != 0 {
		t.Errorf("FireTrigger called on GET: %v", fake.fireTriggerCalls)
	}
}

// TestTriggerFire_notFireableKind maps the typed error to 400.
func TestTriggerFire_notFireableKind(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("subj-X", "alpha", "subject"),
	}
	fake.fireTriggerErr = dnapi.ErrTriggerKindNotFireable
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/triggers/subj-X/fire", nil))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rr.Code)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != string(OutcomeDenied) {
		t.Fatalf("audit events = %+v; want one denied",
			fake.auditEvents)
	}
}

// TestTriggerFire_disabledTrigger maps ErrTriggerDisabled to 409.
func TestTriggerFire_disabledTrigger(t *testing.T) {
	fake := newFakeDS()
	td := sampleTrigger("cron-disabled", "alpha", "cron")
	td.Enabled = false
	fake.triggers = []trigger.TriggerDef{td}
	fake.fireTriggerErr = dnapi.ErrTriggerDisabled
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/triggers/cron-disabled/fire", nil))
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409", rr.Code)
	}
	if len(fake.auditEvents) != 1 {
		t.Fatalf("audit events = %d; want 1", len(fake.auditEvents))
	}
}

// TestTriggerFire_rateLimit429 fires the trigger up to the bucket
// limit (10) and asserts the 11th call returns 429 with Retry-After.
// Asserts the body shape introduced for #352 (first 429 in the
// codebase).
func TestTriggerFire_rateLimit429(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-rl", "alpha", "cron"),
	}
	fake.fireTriggerRunID = "run-x"
	h := mountWithFakeRO(t, fake, false)
	for i := 0; i < fireRateLimitDefault; i++ {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
			"/console/triggers/cron-rl/fire", nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("fire #%d status = %d; want 200",
				i+1, rr.Code)
		}
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/triggers/cron-rl/fire", nil))
	if rr.Code != http.StatusTooManyRequests {
		t.Fatalf("11th fire status = %d; want 429; body=%s",
			rr.Code, rr.Body.String())
	}
	retryAfter := rr.Header().Get("Retry-After")
	if retryAfter == "" {
		t.Fatalf("missing Retry-After header on 429")
	}
	n, err := strconv.Atoi(retryAfter)
	if err != nil || n <= 0 {
		t.Fatalf("Retry-After = %q; want positive integer", retryAfter)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		`"error":"rate_limited"`,
		`"trigger_id":"cron-rl"`,
		`"retry_after_seconds":`,
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("missing %q in 429 body: %s", sub, body)
		}
	}
	// Audit row asserts: 10 success + 1 denied = 11 rows, newest first.
	if len(fake.auditEvents) != fireRateLimitDefault+1 {
		t.Fatalf("audit events = %d; want %d",
			len(fake.auditEvents), fireRateLimitDefault+1)
	}
	if fake.auditEvents[0].Outcome != string(OutcomeDenied) {
		t.Fatalf("newest audit outcome = %q; want denied",
			fake.auditEvents[0].Outcome)
	}
}

// TestTriggerFire_limiterMapBound asserts the limiter map stays
// bounded at fireLimiterMax when many distinct trigger IDs fire.
// We exercise the limiter directly here — going through the handler
// 10001 times would be slow and the limiter is the boundary that
// matters.
func TestTriggerFire_limiterMapBound(t *testing.T) {
	c := &fakeClock{t: time.Now()}
	l := newFireRateLimiter(10, 60*time.Second)
	l.now = c.now
	const overflow = fireLimiterMax + 1
	for i := 0; i < overflow; i++ {
		c.advance(time.Millisecond)
		id := "t-" + strconv.Itoa(i)
		if ok, _ := l.Allow(id); !ok {
			t.Fatalf("Allow %q returned false during populate", id)
		}
	}
	if l.Size() > fireLimiterMax {
		t.Fatalf("limiter Size = %d after overflow; want <= %d",
			l.Size(), fireLimiterMax)
	}
	l.mu.Lock()
	_, stillThere := l.entries["t-0"]
	l.mu.Unlock()
	if stillThere {
		t.Fatalf("oldest entry t-0 still present; eviction failed")
	}
}

// TestTriggerFire_subjectKindNotInUI exercises the kind-allow-list
// helper directly so the UI hide-rule and the server reject-rule
// share one source of truth.
func TestTriggerFire_subjectKindNotInUI(t *testing.T) {
	if fireKindAllows("subject") {
		t.Errorf("fireKindAllows(subject) = true; want false")
	}
	if fireKindAllows("http") {
		t.Errorf("fireKindAllows(http) = true; want false")
	}
	if !fireKindAllows("cron") {
		t.Errorf("fireKindAllows(cron) = false; want true")
	}
	if !fireKindAllows("webhook") {
		t.Errorf("fireKindAllows(webhook) = false; want true")
	}
}

// TestTriggerFire_listRowsRenderButtonForCronWebhookOnly asserts the
// triggers list HTML contains a Fire-now button for cron + webhook
// rows and omits it for subject + http rows.
func TestTriggerFire_listRowsRenderButtonForCronWebhookOnly(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-FB", "alpha", "cron"),
		sampleTrigger("hook-FB", "alpha", "webhook"),
		sampleTrigger("subj-FB", "alpha", "subject"),
		sampleTrigger("http-FB", "alpha", "http"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, id := range []string{"cron-FB", "hook-FB"} {
		needle := `data-trigger-id="` + id + `"`
		if !strings.Contains(body, needle) {
			t.Errorf("expected Fire-now button for %s; body has none",
				id)
		}
	}
	for _, id := range []string{"subj-FB", "http-FB"} {
		needle := `data-trigger-id="` + id + `"`
		if strings.Contains(body, needle) {
			t.Errorf("did not expect Fire-now button for %s", id)
		}
	}
}
