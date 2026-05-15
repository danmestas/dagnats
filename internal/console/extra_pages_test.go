// extra_pages_test.go exercises the PR 4 surfaces: triggers list +
// detail, DLQ list + detail, audit log, run-id lookup, layout-wrapped
// 404, the read-only middleware, the audit emitter, and the
// destructive-action affordances.
//
// Methodology:
//   - Pure handler tests against fakeDataSource (no NATS).
//   - Each subtest sets up its own fake; tests never share state.
//   - Assertions look for stable substrings so cosmetic tweaks don't
//     break the contract.
//   - Read-only / mutation tests assert both the response shape AND
//     the side-effects (e.g. EmitAuditEvent call count, ReplayDeadLetter
//     call count).
package console

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/trigger"
)

func mountWithFakeRO(
	t *testing.T, fake *fakeDataSource, readOnly bool,
) http.Handler {
	t.Helper()
	if fake == nil {
		t.Fatalf("mountWithFakeRO: fake is nil")
	}
	return Mount(Config{
		HTTPAddr: "127.0.0.1:0",
		AuthMode: AuthLoopback,
		Build:    "test",
		Logger:   slog.New(slog.NewTextHandler(testLogWriter(t), nil)),
		Data:     fake,
		ReadOnly: readOnly,
	})
}

func sampleTrigger(id, workflow, kind string) trigger.TriggerDef {
	td := trigger.TriggerDef{
		ID: id, WorkflowID: workflow, Enabled: true,
	}
	switch kind {
	case "cron":
		td.Cron = &trigger.CronConfig{Expression: "*/5 * * * *"}
	case "webhook":
		td.Webhook = &trigger.WebhookConfig{
			Path: "/hooks/" + id, Secret: "secret-secret-secret",
		}
	case "subject":
		td.Subject = &trigger.SubjectConfig{Subject: "demo.events.>"}
	case "http":
		td.HTTP = &trigger.HTTPConfig{
			Path: "/api/" + id, Method: "POST",
			TimeoutMs: 5000, MaxBodyBytes: 1024,
		}
	}
	return td
}

// TestTriggersList_rendersAllKinds asserts each trigger kind renders
// with its kind badge + target. Methodology: seed 4 triggers (one per
// kind), GET /console/triggers, look for stable substrings.
func TestTriggersList_rendersAllKinds(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-1", "alpha", "cron"),
		sampleTrigger("hook-1", "alpha", "webhook"),
		sampleTrigger("subj-1", "alpha", "subject"),
		sampleTrigger("http-1", "alpha", "http"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	wantSubs := []string{
		"Triggers",
		"cron-1",
		"hook-1",
		"subj-1",
		"http-1",
		`data-kind="cron"`,
		`data-kind="webhook"`,
		`data-kind="subject"`,
		`data-kind="http"`,
	}
	for _, sub := range wantSubs {
		if !strings.Contains(body, sub) {
			t.Errorf("missing %q in triggers list", sub)
		}
	}
}

// TestTriggersList_typeFilter narrows to one kind.
func TestTriggersList_typeFilter(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-1", "alpha", "cron"),
		sampleTrigger("hook-1", "alpha", "webhook"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers?type=webhook", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "hook-1") {
		t.Errorf("expected webhook trigger in filtered list")
	}
	if strings.Contains(body, "cron-1") {
		t.Errorf("did not expect cron trigger in webhook-only filter")
	}
}

// TestTriggerDetail_webhookSignature shows the signature banner
// when Webhook.Secret is set.
func TestTriggerDetail_webhookSignature(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("hook-1", "alpha", "webhook"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/hook-1", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "HMAC signature required") {
		t.Errorf("expected signature banner")
	}
	if !strings.Contains(body, "hook-1") {
		t.Errorf("expected trigger id in detail page")
	}
}

// TestTriggerDetail_notFound renders the not-found message inline
// (no separate 404 page swap) so the operator sees consistent chrome.
func TestTriggerDetail_notFound(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/nonexistent", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (inline not-found)", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "No trigger registered") {
		t.Errorf("expected inline 'no trigger registered' message")
	}
}

func sampleDeadLetter(seq uint64, errStr string) api.DeadLetterView {
	dl := api.DeadLetter{
		Sequence:      seq,
		Subject:       "dead.task.alpha.first",
		RunID:         "run-failed-1",
		StepID:        "first",
		Task:          "task.alpha.first",
		Error:         errStr,
		Timestamp:     time.Now().UTC().Add(-time.Minute),
		Body:          []byte(`{"hello":"world"}`),
		DeliveryCount: 5,
		Consumer:      "alpha.first",
	}
	return api.DeadLetterView{DeadLetter: dl, BodyPreserved: true}
}

// TestDLQList_zeroState shows the reassuring message when empty.
func TestDLQList_zeroState(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/dlq", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "workflows are healthy") {
		t.Errorf("expected reassuring zero-state message")
	}
}

// TestDLQList_rendersEntries lists DLQ rows + classifies reasons.
func TestDLQList_rendersEntries(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{
		sampleDeadLetter(101, "task timed out after 30s"),
		sampleDeadLetter(102, "panic: nil pointer"),
		sampleDeadLetter(103, "unrecoverable: bad signature"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/dlq", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		"Dead-letter queue",
		"#101",
		"#102",
		"#103",
		"task timed out",
		"panic",
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("missing %q in DLQ list", sub)
		}
	}
}

// TestDLQList_reasonFilter narrows to one class.
func TestDLQList_reasonFilter(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{
		sampleDeadLetter(201, "task timed out after 30s"),
		sampleDeadLetter(202, "panic: nil pointer"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/dlq?reason=timeout", nil))
	body := rr.Body.String()
	if !strings.Contains(body, "#201") {
		t.Errorf("expected #201 (timeout) in filtered list")
	}
	if strings.Contains(body, "#202") {
		t.Errorf("did not expect #202 (panic) in timeout filter")
	}
}

// TestDLQDetail_rendersAndExposesActions confirms the detail page
// renders the reason, body preview, and BOTH retry/discard buttons.
func TestDLQDetail_rendersAndExposesActions(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{
		sampleDeadLetter(301, "task timed out"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/dlq/301", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		"Dead letter #301",
		`action="/console/dlq/301/retry"`,
		`action="/console/dlq/301/discard"`,
		`data-action-confirm="retry"`,
		`data-action-confirm="discard"`,
		"btn-restorative",
		"btn-destructive",
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("missing %q in DLQ detail", sub)
		}
	}
}

// TestDLQDetail_readOnlyDisablesButtons asserts visible-but-disabled
// state when ReadOnly is on. Both buttons get the disabled attr +
// an explanatory tooltip.
func TestDLQDetail_readOnlyDisablesButtons(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{
		sampleDeadLetter(401, "panic"),
	}
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/dlq/401", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, `disabled aria-disabled="true"`) {
		t.Errorf("expected disabled buttons in read-only mode")
	}
	if !strings.Contains(body, "Console is read-only") {
		t.Errorf("expected read-only tooltip explanation")
	}
	if !strings.Contains(body, "CONSOLE_READ_ONLY") {
		t.Errorf("expected env var hint in read-only banner")
	}
}

// TestDLQRetry_mutating exercises the happy path: POST returns 200,
// ReplayDeadLetter called, discard called, audit emitted with
// "success" outcome.
func TestDLQRetry_mutating(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{
		sampleDeadLetter(501, "panic"),
	}
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/dlq/501/retry", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if len(fake.replayCalls) != 1 || fake.replayCalls[0] != 501 {
		t.Errorf("replay calls = %v, want [501]", fake.replayCalls)
	}
	if len(fake.auditEvents) != 1 {
		t.Fatalf("audit events = %d, want 1", len(fake.auditEvents))
	}
	evt := fake.auditEvents[0]
	if evt.Action != "dlq.retry" {
		t.Errorf("audit action = %q, want dlq.retry", evt.Action)
	}
	if evt.Outcome != "success" {
		t.Errorf("audit outcome = %q, want success", evt.Outcome)
	}
	if evt.Target != "501" {
		t.Errorf("audit target = %q, want 501", evt.Target)
	}
}

// TestDLQRetry_readOnly asserts the mutation refusal path: 405,
// JSON body, no ReplayDeadLetter call, audit row with denied outcome.
func TestDLQRetry_readOnly(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{
		sampleDeadLetter(601, "panic"),
	}
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/dlq/601/retry", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "console_read_only") {
		t.Errorf("expected console_read_only error code; got %q", body)
	}
	if !strings.Contains(body, "CONSOLE_READ_ONLY") {
		t.Errorf("expected env var hint in 405 body")
	}
	if len(fake.replayCalls) != 0 {
		t.Errorf("replay calls = %v, want none in read-only mode",
			fake.replayCalls)
	}
	if len(fake.auditEvents) != 1 {
		t.Fatalf("audit events = %d, want 1 denied attempt",
			len(fake.auditEvents))
	}
	if fake.auditEvents[0].Outcome != "denied" {
		t.Errorf("denied attempt outcome = %q, want denied",
			fake.auditEvents[0].Outcome)
	}
}

// TestDLQRetry_replayFailure asserts the error path: replay returns
// error, response is 500, audit row outcome=failed, no discard.
func TestDLQRetry_replayFailure(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{
		sampleDeadLetter(701, "panic"),
	}
	fake.replayErr = errors.New("simulated transport failure")
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/dlq/701/retry", nil))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	if len(fake.replayCalls) != 1 {
		t.Errorf("replay calls = %d, want 1", len(fake.replayCalls))
	}
	if len(fake.discardCalls) != 0 {
		t.Errorf("discard calls = %d, want 0 on replay failure",
			len(fake.discardCalls))
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != "failed" {
		t.Errorf("expected one failed audit event, got %+v",
			fake.auditEvents)
	}
}

// TestDLQDiscard_mutating removes the entry and emits an audit.
func TestDLQDiscard_mutating(t *testing.T) {
	fake := newFakeDS()
	fake.deadLetters = []api.DeadLetterView{
		sampleDeadLetter(801, "panic"),
	}
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/dlq/801/discard", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if len(fake.discardCalls) != 1 {
		t.Errorf("discard calls = %d, want 1", len(fake.discardCalls))
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Action != "dlq.discard" {
		t.Errorf("expected one dlq.discard audit event; got %+v",
			fake.auditEvents)
	}
}

// TestDLQDiscard_readOnly mirrors the retry refusal path.
func TestDLQDiscard_readOnly(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/dlq/1/discard", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	if len(fake.discardCalls) != 0 {
		t.Errorf("discard calls = %d, want 0", len(fake.discardCalls))
	}
}

// TestAuditLogView_rendersRecentEvents seeds events and asserts they
// appear with actor/action/outcome.
func TestAuditLogView_rendersRecentEvents(t *testing.T) {
	fake := newFakeDS()
	fake.auditEvents = []AuditEvent{
		{
			Time:  time.Now().UTC().Add(-time.Minute),
			Actor: "alice", Action: "dlq.retry", Target: "501",
			Outcome: "success",
		},
		{
			Time:  time.Now().UTC().Add(-2 * time.Minute),
			Actor: "bob", Action: "dlq.discard", Target: "502",
			Outcome: "denied",
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/ops/audit", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		"Audit log",
		"alice", "bob",
		"dlq.retry", "dlq.discard",
		"501", "502",
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("missing %q in audit log view", sub)
		}
	}
}

// TestAuditLogView_actorFilter narrows by actor.
func TestAuditLogView_actorFilter(t *testing.T) {
	fake := newFakeDS()
	fake.auditEvents = []AuditEvent{
		{Time: time.Now(), Actor: "alice", Action: "dlq.retry",
			Target: "1", Outcome: "success"},
		{Time: time.Now(), Actor: "bob", Action: "dlq.retry",
			Target: "2", Outcome: "success"},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/ops/audit?actor=alice", nil))
	body := rr.Body.String()
	if !strings.Contains(body, "alice") {
		t.Errorf("expected alice in filtered list")
	}
	if strings.Contains(body, ">bob<") {
		t.Errorf("did not expect bob in actor=alice filter")
	}
}

// TestRunIDLookup_emptyRedirects sends an empty input back to the
// runs list (the noop path).
func TestRunIDLookup_emptyRedirects(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/runs/lookup?id=", nil))
	if rr.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", rr.Code)
	}
	if loc := rr.Header().Get("Location"); loc != "/console/runs" {
		t.Errorf("Location = %q, want /console/runs", loc)
	}
}

// TestRunIDLookup_exactMatch redirects to the run detail page.
func TestRunIDLookup_exactMatch(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/runs/lookup?id=abc123", nil))
	if rr.Code != http.StatusFound {
		t.Fatalf("status = %d, want 302", rr.Code)
	}
	if loc := rr.Header().Get("Location"); loc != "/console/runs/abc123" {
		t.Errorf("Location = %q, want /console/runs/abc123", loc)
	}
}

// TestNotFound_wrapsLayout asserts the layout chrome renders + a
// useful 404 message + X-Robots-Tag noindex.
func TestNotFound_wrapsLayout(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/garbage", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "<html") {
		t.Errorf("expected HTML markup in 404 (layout chrome)")
	}
	if !strings.Contains(body, "Page not found") {
		t.Errorf("expected 'Page not found' message")
	}
	if !strings.Contains(body, "Return to dashboard") {
		t.Errorf("expected back link in 404")
	}
	if got := rr.Header().Get("X-Robots-Tag"); got != "noindex" {
		t.Errorf("X-Robots-Tag = %q, want noindex", got)
	}
	if got := rr.Header().Get("Content-Type"); !strings.HasPrefix(got,
		"text/html") {
		t.Errorf("Content-Type = %q, want text/html prefix", got)
	}
}

// TestReadOnlyMiddleware_allowsGET ensures read mode preserves reads.
func TestReadOnlyMiddleware_allowsGET(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, true)
	for _, path := range []string{
		"/console/", "/console/workflows", "/console/runs",
		"/console/triggers", "/console/dlq", "/console/ops/audit",
	} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusOK && rr.Code != http.StatusFound {
			t.Errorf("GET %s in read-only mode: status = %d, want 200/302",
				path, rr.Code)
		}
	}
}

// TestReadOnlyMiddleware_blocksMutations ensures every POST under
// /console/ returns 405 + the canonical body when active.
func TestReadOnlyMiddleware_blocksMutations(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, true)
	postPaths := []string{
		"/console/dlq/1/retry", "/console/dlq/1/discard",
	}
	for _, path := range postPaths {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, path, nil))
		if rr.Code != http.StatusMethodNotAllowed {
			t.Errorf("POST %s in read-only mode: status = %d, want 405",
				path, rr.Code)
		}
		body := rr.Body.String()
		if !strings.Contains(body, "console_read_only") {
			t.Errorf("POST %s: expected console_read_only error in body",
				path)
		}
	}
}

// TestReadOnlyBanner_appearsInLayout flips ReadOnly on and asserts
// the banner is present on a generic page render.
func TestReadOnlyBanner_appearsInLayout(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "console-readonly-banner") {
		t.Errorf("expected read-only banner in layout")
	}
	if !strings.Contains(body, "CONSOLE_READ_ONLY") {
		t.Errorf("expected env var hint in banner")
	}
}

// TestReadOnlyFromEnv accepts the documented truthy strings.
func TestReadOnlyFromEnv(t *testing.T) {
	cases := map[string]bool{
		"":      false,
		"false": false,
		"0":     false,
		"no":    false,
		"true":  true,
		"TRUE":  true,
		"1":     true,
		"yes":   true,
		"on":    true,
	}
	for in, want := range cases {
		if got := ReadOnlyFromEnv(in); got != want {
			t.Errorf("ReadOnlyFromEnv(%q) = %v, want %v", in, got, want)
		}
	}
}

// TestAuditEmitter_nilKVWarnsAndContinues asserts the nil-KV path:
// no panic, no error, slog.Warn fires. Methodology: capture the logger
// output and look for the warning substring.
func TestAuditEmitter_nilKVWarnsAndContinues(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&buf, nil))
	err := emitAuditEventInner(
		context.Background(), nil, logger,
		AuditEvent{Action: "test.action", Target: "x"},
	)
	if err != nil {
		t.Fatalf("emit on nil KV returned err = %v, want nil", err)
	}
	if !strings.Contains(buf.String(),
		"audit bucket not configured") {
		t.Errorf("expected warning, got %q", buf.String())
	}
}

// TestListAuditEvents_nilKVEmpty returns empty + no error.
func TestListAuditEvents_nilKVEmpty(t *testing.T) {
	out, err := listAuditEventsInner(context.Background(), nil, 10)
	if err != nil {
		t.Fatalf("list on nil KV err = %v, want nil", err)
	}
	if len(out) != 0 {
		t.Errorf("nil KV list = %d events, want 0", len(out))
	}
}

// TestAuditKeyFor_chronologicalOrder confirms keys at later times
// sort lexicographically after earlier times — required so the
// reader walks oldest→newest.
func TestAuditKeyFor_chronologicalOrder(t *testing.T) {
	t0 := time.Date(2026, 5, 14, 12, 0, 0, 0, time.UTC)
	t1 := t0.Add(time.Second)
	k0, err := auditKeyFor(t0)
	if err != nil {
		t.Fatalf("keyFor t0: %v", err)
	}
	k1, err := auditKeyFor(t1)
	if err != nil {
		t.Fatalf("keyFor t1: %v", err)
	}
	if k0 >= k1 {
		t.Errorf("keys not in chronological order: %q vs %q", k0, k1)
	}
}

// TestTriggerToggle_disablesEnabled flips an enabled trigger off,
// asserts the service was called with enabled=false, audit row has
// action=trigger.disable outcome=success, response carries the new
// state for the client to render the pill.
func TestTriggerToggle_disablesEnabled(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-A", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/triggers/cron-A/toggle", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.triggerSetCalls) != 1 ||
		fake.triggerSetCalls[0].ID != "cron-A" ||
		fake.triggerSetCalls[0].Enabled != false {
		t.Errorf("toggle calls = %+v, want one disable-cron-A",
			fake.triggerSetCalls)
	}
	if len(fake.auditEvents) != 1 {
		t.Fatalf("audit events = %d, want 1", len(fake.auditEvents))
	}
	evt := fake.auditEvents[0]
	if evt.Action != string(ActionTriggerDisable) {
		t.Errorf("action = %q, want %q", evt.Action, ActionTriggerDisable)
	}
	if evt.Outcome != string(OutcomeSuccess) {
		t.Errorf("outcome = %q, want success", evt.Outcome)
	}
	if evt.Target != "cron-A" {
		t.Errorf("target = %q, want cron-A", evt.Target)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		`"action":"toggle"`,
		`"enabled":false`,
		`"state":"disabled"`,
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("missing %q in toggle response: %s", sub, body)
		}
	}
}

// TestTriggerToggle_enablesDisabled flips a disabled trigger on.
func TestTriggerToggle_enablesDisabled(t *testing.T) {
	fake := newFakeDS()
	td := sampleTrigger("cron-B", "alpha", "cron")
	td.Enabled = false
	fake.triggers = []trigger.TriggerDef{td}
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/triggers/cron-B/toggle", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !fake.triggerSetCalls[0].Enabled {
		t.Errorf("expected enabled=true; got %+v", fake.triggerSetCalls)
	}
	if fake.auditEvents[0].Action != string(ActionTriggerEnable) {
		t.Errorf("action = %q, want trigger.enable",
			fake.auditEvents[0].Action)
	}
}

// TestTriggerToggle_readOnly returns 405 and records denied audit.
func TestTriggerToggle_readOnly(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-C", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/triggers/cron-C/toggle", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	if len(fake.triggerSetCalls) != 0 {
		t.Errorf("expected no toggle calls in read-only mode")
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != string(OutcomeDenied) {
		t.Errorf("expected one denied audit event; got %+v",
			fake.auditEvents)
	}
}

// TestTriggerToggle_notFound returns 404 when the trigger id is
// unknown. The handler does the lookup itself (to know current state)
// so the 404 path is server-side.
func TestTriggerToggle_notFound(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/triggers/no-such/toggle", nil))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	if len(fake.triggerSetCalls) != 0 {
		t.Errorf("expected no toggle calls for unknown trigger")
	}
}

// TestTriggerToggle_methodCheck rejects GET on the toggle endpoint.
func TestTriggerToggle_methodCheck(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-D", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/cron-D/toggle", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET on toggle: status = %d, want 405", rr.Code)
	}
}

// TestTriggerToggle_serviceFailure records a failed audit.
func TestTriggerToggle_serviceFailure(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-E", "alpha", "cron"),
	}
	fake.triggerSetErr = errors.New("simulated KV failure")
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
		"/console/triggers/cron-E/toggle", nil))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != string(OutcomeFailed) {
		t.Errorf("expected one failed audit event; got %+v",
			fake.auditEvents)
	}
}

// TestTriggerDetail_rendersFireHistory feeds the fake recent firings
// and asserts the activity table renders them with the run link and
// outcome badge.
func TestTriggerDetail_rendersFireHistory(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-fire", "alpha", "cron"),
	}
	fake.triggerFires["cron-fire"] = []TriggerFireRow{
		{
			FiredAt: time.Now().Add(-2 * time.Minute),
			RunID:   "run-abc-12345678",
			Status:  "completed",
		},
		{
			FiredAt:    time.Now().Add(-3 * time.Minute),
			Skipped:    true,
			SkipReason: "trigger disabled",
		},
		{
			FiredAt: time.Now().Add(-90 * time.Second),
			RunID:   "run-def-99999999",
			Status:  "running",
		},
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/cron-fire", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	for _, sub := range []string{
		"Recent activity",
		"run-abc-",
		"run-def-",
		"skipped",
		"running",
	} {
		if !strings.Contains(body, sub) {
			t.Errorf("missing %q in fire history view", sub)
		}
	}
	// Empty-state copy must NOT appear when there are firings.
	if strings.Contains(body, "No firings recorded") {
		t.Errorf("unexpected empty state with seeded firings")
	}
}

// TestTriggerDetail_zeroStateNoFirings asserts the empty-state copy
// stays in place when the fake reports no firings.
func TestTriggerDetail_zeroStateNoFirings(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-empty", "alpha", "cron"),
	}
	h := mountWithFake(t, fake)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/cron-empty", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	if !strings.Contains(rr.Body.String(), "No firings recorded") {
		t.Errorf("expected empty-state copy for trigger with no firings")
	}
}

// TestClassifyDLQReason maps known patterns to fixed classes.
func TestClassifyDLQReason(t *testing.T) {
	cases := map[string]string{
		"task timed out after 30s":     "timeout",
		"panic: nil pointer":           "panic",
		"unrecoverable: bad signature": "unrecoverable",
		"delivery limit reached":       "max-attempts",
		"something we haven't seen":    "other",
	}
	for in, want := range cases {
		got := classifyDLQReason(in)
		if got != want {
			t.Errorf("classify(%q) = %q, want %q", in, got, want)
		}
	}
}

// Silence unused-import linter when nothing exercises io in this file.
var _ = io.Discard
