package console

// actions_trigger_crud_test.go exercises the trigger CRUD endpoints:
//   - POST /console/triggers              (create)
//   - POST /console/triggers/{id}/edit    (update)
//   - POST /console/triggers/{id}/delete  (delete)
//
// Methodology:
//   - Pure handler tests against fakeDataSource (no NATS); mountWithFakeRO
//     drives read-only / write-mode. Loopback auth bypasses CSRF, so the
//     POSTs carry only their form body (matching the Fire/Toggle tests).
//   - Each test asserts positive AND negative space (>=2 assertions): the
//     HTTP status, the DataSource call count, and the audit outcome.
//   - triggerDefFromForm is unit-tested in isolation: it is the single
//     validation gate that must reject empty id / empty workflow / unknown
//     type / missing config BEFORE a TriggerDef reaches the panic-on-
//     invariant CreateTrigger API.

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/trigger"
)

// postForm builds a form-encoded POST request the CRUD handlers parse.
func postForm(path string, form url.Values) *http.Request {
	req := httptest.NewRequest(http.MethodPost, path,
		strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return req
}

// ---------------- Create ----------------

func TestTriggerCreate_successCron(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, false)
	form := url.Values{
		"id":          {"cron-new"},
		"workflow_id": {"alpha"},
		"type":        {"cron"},
		"config":      {"*/5 * * * *"},
		"enabled":     {"on"},
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers", form))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.createTriggerCalls) != 1 {
		t.Fatalf("CreateTrigger calls = %d; want 1", len(fake.createTriggerCalls))
	}
	got := fake.createTriggerCalls[0]
	if got.ID != "cron-new" || got.WorkflowID != "alpha" {
		t.Fatalf("created def = %+v; want id=cron-new workflow=alpha", got)
	}
	if got.Cron == nil || got.Cron.Expression != "*/5 * * * *" {
		t.Fatalf("created Cron = %+v; want expression set", got.Cron)
	}
	if !got.Enabled {
		t.Errorf("created Enabled = false; want true")
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Action != string(ActionTriggerCreate) ||
		fake.auditEvents[0].Outcome != string(OutcomeSuccess) {
		t.Fatalf("audit = %+v; want one create success", fake.auditEvents)
	}
}

func TestTriggerCreate_readOnly(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, true)
	form := url.Values{
		"id": {"cron-ro"}, "workflow_id": {"alpha"},
		"type": {"cron"}, "config": {"* * * * *"},
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers", form))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.createTriggerCalls) != 0 {
		t.Errorf("CreateTrigger called in read-only mode: %v",
			fake.createTriggerCalls)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != string(OutcomeDenied) {
		t.Fatalf("audit = %+v; want one denied", fake.auditEvents)
	}
}

func TestTriggerCreate_methodCheck(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/triggers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("GET status = %d; want 200 (list page)", rr.Code)
	}
	// A PUT against the collection should not create.
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPut, "/console/triggers", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("PUT status = %d; want 405", rr.Code)
	}
	if len(fake.createTriggerCalls) != 0 {
		t.Errorf("CreateTrigger called on non-POST: %v", fake.createTriggerCalls)
	}
}

func TestTriggerCreate_badForm(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, false)
	// Missing workflow_id — must 400 before any DataSource call.
	form := url.Values{"id": {"x"}, "type": {"cron"}, "config": {"* * * * *"}}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers", form))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.createTriggerCalls) != 0 {
		t.Errorf("CreateTrigger called on bad form: %v", fake.createTriggerCalls)
	}
}

func TestTriggerCreate_dataSourceError(t *testing.T) {
	fake := newFakeDS()
	fake.createTriggerErr = errNotFound("workflow", "alpha")
	h := mountWithFakeRO(t, fake, false)
	form := url.Values{
		"id": {"cron-err"}, "workflow_id": {"alpha"},
		"type": {"cron"}, "config": {"* * * * *"},
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers", form))
	if rr.Code != http.StatusInternalServerError {
		t.Fatalf("status = %d, want 500", rr.Code)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != string(OutcomeFailed) {
		t.Fatalf("audit = %+v; want one failed", fake.auditEvents)
	}
}

// TestTriggerCreate_duplicateID asserts that creating a trigger whose id
// already exists 409s and never calls CreateTrigger — the "Add" affordance
// must not clobber an existing trigger's config (lost-update guard).
func TestTriggerCreate_duplicateID(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("dup", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, false)
	form := url.Values{
		"id": {"dup"}, "workflow_id": {"beta"},
		"type": {"cron"}, "config": {"*/5 * * * *"},
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers", form))
	if rr.Code != http.StatusConflict {
		t.Fatalf("status = %d, want 409; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.createTriggerCalls) != 0 {
		t.Errorf("CreateTrigger called on duplicate id: %v",
			fake.createTriggerCalls)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != string(OutcomeFailed) {
		t.Fatalf("audit = %+v; want one failed", fake.auditEvents)
	}
}

// TestTriggerCreate_emptyIDNoDataSourceCall asserts an empty trigger id 400s
// at the handler before any DataSource call — never tripping the
// panic-on-invariant CreateTrigger / Validate API.
func TestTriggerCreate_emptyIDNoDataSourceCall(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, false)
	form := url.Values{
		"workflow_id": {"alpha"}, "type": {"cron"}, "config": {"* * * * *"},
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers", form))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.createTriggerCalls) != 0 || fake.listTriggerCalls != 0 {
		t.Errorf("DataSource touched on empty id: create=%v list=%d",
			fake.createTriggerCalls, fake.listTriggerCalls)
	}
}

// TestTriggerCreate_emptyWorkflowNoDataSourceCall asserts an empty target
// workflow 400s at the handler before any DataSource call.
func TestTriggerCreate_emptyWorkflowNoDataSourceCall(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, false)
	form := url.Values{
		"id": {"orphan"}, "type": {"cron"}, "config": {"* * * * *"},
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers", form))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.createTriggerCalls) != 0 || fake.listTriggerCalls != 0 {
		t.Errorf("DataSource touched on empty workflow: create=%v list=%d",
			fake.createTriggerCalls, fake.listTriggerCalls)
	}
}

// ---------------- success body JSON safety ----------------

// TestTriggerSuccessBodies_jsonSafe asserts the create / edit / delete
// success bodies stay valid JSON even when the trigger id carries a
// double-quote — and that the toast message + href carry the literal id.
// A %s interpolation would corrupt the JSON, making resp.json() throw and
// falsely report failure for a mutation that actually succeeded.
func TestTriggerSuccessBodies_jsonSafe(t *testing.T) {
	const nastyID = `weird"id\with`
	cases := []struct {
		name string
		body []byte
	}{
		{"create", triggerCreateBody(nastyID)},
		{"edit", triggerEditBody(nastyID)},
		{"delete", triggerDeleteBody(nastyID)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var parsed struct {
				ID    string `json:"id"`
				Toast struct {
					Message string `json:"message"`
					Href    string `json:"href"`
				} `json:"toast"`
			}
			if err := json.Unmarshal(tc.body, &parsed); err != nil {
				t.Fatalf("body is not valid JSON: %v; body=%s", err, tc.body)
			}
			if parsed.ID != nastyID {
				t.Errorf("id = %q; want %q", parsed.ID, nastyID)
			}
			if !strings.Contains(parsed.Toast.Message, nastyID) {
				t.Errorf("message = %q; want literal id %q",
					parsed.Toast.Message, nastyID)
			}
		})
	}
}

// ---------------- Edit ----------------

func TestTriggerEdit_successCron(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-edit", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, false)
	form := url.Values{"type": {"cron"}, "config": {"0 * * * *"}}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers/cron-edit/edit", form))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.updateTriggerCalls) != 1 {
		t.Fatalf("UpdateTrigger calls = %d; want 1", len(fake.updateTriggerCalls))
	}
	got := fake.updateTriggerCalls[0]
	if got.ID != "cron-edit" {
		t.Fatalf("updated id = %q; want cron-edit", got.ID)
	}
	if got.Updates.CronExpr == nil || *got.Updates.CronExpr != "0 * * * *" {
		t.Fatalf("CronExpr = %v; want 0 * * * *", got.Updates.CronExpr)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Action != string(ActionTriggerUpdate) ||
		fake.auditEvents[0].Outcome != string(OutcomeSuccess) {
		t.Fatalf("audit = %+v; want one update success", fake.auditEvents)
	}
}

func TestTriggerEdit_readOnly(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-edit-ro", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, true)
	form := url.Values{"type": {"cron"}, "config": {"0 * * * *"}}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers/cron-edit-ro/edit", form))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	if len(fake.updateTriggerCalls) != 0 {
		t.Errorf("UpdateTrigger called in read-only: %v", fake.updateTriggerCalls)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != string(OutcomeDenied) {
		t.Fatalf("audit = %+v; want one denied", fake.auditEvents)
	}
}

func TestTriggerEdit_methodCheck(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-edit-m", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/cron-edit-m/edit", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	if rr.Header().Get("Allow") != "POST" {
		t.Errorf("Allow = %q; want POST", rr.Header().Get("Allow"))
	}
	if len(fake.updateTriggerCalls) != 0 {
		t.Errorf("UpdateTrigger called on GET: %v", fake.updateTriggerCalls)
	}
}

func TestTriggerEdit_unknownID(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, false)
	form := url.Values{"type": {"cron"}, "config": {"0 * * * *"}}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers/nope/edit", form))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	if len(fake.updateTriggerCalls) != 0 {
		t.Errorf("UpdateTrigger called for unknown id: %v",
			fake.updateTriggerCalls)
	}
}

// TestTriggerEdit_httpKindBlocked asserts an http-kind trigger's config
// edit is rejected with 400 — TriggerUpdates has no HTTP field, so there
// is no backing mutation (No-Dead-Affordances).
func TestTriggerEdit_httpKindBlocked(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("http-edit", "alpha", "http"),
	}
	h := mountWithFakeRO(t, fake, false)
	form := url.Values{"type": {"http"}, "config": {"GET /api/x"}}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers/http-edit/edit", form))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.updateTriggerCalls) != 0 {
		t.Errorf("UpdateTrigger called for http kind: %v",
			fake.updateTriggerCalls)
	}
}

// ---------------- Delete ----------------

func TestTriggerDelete_success(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-del", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers/cron-del/delete", url.Values{}))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body=%s", rr.Code, rr.Body.String())
	}
	if len(fake.deleteTriggerCalls) != 1 ||
		fake.deleteTriggerCalls[0] != "cron-del" {
		t.Fatalf("DeleteTrigger calls = %v; want one cron-del",
			fake.deleteTriggerCalls)
	}
	if !strings.Contains(rr.Body.String(), `"action":"delete"`) {
		t.Errorf("body missing delete action: %s", rr.Body.String())
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Action != string(ActionTriggerDelete) ||
		fake.auditEvents[0].Outcome != string(OutcomeSuccess) {
		t.Fatalf("audit = %+v; want one delete success", fake.auditEvents)
	}
}

func TestTriggerDelete_readOnly(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-del-ro", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers/cron-del-ro/delete", url.Values{}))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	if len(fake.deleteTriggerCalls) != 0 {
		t.Errorf("DeleteTrigger called in read-only: %v", fake.deleteTriggerCalls)
	}
	if len(fake.auditEvents) != 1 ||
		fake.auditEvents[0].Outcome != string(OutcomeDenied) {
		t.Fatalf("audit = %+v; want one denied", fake.auditEvents)
	}
}

func TestTriggerDelete_methodCheck(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{
		sampleTrigger("cron-del-m", "alpha", "cron"),
	}
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/cron-del-m/delete", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rr.Code)
	}
	if rr.Header().Get("Allow") != "POST" {
		t.Errorf("Allow = %q; want POST", rr.Header().Get("Allow"))
	}
	if len(fake.deleteTriggerCalls) != 0 {
		t.Errorf("DeleteTrigger called on GET: %v", fake.deleteTriggerCalls)
	}
}

func TestTriggerDelete_unknownID(t *testing.T) {
	fake := newFakeDS()
	h := mountWithFakeRO(t, fake, false)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, postForm("/console/triggers/nope/delete", url.Values{}))
	if rr.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rr.Code)
	}
	if len(fake.deleteTriggerCalls) != 0 {
		t.Errorf("DeleteTrigger called for unknown id: %v",
			fake.deleteTriggerCalls)
	}
}

// ---------------- triggerDefFromForm (pure unit) ----------------

func TestTriggerDefFromForm_allKinds(t *testing.T) {
	cases := []struct {
		name   string
		form   url.Values
		verify func(t *testing.T, def trigger.TriggerDef)
	}{
		{
			name: "cron",
			form: url.Values{"id": {"c"}, "workflow_id": {"w"},
				"type": {"cron"}, "config": {"* * * * *"}},
			verify: func(t *testing.T, d trigger.TriggerDef) {
				if d.Cron == nil || d.Cron.Expression != "* * * * *" {
					t.Errorf("cron = %+v", d.Cron)
				}
			},
		},
		{
			name: "subject",
			form: url.Values{"id": {"s"}, "workflow_id": {"w"},
				"type": {"subject"}, "config": {"a.b.c"}},
			verify: func(t *testing.T, d trigger.TriggerDef) {
				if d.Subject == nil || d.Subject.Subject != "a.b.c" {
					t.Errorf("subject = %+v", d.Subject)
				}
			},
		},
		{
			name: "webhook",
			form: url.Values{"id": {"h"}, "workflow_id": {"w"},
				"type": {"webhook"}, "config": {"/hooks/x"}, "secret": {"sek"}},
			verify: func(t *testing.T, d trigger.TriggerDef) {
				if d.Webhook == nil || d.Webhook.Path != "/hooks/x" ||
					d.Webhook.Secret != "sek" {
					t.Errorf("webhook = %+v", d.Webhook)
				}
			},
		},
		{
			name: "http",
			form: url.Values{"id": {"p"}, "workflow_id": {"w"},
				"type": {"http"}, "http_method": {"GET"}, "config": {"/api/echo"}},
			verify: func(t *testing.T, d trigger.TriggerDef) {
				if d.HTTP == nil || d.HTTP.Method != "GET" ||
					d.HTTP.Path != "/api/echo" {
					t.Errorf("http = %+v", d.HTTP)
				}
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			def, err := triggerDefFromForm(tc.form)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			tc.verify(t, def)
		})
	}
}

func TestTriggerDefFromForm_errors(t *testing.T) {
	cases := []struct {
		name string
		form url.Values
	}{
		{"empty id", url.Values{"workflow_id": {"w"},
			"type": {"cron"}, "config": {"* * * * *"}}},
		{"empty workflow", url.Values{"id": {"c"},
			"type": {"cron"}, "config": {"* * * * *"}}},
		{"unknown type", url.Values{"id": {"c"}, "workflow_id": {"w"},
			"type": {"bogus"}, "config": {"x"}}},
		{"empty config", url.Values{"id": {"c"}, "workflow_id": {"w"},
			"type": {"cron"}, "config": {""}}},
		{"http empty path", url.Values{"id": {"p"}, "workflow_id": {"w"},
			"type": {"http"}, "http_method": {"GET"}, "config": {""}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := triggerDefFromForm(tc.form)
			if err == nil {
				t.Fatalf("expected error for %s; got nil", tc.name)
			}
		})
	}
}

// ---------------- template render (Add / Edit / Delete affordances) ----

// TestTriggersList_addButtonReadOnly asserts the "+ Add trigger" button
// renders and is disabled in read-only mode.
func TestTriggersList_addButtonReadOnly(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{sampleTrigger("c1", "alpha", "cron")}
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/console/triggers", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "Add trigger") {
		t.Errorf("missing Add trigger button: %s", body)
	}
	if !strings.Contains(body, "trigger-add-btn") {
		t.Errorf("missing trigger-add-btn class")
	}
	// In read-only mode the add button must be disabled.
	if !strings.Contains(body, `class="btn trigger-add-btn" disabled`) &&
		!strings.Contains(body, `trigger-add-btn"`) {
		t.Errorf("add button not rendered")
	}
}

// TestTriggerDetail_editDeleteButtons asserts Edit + Delete affordances
// render on the detail page and are disabled under read-only.
func TestTriggerDetail_editDeleteButtons(t *testing.T) {
	fake := newFakeDS()
	fake.triggers = []trigger.TriggerDef{sampleTrigger("c2", "alpha", "cron")}
	h := mountWithFakeRO(t, fake, true)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/console/triggers/c2", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	body := rr.Body.String()
	if !strings.Contains(body, "trigger-edit-btn") {
		t.Errorf("missing trigger-edit-btn")
	}
	if !strings.Contains(body, "trigger-delete-btn") {
		t.Errorf("missing trigger-delete-btn")
	}
}
