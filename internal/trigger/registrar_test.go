package trigger

// Methodology: ADR-016 acceptance tests. Verify the per-kind
// registrars are idempotent on Activate/Deactivate and that the
// TriggerService reader surface (WebhookHandler / TriggerCount /
// HTTPRouter) keeps its observable behavior after the refactor.
//
// Each test starts an embedded NATS server and a fresh service so
// the registrars run against real NATS resources (not mocks).
// Bounded waits keep CI well-behaved.

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestRegistrarsAreIdempotent(t *testing.T) {
	t.Run("cron", func(t *testing.T) {
		_, nc := natsutil.StartTestServer(t)
		if err := natsutil.SetupAll(nc,
			natsutil.WithKVBuckets(
				natsutil.KVConfig{Bucket: "triggers"},
				natsutil.KVConfig{Bucket: "trigger_state"},
			),
		); err != nil {
			t.Fatalf("setup: %v", err)
		}
		svc, err := NewTriggerService(nc)
		if err != nil {
			t.Fatalf("NewTriggerService: %v", err)
		}
		defer svc.Stop()
		reg := svc.registrars[kindCron]
		def := TriggerDef{
			ID: "cron-idem", WorkflowID: "wf", Enabled: true,
			Cron: &CronConfig{
				Expression: "0 9 * * *", Timezone: "UTC",
			},
		}
		ctx := context.Background()
		if err := reg.Activate(ctx, def); err != nil {
			t.Fatalf("first Activate: %v", err)
		}
		if err := reg.Activate(ctx, def); err != nil {
			t.Fatalf("second Activate (idempotent): %v", err)
		}
		// Positive: scheduler holds exactly one entry for this id.
		if svc.scheduler.Count() != 1 {
			t.Fatalf("scheduler count = %d, want 1",
				svc.scheduler.Count())
		}
		if err := reg.Deactivate(ctx, def); err != nil {
			t.Fatalf("first Deactivate: %v", err)
		}
		// Negative: second Deactivate is a no-op.
		if err := reg.Deactivate(ctx, def); err != nil {
			t.Fatalf("second Deactivate (idempotent): %v", err)
		}
		if svc.scheduler.Count() != 0 {
			t.Fatalf("count after deactivate = %d, want 0",
				svc.scheduler.Count())
		}
	})

	t.Run("subject", func(t *testing.T) {
		_, nc := natsutil.StartTestServer(t)
		if err := natsutil.SetupAll(nc,
			natsutil.WithKVBuckets(
				natsutil.KVConfig{Bucket: "triggers"},
				natsutil.KVConfig{Bucket: "trigger_state"},
			),
		); err != nil {
			t.Fatalf("setup: %v", err)
		}
		svc, err := NewTriggerService(nc)
		if err != nil {
			t.Fatalf("NewTriggerService: %v", err)
		}
		defer svc.Stop()
		reg := svc.registrars[kindSubject]
		def := TriggerDef{
			ID: "subj-idem", WorkflowID: "wf", Enabled: true,
			Subject: &SubjectConfig{Subject: "events.idem"},
		}
		ctx := context.Background()
		if err := reg.Activate(ctx, def); err != nil {
			t.Fatalf("first Activate: %v", err)
		}
		first := svc.subjects[def.ID]
		if first == nil {
			t.Fatal("subject not installed after Activate")
		}
		if err := reg.Activate(ctx, def); err != nil {
			t.Fatalf("second Activate (idempotent): %v", err)
		}
		// Positive: same pointer — idempotent, no re-subscribe.
		if svc.subjects[def.ID] != first {
			t.Fatal("second Activate replaced the SubjectTrigger; " +
				"idempotency contract violated")
		}
		if err := reg.Deactivate(ctx, def); err != nil {
			t.Fatalf("first Deactivate: %v", err)
		}
		// Negative: second Deactivate is a no-op.
		if err := reg.Deactivate(ctx, def); err != nil {
			t.Fatalf("second Deactivate (idempotent): %v", err)
		}
		if _, ok := svc.subjects[def.ID]; ok {
			t.Fatal("subject still present after deactivate")
		}
	})

	t.Run("webhook", func(t *testing.T) {
		_, nc := natsutil.StartTestServer(t)
		if err := natsutil.SetupAll(nc,
			natsutil.WithKVBuckets(
				natsutil.KVConfig{Bucket: "triggers"},
				natsutil.KVConfig{Bucket: "trigger_state"},
			),
		); err != nil {
			t.Fatalf("setup: %v", err)
		}
		svc, err := NewTriggerService(nc)
		if err != nil {
			t.Fatalf("NewTriggerService: %v", err)
		}
		defer svc.Stop()
		reg := svc.registrars[kindWebhook]
		def := TriggerDef{
			ID: "wh-idem", WorkflowID: "wf", Enabled: true,
			Webhook: &WebhookConfig{Path: "/hooks/idem"},
		}
		ctx := context.Background()
		if err := reg.Activate(ctx, def); err != nil {
			t.Fatalf("first Activate: %v", err)
		}
		first := svc.webhooks[def.Webhook.Path]
		if first == nil {
			t.Fatal("webhook not installed after Activate")
		}
		if err := reg.Activate(ctx, def); err != nil {
			t.Fatalf("second Activate (idempotent): %v", err)
		}
		// Positive: pointer identity preserved.
		if svc.webhooks[def.Webhook.Path] != first {
			t.Fatal("second Activate replaced the WebhookHandler")
		}
		if err := reg.Deactivate(ctx, def); err != nil {
			t.Fatalf("first Deactivate: %v", err)
		}
		if err := reg.Deactivate(ctx, def); err != nil {
			t.Fatalf("second Deactivate (idempotent): %v", err)
		}
		if _, ok := svc.webhooks[def.Webhook.Path]; ok {
			t.Fatal("webhook still present after deactivate")
		}
	})

	t.Run("http", func(t *testing.T) {
		_, nc := natsutil.StartTestServer(t)
		if err := natsutil.SetupAll(nc,
			natsutil.WithKVBuckets(
				natsutil.KVConfig{Bucket: "triggers"},
				natsutil.KVConfig{Bucket: "trigger_state"},
				natsutil.KVConfig{Bucket: "http_idempotency"},
			),
		); err != nil {
			t.Fatalf("setup: %v", err)
		}
		svc, err := NewTriggerService(nc)
		if err != nil {
			t.Fatalf("NewTriggerService: %v", err)
		}
		defer svc.Stop()
		reg := svc.registrars[kindHTTP]
		def := TriggerDef{
			ID: "http-idem", WorkflowID: "wf", Enabled: true,
			HTTP: &HTTPConfig{
				Method:       "POST",
				Path:         "/api/idem",
				TimeoutMs:    1000,
				MaxBodyBytes: 1024,
			},
		}
		ctx := context.Background()
		if err := reg.Activate(ctx, def); err != nil {
			t.Fatalf("first Activate: %v", err)
		}
		key := httpRouteKey(def.HTTP.Method, def.HTTP.Path)
		first := svc.httpRoutes[key]
		if first == nil {
			t.Fatal("http route not installed after Activate")
		}
		if err := reg.Activate(ctx, def); err != nil {
			t.Fatalf("second Activate (idempotent): %v", err)
		}
		// Positive: pointer identity preserved.
		if svc.httpRoutes[key] != first {
			t.Fatal("second Activate replaced the HTTPHandler")
		}
		if err := reg.Deactivate(ctx, def); err != nil {
			t.Fatalf("first Deactivate: %v", err)
		}
		if err := reg.Deactivate(ctx, def); err != nil {
			t.Fatalf("second Deactivate (idempotent): %v", err)
		}
		if _, ok := svc.httpRoutes[key]; ok {
			t.Fatal("http route still present after deactivate")
		}
	})
}

// TestServiceReaderSurfaceUnchanged confirms the three reader
// accessors keep their observable shape after the refactor:
//
//   - WebhookHandler() returns a non-nil http.Handler that 404s on
//     unknown paths and dispatches to registered webhooks.
//   - HTTPRouter() returns a non-nil http.Handler that 404s on
//     unknown paths and 405s on known-path/wrong-method.
//   - TriggerCount() reflects the sum across every kind.
//
// These are the only externally callable shape claims about the
// reader surface — the refactor is a pass-through layer over the
// registrars and may not regress any of them.
func TestServiceReaderSurfaceUnchanged(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
			natsutil.KVConfig{Bucket: "trigger_state"},
			natsutil.KVConfig{Bucket: "http_idempotency"},
		),
	); err != nil {
		t.Fatalf("setup: %v", err)
	}
	svc, err := NewTriggerService(nc)
	if err != nil {
		t.Fatalf("NewTriggerService: %v", err)
	}
	if err := svc.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer svc.Stop()

	// Positive: handlers are non-nil before any trigger registers.
	if svc.WebhookHandler() == nil {
		t.Fatal("WebhookHandler returned nil")
	}
	if svc.HTTPRouter() == nil {
		t.Fatal("HTTPRouter returned nil")
	}
	// Negative: count is zero with no triggers.
	if c := svc.TriggerCount(); c != 0 {
		t.Fatalf("TriggerCount = %d, want 0", c)
	}

	// Add one of every kind via addTrigger (the path real loads use)
	defs := []TriggerDef{
		{
			ID: "rs-cron", WorkflowID: "wf", Enabled: true,
			Cron: &CronConfig{
				Expression: "0 9 * * *", Timezone: "UTC",
			},
		},
		{
			ID: "rs-subj", WorkflowID: "wf", Enabled: true,
			Subject: &SubjectConfig{Subject: "events.rs"},
		},
		{
			ID: "rs-wh", WorkflowID: "wf", Enabled: true,
			Webhook: &WebhookConfig{Path: "/hooks/rs"},
		},
		{
			ID: "rs-http", WorkflowID: "wf", Enabled: true,
			HTTP: &HTTPConfig{
				Method:       "POST",
				Path:         "/api/rs",
				TimeoutMs:    1000,
				MaxBodyBytes: 1024,
			},
		},
	}
	for _, def := range defs {
		if err := svc.addTrigger(def); err != nil {
			t.Fatalf("addTrigger %q: %v", def.ID, err)
		}
	}

	// Positive: count reflects all four kinds.
	if c := svc.TriggerCount(); c != 4 {
		t.Fatalf("TriggerCount = %d, want 4", c)
	}

	// Negative: webhook 404 on unknown path.
	rec := httptest.NewRecorder()
	req := httptest.NewRequest("POST", "/hooks/missing", nil)
	svc.WebhookHandler().ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Fatalf("webhook unknown path: code = %d, want 404",
			rec.Code)
	}

	// Negative: http router 404 on unknown path.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("POST", "/api/missing", nil)
	svc.HTTPRouter().ServeHTTP(rec, req)
	if rec.Code != 404 {
		t.Fatalf("http unknown path: code = %d, want 404", rec.Code)
	}

	// Negative: http router 405 on known path / wrong method.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest("GET", "/api/rs", nil)
	svc.HTTPRouter().ServeHTTP(rec, req)
	if rec.Code != 405 {
		t.Fatalf("http wrong method: code = %d, want 405", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "method not allowed") {
		t.Fatalf("405 body = %q, expected 'method not allowed'",
			rec.Body.String())
	}
}
