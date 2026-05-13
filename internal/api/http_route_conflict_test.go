// api/http_route_conflict_test.go
//
// Methodology: integration tests with embedded NATS. Per ADR-013 PR 3,
// registering a second HTTP trigger that collides on (method, path)
// with an already-registered HTTP trigger must fail at registration
// time so operators see the conflict instead of silently losing the
// new route at runtime. Each test starts its own NATS server.
package api

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
)

func TestServiceCreateTriggerRouteConflictReturns409(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	first := trigger.TriggerDef{
		ID:         "http-first",
		WorkflowID: "wf-a",
		Enabled:    true,
		HTTP: &trigger.HTTPConfig{
			Path:         "/api/orders",
			Method:       http.MethodPost,
			TimeoutMs:    3_000,
			MaxBodyBytes: 1024,
		},
	}
	if err := svc.CreateTrigger(context.Background(), first); err != nil {
		t.Fatalf("first CreateTrigger: %v", err)
	}

	second := first
	second.ID = "http-second"
	if err := svc.CreateTrigger(
		context.Background(), second,
	); err == nil {
		t.Fatal("second CreateTrigger: expected route_conflict error")
	} else {
		// Error must be a typed RouteConflictError so transport
		// layers can map it to 409.
		var rce *trigger.RouteConflictError
		if !errors.As(err, &rce) {
			t.Fatalf("err = %T %q, want *trigger.RouteConflictError",
				err, err)
		}
		if rce.Method != http.MethodPost {
			t.Fatalf("Method = %q, want POST", rce.Method)
		}
		if rce.Path != "/api/orders" {
			t.Fatalf("Path = %q, want /api/orders", rce.Path)
		}
		if rce.HolderTriggerID != "http-first" {
			t.Fatalf("HolderTriggerID = %q, want http-first",
				rce.HolderTriggerID)
		}
	}
}

func TestServiceCreateTriggerSamePathDifferentMethodAllowed(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	post := trigger.TriggerDef{
		ID:         "http-post",
		WorkflowID: "wf-a",
		Enabled:    true,
		HTTP: &trigger.HTTPConfig{
			Path:         "/api/widgets",
			Method:       http.MethodPost,
			TimeoutMs:    3_000,
			MaxBodyBytes: 1024,
		},
	}
	if err := svc.CreateTrigger(context.Background(), post); err != nil {
		t.Fatalf("POST: %v", err)
	}
	get := post
	get.ID = "http-get"
	get.HTTP = &trigger.HTTPConfig{
		Path:         "/api/widgets",
		Method:       http.MethodGet,
		TimeoutMs:    3_000,
		MaxBodyBytes: 1024,
	}
	if err := svc.CreateTrigger(context.Background(), get); err != nil {
		t.Fatalf("GET on same path different method: %v", err)
	}
}

func TestServiceCreateTriggerSelfReplaceNoConflict(t *testing.T) {
	// Re-registering the same trigger ID with the same route must
	// succeed (idempotent update), not return a conflict. Conflicts
	// are between *different* trigger IDs claiming the same route.
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	svc := NewService(nc)

	def := trigger.TriggerDef{
		ID:         "http-self",
		WorkflowID: "wf-a",
		Enabled:    true,
		HTTP: &trigger.HTTPConfig{
			Path:         "/api/self",
			Method:       http.MethodPost,
			TimeoutMs:    3_000,
			MaxBodyBytes: 1024,
		},
	}
	if err := svc.CreateTrigger(context.Background(), def); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Update — same ID, same route → no conflict.
	def.WorkflowID = "wf-b"
	if err := svc.CreateTrigger(context.Background(), def); err != nil {
		t.Fatalf("self-replace: %v", err)
	}
}

func TestRouteConflictErrorImplementsError(t *testing.T) {
	err := &trigger.RouteConflictError{
		Method:          "POST",
		Path:            "/x",
		HolderTriggerID: "trig-a",
	}
	msg := err.Error()
	if !strings.Contains(msg, "/x") {
		t.Fatalf("Error() = %q, want path", msg)
	}
	if !strings.Contains(msg, "trig-a") {
		t.Fatalf("Error() = %q, want holder id", msg)
	}
}
