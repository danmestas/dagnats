package trigger

import (
	"context"
	"net/http"
	"sync"

	"github.com/nats-io/nats.go"
)

// webhookRegistrar owns the WebhookHandler table keyed by path.
// ADR-016. Map shared by reference with TriggerService for the same
// reason as subjectRegistrar (see registrar_subject.go).
type webhookRegistrar struct {
	nc       *nats.Conn
	webhooks map[string]*WebhookHandler
	mu       *sync.RWMutex // shared with TriggerService
}

func newWebhookRegistrar(
	nc *nats.Conn,
	webhooks map[string]*WebhookHandler,
	mu *sync.RWMutex,
) *webhookRegistrar {
	if nc == nil {
		panic("newWebhookRegistrar: nc must not be nil")
	}
	if webhooks == nil {
		panic("newWebhookRegistrar: webhooks map must not be nil")
	}
	if mu == nil {
		panic("newWebhookRegistrar: mu must not be nil")
	}
	return &webhookRegistrar{nc: nc, webhooks: webhooks, mu: mu}
}

// Activate installs the webhook handler at def.Webhook.Path.
// Idempotent: if an entry with the same def.ID already exists at
// any path, no-op.
func (r *webhookRegistrar) Activate(_ context.Context, def TriggerDef) error {
	if def.Webhook == nil {
		panic("webhookRegistrar.Activate: def.Webhook must not be nil")
	}
	for _, h := range r.webhooks {
		if h.def.ID == def.ID {
			return nil
		}
	}
	handler := NewWebhookHandler(r.nc, def)
	if def.Webhook.Path != "" {
		r.webhooks[def.Webhook.Path] = handler
	}
	return nil
}

// Deactivate removes the entry whose def.ID matches. Idempotent.
func (r *webhookRegistrar) Deactivate(_ context.Context, def TriggerDef) error {
	if def.ID == "" {
		panic("webhookRegistrar.Deactivate: def.ID must not be empty")
	}
	for path, h := range r.webhooks {
		if h.def.ID == def.ID {
			delete(r.webhooks, path)
			return nil
		}
	}
	return nil
}

// ValidateConfig delegates to the shared webhook validator.
func (r *webhookRegistrar) ValidateConfig(def TriggerDef) error {
	return validateWebhookConfig(def.ID, def.Webhook)
}

// Handler returns the HTTP handler that routes incoming requests by
// URL path to the registered webhook. Closes over the webhooks map
// reference; the service's RWMutex guards concurrent access.
func (r *webhookRegistrar) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		r.mu.RLock()
		handler, ok := r.webhooks[req.URL.Path]
		r.mu.RUnlock()
		if !ok {
			http.NotFound(w, req)
			return
		}
		handler.ServeHTTP(w, req)
	})
}
