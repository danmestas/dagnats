package trigger

// Methodology: unit tests for trigger types. No NATS dependency.
// Each test verifies JSON round-trip and envelope construction.

import (
	"encoding/json"
	"testing"
	"time"
)

func TestTriggerDefHTTPJSON(t *testing.T) {
	def := TriggerDef{
		ID:         "t-http",
		WorkflowID: "orders-wf",
		Enabled:    true,
		HTTP: &HTTPConfig{
			Path:              "/api/orders",
			Method:            "POST",
			TimeoutMs:         30000,
			MaxBodyBytes:      1024,
			IdempotencyHeader: "Idempotency-Key",
		},
	}

	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got TriggerDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Positive: HTTPConfig round-trips.
	if got.HTTP == nil {
		t.Fatal("HTTP should be non-nil after round-trip")
	}
	if got.HTTP.Path != "/api/orders" {
		t.Fatalf("Path = %q, want /api/orders", got.HTTP.Path)
	}
	// Negative: other one-of variants stay nil.
	if got.Cron != nil || got.Webhook != nil ||
		got.Subject != nil {
		t.Fatalf(
			"only HTTP should be set, got cron=%v webhook=%v subject=%v",
			got.Cron, got.Webhook, got.Subject,
		)
	}
}

func TestTriggerDefCronJSON(t *testing.T) {
	def := TriggerDef{
		ID:         "t1",
		WorkflowID: "deploy-wf",
		Enabled:    true,
		Cron: &CronConfig{
			Expression: "0 9 * * 1-5",
			Timezone:   "America/Denver",
			Backfill:   true,
		},
	}

	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got TriggerDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Positive: fields round-trip
	if got.Cron.Expression != "0 9 * * 1-5" {
		t.Fatalf("Expression = %q, want %q",
			got.Cron.Expression, "0 9 * * 1-5")
	}
	if !got.Cron.Backfill {
		t.Fatalf("Backfill should be true")
	}

	// Positive: Subject and Webhook are nil
	if got.Subject != nil {
		t.Fatalf("Subject should be nil for cron trigger")
	}
	if got.Webhook != nil {
		t.Fatalf("Webhook should be nil for cron trigger")
	}
}

func TestTriggerEnvelopeJSON(t *testing.T) {
	env := TriggerEnvelope{
		Trigger:   "cron",
		Source:    "0 9 * * 1-5",
		Timestamp: time.Date(2026, 3, 31, 9, 0, 0, 0, time.UTC),
		Data:      nil,
	}

	data, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var got TriggerEnvelope
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Positive: trigger type preserved
	if got.Trigger != "cron" {
		t.Fatalf("Trigger = %q, want cron", got.Trigger)
	}

	// Positive: nil data omitted
	if got.Data != nil {
		t.Fatalf("Data should be nil")
	}
}
