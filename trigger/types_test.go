package trigger

// Methodology: unit tests for trigger types. No NATS dependency.
// Each test verifies JSON round-trip and envelope construction.

import (
	"encoding/json"
	"testing"
	"time"
)

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
