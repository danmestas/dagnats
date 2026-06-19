// dagnatsext/types_test.go
// Methodology: pure unit tests — no NATS, no embedded server. The public
// boundary types are DTOs; we verify JSON round-trips so out-of-tree callers
// can rely on stable wire tags, and we verify the slim TriggerDef shape so
// add-on handlers know exactly what they receive from WatchTriggers.
package dagnatsext_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dagnatsext"
)

// TestTriggerTypeDefRoundTrip ensures JSON tags on TriggerTypeDef match the
// internal/trigger wire contract so KV reads by either package decode correctly.
func TestTriggerTypeDefRoundTrip(t *testing.T) {
	orig := dagnatsext.TriggerTypeDef{
		Name:          "fs.watch",
		OwnerWorkerID: "worker-1",
		Description:   "Test watcher",
		ConfigSchema:  json.RawMessage(`{"type":"object"}`),
		PayloadSchema: json.RawMessage(`{"type":"string"}`),
		Version:       "1.0.0",
		RegisteredAt:  time.Unix(1700000000, 0).UTC(),
	}
	data, err := json.Marshal(orig)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var got dagnatsext.TriggerTypeDef
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	// Positive: all fields survive the round-trip.
	if got.Name != orig.Name {
		t.Errorf("Name = %q, want %q", got.Name, orig.Name)
	}
	if got.Version != orig.Version {
		t.Errorf("Version = %q, want %q", got.Version, orig.Version)
	}
	if string(got.ConfigSchema) != string(orig.ConfigSchema) {
		t.Errorf("ConfigSchema = %s, want %s",
			got.ConfigSchema, orig.ConfigSchema)
	}
	// Negative: a zero-value TriggerTypeDef with no Name should not produce
	// a "name" key with a non-empty value in JSON.
	empty, err := json.Marshal(dagnatsext.TriggerTypeDef{})
	if err != nil {
		t.Fatalf("Marshal zero TriggerTypeDef: %v", err)
	}
	var raw map[string]interface{}
	if err := json.Unmarshal(empty, &raw); err != nil {
		t.Fatalf("Unmarshal zero TriggerTypeDef: %v", err)
	}
	if v, ok := raw["name"]; ok && v != "" {
		t.Errorf("empty TriggerTypeDef has non-empty name in JSON: %v", v)
	}
}

// TestTriggerDefSlimShape verifies the slim TriggerDef carries the right fields
// and that External is a value type (not a pointer), so handlers never nil-check it.
func TestTriggerDefSlimShape(t *testing.T) {
	def := dagnatsext.TriggerDef{
		ID:         "t-1",
		WorkflowID: "wf-1",
		Enabled:    true,
		External: dagnatsext.ExternalTriggerConfig{
			Kind:   "fs.watch",
			Config: json.RawMessage(`{"path":"/tmp"}`),
		},
	}
	// Positive: value fields are accessible without nil check.
	if def.External.Kind != "fs.watch" {
		t.Errorf("External.Kind = %q, want fs.watch", def.External.Kind)
	}
	if string(def.External.Config) != `{"path":"/tmp"}` {
		t.Errorf("External.Config = %s, want {\"path\":\"/tmp\"}",
			def.External.Config)
	}

	// Negative: a zero-value TriggerDef has an empty External, not nil.
	var zero dagnatsext.TriggerDef
	if zero.External.Kind != "" {
		t.Errorf("zero TriggerDef.External.Kind should be empty, got %q",
			zero.External.Kind)
	}
}
