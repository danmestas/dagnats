// trigger/trigger_type_def_test.go
// Methodology: KV round-trip for TriggerTypeDef. Real embedded NATS
// server; SetupAll provisions the trigger_types bucket. Asserts that
// json.RawMessage schema fields survive a marshal → KV put → KV get
// → unmarshal cycle without being double-encoded (i.e. the bytes
// inside ConfigSchema are still a JSON object literal, not a
// JSON-encoded string).
package trigger

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

func TestTriggerTypeDef_Roundtrip(t *testing.T) {
	_, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	kv, err := js.KeyValue(ctx, "trigger_types")
	if err != nil {
		t.Fatalf("KeyValue(trigger_types): %v", err)
	}

	// Non-trivial JSON schema. Asserted post-roundtrip to verify
	// that json.RawMessage avoids the double-encoding trap.
	schema := json.RawMessage(
		`{"type":"object","properties":{"path":{"type":"string"}}}`,
	)
	payloadSchema := json.RawMessage(
		`{"type":"object","properties":{"event":{"type":"string"}}}`,
	)
	now := time.Date(2026, 5, 22, 12, 0, 0, 0, time.UTC)
	def := TriggerTypeDef{
		Name:          "fs.watch",
		OwnerWorkerID: "worker-fs-1",
		Description:   "Filesystem watcher trigger",
		ConfigSchema:  schema,
		PayloadSchema: payloadSchema,
		Version:       "1.0.0",
		RegisteredAt:  now,
	}

	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	// Negative: the encoded form must contain the raw schema as a
	// JSON object — NOT a JSON-encoded string. If ConfigSchema were
	// `string`, the encoded form would contain
	// `"config_schema":"{\"type\":...}"` with escaped quotes. Guard
	// against that regression here before we even touch KV.
	if !containsObject(data, `"config_schema":{"type":"object"`) {
		t.Fatalf("config_schema must encode as object, got: %s",
			string(data))
	}

	if _, err := kv.Put(ctx, def.Name, data); err != nil {
		t.Fatalf("Put: %v", err)
	}
	entry, err := kv.Get(ctx, def.Name)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	var got TriggerTypeDef
	if err := json.Unmarshal(entry.Value(), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	// Positive: every field round-trips.
	if got.Name != def.Name {
		t.Fatalf("Name = %q, want %q", got.Name, def.Name)
	}
	if got.OwnerWorkerID != def.OwnerWorkerID {
		t.Fatalf("OwnerWorkerID = %q, want %q",
			got.OwnerWorkerID, def.OwnerWorkerID)
	}
	if got.Description != def.Description {
		t.Fatalf("Description = %q, want %q",
			got.Description, def.Description)
	}
	if got.Version != def.Version {
		t.Fatalf("Version = %q, want %q",
			got.Version, def.Version)
	}
	if !got.RegisteredAt.Equal(def.RegisteredAt) {
		t.Fatalf("RegisteredAt = %v, want %v",
			got.RegisteredAt, def.RegisteredAt)
	}

	// Positive: ConfigSchema deserialises back to a usable JSON
	// object — proves no double-encoding occurred.
	var probe map[string]interface{}
	if err := json.Unmarshal(got.ConfigSchema, &probe); err != nil {
		t.Fatalf(
			"ConfigSchema is not valid JSON post-roundtrip "+
				"(double-encoded?): %v; raw=%s",
			err, string(got.ConfigSchema),
		)
	}
	if probe["type"] != "object" {
		t.Fatalf("ConfigSchema.type = %v, want object",
			probe["type"])
	}

	// Positive: PayloadSchema also round-trips intact.
	var pprobe map[string]interface{}
	if err := json.Unmarshal(got.PayloadSchema, &pprobe); err != nil {
		t.Fatalf("PayloadSchema unmarshal: %v; raw=%s",
			err, string(got.PayloadSchema))
	}
	if pprobe["type"] != "object" {
		t.Fatalf("PayloadSchema.type = %v, want object",
			pprobe["type"])
	}
}

// containsObject reports whether haystack contains needle as a raw
// byte substring. Tiny helper to keep the assertion above readable
// without pulling in `bytes` solely for `bytes.Contains` on a string
// literal.
func containsObject(haystack []byte, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
	for i := 0; i <= len(haystack)-len(needle); i++ {
		if string(haystack[i:i+len(needle)]) == needle {
			return true
		}
	}
	return false
}
