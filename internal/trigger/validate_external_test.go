// trigger/validate_external_test.go
// Methodology: tests for ValidateWithKV against TriggerDef.External.
// Pure-error paths (no KV, missing kind, schema-fail) use either nil
// or a real KV depending on what the path requires. The KV-backed
// happy-path and miss tests spin up an embedded NATS server and seed
// the "trigger_types" bucket with a TriggerTypeDef.
package trigger

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/nats-io/nats.go/jetstream"
)

// Each test asserts positive and negative space: the expected error
// is returned and a successful path does not error (or vice versa).

func TestValidateRejectsExternalWithoutKV(t *testing.T) {
	def := TriggerDef{
		ID: "e1", WorkflowID: "wf", Enabled: true,
		External: &ExternalTriggerConfig{
			Kind:   "fs.watch",
			Config: json.RawMessage(`{"path":"/tmp"}`),
		},
	}
	err := Validate(def)

	// Positive: error returned because Validate cannot fetch the schema.
	if err == nil {
		t.Fatalf("expected error for External without KV")
	}
	// Positive: error names the supported entry point.
	if !strings.Contains(err.Error(), "ValidateWithKV") {
		t.Fatalf("error = %q, should mention ValidateWithKV", err)
	}
}

func TestValidateWithKVRejectsUnknownKind(t *testing.T) {
	kv := setupTriggerTypesKV(t)
	def := TriggerDef{
		ID: "e2", WorkflowID: "wf", Enabled: true,
		External: &ExternalTriggerConfig{
			Kind:   "nonexistent",
			Config: json.RawMessage(`{}`),
		},
	}

	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	err := ValidateWithKV(ctx, kv, def)

	// Positive: unknown kind reports clearly.
	if err == nil {
		t.Fatal("expected error for unknown trigger kind")
	}
	if !strings.Contains(err.Error(),
		"no trigger type registered: nonexistent") {
		t.Fatalf("error = %q, should mention unknown kind", err)
	}
}

func TestValidateWithKVRejectsSchemaFailingConfig(t *testing.T) {
	kv := setupTriggerTypesKV(t)
	// Register a kind whose ConfigSchema requires `path` (string).
	registerTriggerType(t, kv, TriggerTypeDef{
		Name:          "fs.watch",
		OwnerWorkerID: "worker-fs-1",
		Description:   "Filesystem watcher",
		ConfigSchema: json.RawMessage(
			`{"type":"object","required":["path"],` +
				`"properties":{"path":{"type":"string"}}}`,
		),
		Version:      "1.0.0",
		RegisteredAt: time.Now().UTC(),
	})

	def := TriggerDef{
		ID: "e3", WorkflowID: "wf", Enabled: true,
		External: &ExternalTriggerConfig{
			Kind:   "fs.watch",
			Config: json.RawMessage(`{}`), // missing "path"
		},
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	err := ValidateWithKV(ctx, kv, def)

	// Positive: schema violation surfaces.
	if err == nil {
		t.Fatal("expected schema validation error")
	}
	// Positive: error mentions the missing field or schema path so the
	// operator can locate the violation.
	msg := err.Error()
	if !strings.Contains(msg, "path") &&
		!strings.Contains(msg, "required") {
		t.Fatalf("error = %q, should mention schema path/field", err)
	}
}

func TestValidateWithKVAcceptsValidExternal(t *testing.T) {
	kv := setupTriggerTypesKV(t)
	registerTriggerType(t, kv, TriggerTypeDef{
		Name:          "fs.watch",
		OwnerWorkerID: "worker-fs-1",
		Description:   "Filesystem watcher",
		ConfigSchema: json.RawMessage(
			`{"type":"object","required":["path"],` +
				`"properties":{"path":{"type":"string"}}}`,
		),
		Version:      "1.0.0",
		RegisteredAt: time.Now().UTC(),
	})

	def := TriggerDef{
		ID: "e4", WorkflowID: "wf", Enabled: true,
		External: &ExternalTriggerConfig{
			Kind:   "fs.watch",
			Config: json.RawMessage(`{"path":"/tmp"}`),
		},
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	if err := ValidateWithKV(ctx, kv, def); err != nil {
		t.Fatalf("valid External rejected: %v", err)
	}
}

func TestValidateWithKVRejectsExternalPlusCron(t *testing.T) {
	kv := setupTriggerTypesKV(t)
	def := TriggerDef{
		ID: "e5", WorkflowID: "wf", Enabled: true,
		Cron: &CronConfig{Expression: "* * * * *"},
		External: &ExternalTriggerConfig{
			Kind:   "fs.watch",
			Config: json.RawMessage(`{"path":"/tmp"}`),
		},
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	err := ValidateWithKV(ctx, kv, def)

	// Positive: exactly-one-of rejects two-set case.
	if err == nil {
		t.Fatal("expected error for External + Cron")
	}
	if !strings.Contains(err.Error(), "exactly one") {
		t.Fatalf("error = %q, should mention exactly one", err)
	}
}

func TestValidateWithKVRejectsEmptyKind(t *testing.T) {
	kv := setupTriggerTypesKV(t)
	def := TriggerDef{
		ID: "e6", WorkflowID: "wf", Enabled: true,
		External: &ExternalTriggerConfig{
			Kind:   "",
			Config: json.RawMessage(`{}`),
		},
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	err := ValidateWithKV(ctx, kv, def)

	// Positive: empty kind is a structural error before any KV lookup.
	if err == nil {
		t.Fatal("expected error for empty External.Kind")
	}
	if !strings.Contains(err.Error(), "kind") {
		t.Fatalf("error = %q, should mention kind", err)
	}
}

func TestValidateWithKVDelegatesToValidateForNonExternal(t *testing.T) {
	kv := setupTriggerTypesKV(t)
	def := TriggerDef{
		ID: "e7", WorkflowID: "wf", Enabled: true,
		Cron: &CronConfig{Expression: "* * * * *"},
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	// Positive: a Cron trigger validated via ValidateWithKV behaves
	// identically to Validate — no spurious KV lookup, no error.
	if err := ValidateWithKV(ctx, kv, def); err != nil {
		t.Fatalf("valid cron rejected via ValidateWithKV: %v", err)
	}
}

// setupTriggerTypesKV provisions an embedded NATS server, runs
// SetupAll to create the trigger_types bucket, and returns the KV
// handle. Bounded 5s context inside SetupAll.
func setupTriggerTypesKV(t *testing.T) jetstream.KeyValue {
	t.Helper()
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
	return kv
}

func registerTriggerType(
	t *testing.T, kv jetstream.KeyValue, def TriggerTypeDef,
) {
	t.Helper()
	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("marshal TriggerTypeDef: %v", err)
	}
	ctx, cancel := context.WithTimeout(
		context.Background(), 5*time.Second,
	)
	defer cancel()
	if _, err := kv.Put(ctx, def.Name, data); err != nil {
		t.Fatalf("kv.Put(%q): %v", def.Name, err)
	}
}
