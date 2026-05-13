// cli/trigger_test.go
// Tests for trigger CLI commands: create, list, delete.
// Methodology: integration tests with embedded NATS to verify KV operations.
package cli

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
)

func TestTriggerCreateStoresCronInKV(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	// Set NATS_URL env var for the CLI to use
	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	// Run trigger create command
	runTriggerCreateCmd([]string{
		"test-workflow",
		"--cron=* * * * *",
		"--tz=America/New_York",
		"--backfill",
	})

	// Positive: exactly one trigger should be stored in KV
	keys, _ := trigKV.Keys()
	if len(keys) != 1 {
		t.Fatalf("expected 1 trigger, got %d", len(keys))
	}

	entry, err := trigKV.Get(keys[0])
	if err != nil {
		t.Fatalf("trigger not found in KV: %v", err)
	}

	var def trigger.TriggerDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		t.Fatalf("unmarshal trigger failed: %v", err)
	}

	if def.WorkflowID != "test-workflow" {
		t.Fatalf(
			"expected WorkflowID test-workflow, got %s", def.WorkflowID,
		)
	}
	if def.Cron == nil {
		t.Fatal("Cron config should not be nil")
	}
	if def.Cron.Expression != "* * * * *" {
		t.Fatalf(
			"expected cron expression '* * * * *', got %s",
			def.Cron.Expression,
		)
	}
	if def.Cron.Timezone != "America/New_York" {
		t.Fatalf(
			"expected timezone America/New_York, got %s",
			def.Cron.Timezone,
		)
	}
	if !def.Cron.Backfill {
		t.Fatal("Backfill should be true")
	}

	// Negative: ID should start with "trig-"
	if len(def.ID) < 5 || def.ID[:5] != "trig-" {
		t.Fatalf("expected ID to start with 'trig-', got %s", def.ID)
	}
}

func TestTriggerCreateSubjectStoresInKV(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	// Run trigger create command with subject
	runTriggerCreateCmd([]string{
		"test-workflow",
		"--subject=events.github.push",
	})

	// Positive: exactly one subject trigger should be stored
	keys, _ := trigKV.Keys()
	if len(keys) != 1 {
		t.Fatalf("expected 1 trigger, got %d", len(keys))
	}

	entry, err := trigKV.Get(keys[0])
	if err != nil {
		t.Fatalf("trigger not found in KV: %v", err)
	}

	var def trigger.TriggerDef
	if err := json.Unmarshal(entry.Value(), &def); err != nil {
		t.Fatalf("unmarshal trigger failed: %v", err)
	}

	if def.Subject == nil {
		t.Fatal("Subject config should not be nil")
	}
	if def.Subject.Subject != "events.github.push" {
		t.Fatalf(
			"expected subject events.github.push, got %s",
			def.Subject.Subject,
		)
	}

	// Negative: Cron and Webhook should be nil
	if def.Cron != nil {
		t.Fatal("Cron should be nil for subject trigger")
	}
	if def.Webhook != nil {
		t.Fatal("Webhook should be nil for subject trigger")
	}
}

func TestTriggerListPrintsTriggers(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	// Store two triggers
	def1 := trigger.TriggerDef{
		ID:         "trig-1",
		WorkflowID: "wf-1",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "0 0 * * *",
			Timezone:   "UTC",
		},
	}
	data1, _ := json.Marshal(def1)
	trigKV.Put("trig-1", data1)

	def2 := trigger.TriggerDef{
		ID:         "trig-2",
		WorkflowID: "wf-2",
		Enabled:    false,
		Subject: &trigger.SubjectConfig{
			Subject: "events.test",
		},
	}
	data2, _ := json.Marshal(def2)
	trigKV.Put("trig-2", data2)

	// Run trigger list command (output goes to stdout, just verify no panic)
	runTriggerListCmd([]string{})

	// Positive: both triggers should exist in KV
	keys, _ := trigKV.Keys()
	if len(keys) != 2 {
		t.Fatalf("expected 2 triggers, got %d", len(keys))
	}

	// Negative: list should not modify KV
	time.Sleep(100 * time.Millisecond)
	keysAfter, _ := trigKV.Keys()
	if len(keysAfter) != 2 {
		t.Fatal("list command should not modify KV")
	}
}

func TestTriggerEnableDisableFlipsKV(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	// Store a trigger with Enabled: true
	def := trigger.TriggerDef{
		ID:         "trig-test",
		WorkflowID: "wf-test",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "* * * * *",
		},
	}
	data, _ := json.Marshal(def)
	trigKV.Put("trig-test", data)

	// Positive: disable flips Enabled to false
	runTriggerDisableCmd([]string{"trig-test"})

	entry, err := trigKV.Get("trig-test")
	if err != nil {
		t.Fatalf("Get after disable failed: %v", err)
	}
	var disabled trigger.TriggerDef
	json.Unmarshal(entry.Value(), &disabled)
	if disabled.Enabled {
		t.Fatal("expected Enabled=false after disable")
	}

	// Positive: enable flips Enabled back to true
	runTriggerEnableCmd([]string{"trig-test"})

	entry, err = trigKV.Get("trig-test")
	if err != nil {
		t.Fatalf("Get after enable failed: %v", err)
	}
	var enabled trigger.TriggerDef
	json.Unmarshal(entry.Value(), &enabled)
	if !enabled.Enabled {
		t.Fatal("expected Enabled=true after enable")
	}

	// Negative: no extra keys were created
	keys, _ := trigKV.Keys()
	if len(keys) != 1 {
		t.Fatalf("expected 1 key, got %d", len(keys))
	}
}

func TestTriggerDeleteRemovesFromKV(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	// Store a trigger
	def := trigger.TriggerDef{
		ID:         "trig-delete",
		WorkflowID: "wf-test",
		Enabled:    true,
		Cron:       &trigger.CronConfig{Expression: "* * * * *"},
	}
	data, _ := json.Marshal(def)
	trigKV.Put("trig-delete", data)

	// Positive: trigger exists before delete
	_, err := trigKV.Get("trig-delete")
	if err != nil {
		t.Fatal("trigger should exist before delete")
	}

	// Run trigger delete command
	runTriggerDeleteCmd([]string{"trig-delete"})

	// Positive: trigger should be removed
	_, err = trigKV.Get("trig-delete")
	if err == nil {
		t.Fatal("trigger should be deleted")
	}

	// Negative: KV should be empty
	keys, _ := trigKV.Keys()
	if len(keys) != 0 {
		t.Fatalf("expected 0 triggers after delete, got %d", len(keys))
	}
}

func TestTriggerCreateJSON(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	var buf bytes.Buffer
	runTriggerCreateCmdWithWriter(
		[]string{
			"test-workflow",
			"--cron=* * * * *",
			"--json",
		},
		&buf,
	)

	// Positive: output is valid JSON with trigger_id
	var result map[string]string
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if result["trigger_id"] == "" {
		t.Fatal("trigger_id should not be empty")
	}

	// Negative: output should not contain human text
	if bytes.Contains(buf.Bytes(), []byte("Trigger created")) {
		t.Fatal("JSON mode should not contain human text")
	}
}

func TestTriggerListJSON(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	def := trigger.TriggerDef{
		ID:         "trig-json-1",
		WorkflowID: "wf-1",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "0 0 * * *",
			Timezone:   "UTC",
		},
	}
	data, _ := json.Marshal(def)
	trigKV.Put("trig-json-1", data)

	var buf bytes.Buffer
	runTriggerListCmdWithWriter([]string{"--json"}, &buf)

	// Positive: output is valid JSON array
	var defs []trigger.TriggerDef
	if err := json.Unmarshal(buf.Bytes(), &defs); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if len(defs) != 1 {
		t.Fatalf("expected 1 trigger, got %d", len(defs))
	}

	// Negative: output should not contain table headers
	if bytes.Contains(buf.Bytes(), []byte("WORKFLOW")) {
		t.Fatal("JSON mode should not contain table headers")
	}
}

func TestTriggerDeleteJSON(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	def := trigger.TriggerDef{
		ID: "trig-del", WorkflowID: "wf",
		Enabled: true,
		Cron:    &trigger.CronConfig{Expression: "* * * * *"},
	}
	data, _ := json.Marshal(def)
	trigKV.Put("trig-del", data)

	var buf bytes.Buffer
	runTriggerDeleteCmdWithWriter(
		[]string{"trig-del", "--json"}, &buf,
	)

	// Positive: valid JSON with action=deleted
	var result triggerActionResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if result.Action != "deleted" {
		t.Fatalf("expected action=deleted, got %s", result.Action)
	}

	// Negative: trigger_id should match
	if result.TriggerID != "trig-del" {
		t.Fatalf(
			"expected trigger_id=trig-del, got %s",
			result.TriggerID,
		)
	}
}

func TestTriggerEnableJSON(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	def := trigger.TriggerDef{
		ID: "trig-en", WorkflowID: "wf",
		Enabled: false,
		Cron:    &trigger.CronConfig{Expression: "* * * * *"},
	}
	data, _ := json.Marshal(def)
	trigKV.Put("trig-en", data)

	var buf bytes.Buffer
	runTriggerEnableCmdWithWriter(
		[]string{"trig-en", "--json"}, &buf,
	)

	// Positive: valid JSON with action=enabled
	var result triggerActionResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if result.Action != "enabled" {
		t.Fatalf("expected action=enabled, got %s", result.Action)
	}

	// Negative: no human text in output
	if bytes.Contains(buf.Bytes(), []byte("Trigger enabled")) {
		t.Fatal("JSON mode should not contain human text")
	}
}

func TestTriggerCreateWebhookSecretFromEnv(t *testing.T) {
	// Positive: env var is used when --secret not provided.
	t.Setenv("DAGNATS_WEBHOOK_SECRET", "env-secret-123")

	def := parseTriggerCreateFlags([]string{
		"test-workflow",
		"--webhook=/hooks/test",
	})
	if def == nil {
		t.Fatal("expected non-nil def")
	}
	if def.Webhook == nil {
		t.Fatal("Webhook config should not be nil")
	}
	if def.Webhook.Secret != "env-secret-123" {
		t.Fatalf(
			"expected secret env-secret-123, got %q",
			def.Webhook.Secret,
		)
	}

	// Negative: --secret flag overrides env var.
	def2 := parseTriggerCreateFlags([]string{
		"test-workflow",
		"--webhook=/hooks/test",
		"--secret=flag-secret",
	})
	if def2 == nil {
		t.Fatal("expected non-nil def")
	}
	if def2.Webhook.Secret != "flag-secret" {
		t.Fatalf(
			"expected secret flag-secret, got %q",
			def2.Webhook.Secret,
		)
	}
}

func TestTriggerDisableJSON(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc,
		natsutil.WithKVBuckets(
			natsutil.KVConfig{Bucket: "triggers"},
		),
	); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}

	t.Setenv("NATS_URL", srv.ClientURL())

	js, _ := nc.JetStream()
	trigKV, _ := js.KeyValue("triggers")

	def := trigger.TriggerDef{
		ID: "trig-dis", WorkflowID: "wf",
		Enabled: true,
		Cron:    &trigger.CronConfig{Expression: "* * * * *"},
	}
	data, _ := json.Marshal(def)
	trigKV.Put("trig-dis", data)

	var buf bytes.Buffer
	runTriggerDisableCmdWithWriter(
		[]string{"trig-dis", "--json"}, &buf,
	)

	// Positive: valid JSON with action=disabled
	var result triggerActionResult
	if err := json.Unmarshal(buf.Bytes(), &result); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if result.Action != "disabled" {
		t.Fatalf("expected action=disabled, got %s", result.Action)
	}

	// Negative: trigger_id should match
	if result.TriggerID != "trig-dis" {
		t.Fatalf(
			"expected trigger_id=trig-dis, got %s",
			result.TriggerID,
		)
	}
}

// TestTriggerTypeConfig is a pure-function table test covering each
// trigger kind. The list command's TYPE and CONFIG columns flow from
// this; if a new trigger kind lands without a case here, the CLI
// quietly renders "unknown" (the bug that motivated this test).
func TestTriggerTypeConfig(t *testing.T) {
	cases := []struct {
		name     string
		def      trigger.TriggerDef
		wantKind string
		wantCfg  string
	}{
		{
			name: "cron",
			def: trigger.TriggerDef{
				Cron: &trigger.CronConfig{Expression: "*/5 * * * *"},
			},
			wantKind: "cron",
			wantCfg:  "*/5 * * * *",
		},
		{
			name: "subject",
			def: trigger.TriggerDef{
				Subject: &trigger.SubjectConfig{Subject: "events.>"},
			},
			wantKind: "subject",
			wantCfg:  "events.>",
		},
		{
			name: "webhook",
			def: trigger.TriggerDef{
				Webhook: &trigger.WebhookConfig{Path: "/hooks/x"},
			},
			wantKind: "webhook",
			wantCfg:  "/hooks/x",
		},
		{
			name: "http",
			def: trigger.TriggerDef{
				HTTP: &trigger.HTTPConfig{
					Method: "POST",
					Path:   "/api/echo",
				},
			},
			wantKind: "http",
			wantCfg:  "POST /api/echo",
		},
		{
			name:     "unknown when no variant set",
			def:      trigger.TriggerDef{},
			wantKind: "unknown",
			wantCfg:  "",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotKind, gotCfg := triggerTypeConfig(tc.def)
			// Positive: kind matches.
			if gotKind != tc.wantKind {
				t.Fatalf("kind: want %q got %q",
					tc.wantKind, gotKind)
			}
			// Negative: config matches the kind, not some other variant's.
			if gotCfg != tc.wantCfg {
				t.Fatalf("config: want %q got %q",
					tc.wantCfg, gotCfg)
			}
		})
	}
}
