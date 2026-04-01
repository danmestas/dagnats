// cli/trigger_test.go
// Tests for trigger CLI commands: create, list, delete.
// Methodology: integration tests with embedded NATS to verify KV operations.
package cli

import (
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/trigger"
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
	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

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

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

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

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

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

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

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

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

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
