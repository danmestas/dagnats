// cli/trigger_update_test.go
// Tests for trigger update CLI command.
// Methodology: integration tests with embedded NATS to verify KV updates.
package cli

import (
	"bytes"
	"encoding/json"
	"os"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/trigger"
)

func TestTriggerUpdateCronExpression(t *testing.T) {
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

	// Seed a cron trigger with initial expression
	def := trigger.TriggerDef{
		ID:         "trig-upd-cron",
		WorkflowID: "wf-test",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
		},
	}
	data, _ := json.Marshal(def)
	trigKV.Put("trig-upd-cron", data)

	// Update the cron expression
	var buf bytes.Buffer
	runTriggerUpdateCmdWithWriter(
		[]string{"trig-upd-cron", "--cron=0 0 * * *"},
		&buf,
	)

	// Positive: cron expression should be updated in KV
	entry, err := trigKV.Get("trig-upd-cron")
	if err != nil {
		t.Fatalf("trigger not found after update: %v", err)
	}
	var updated trigger.TriggerDef
	if err := json.Unmarshal(
		entry.Value(), &updated,
	); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if updated.Cron.Expression != "0 0 * * *" {
		t.Fatalf(
			"expected cron '0 0 * * *', got %s",
			updated.Cron.Expression,
		)
	}

	// Negative: timezone should remain unchanged
	if updated.Cron.Timezone != "UTC" {
		t.Fatalf(
			"expected timezone UTC, got %s",
			updated.Cron.Timezone,
		)
	}
}

func TestTriggerUpdateTimezone(t *testing.T) {
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

	// Seed a cron trigger
	def := trigger.TriggerDef{
		ID:         "trig-upd-tz",
		WorkflowID: "wf-test",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "0 0 * * *",
			Timezone:   "UTC",
		},
	}
	data, _ := json.Marshal(def)
	trigKV.Put("trig-upd-tz", data)

	// Update the timezone
	var buf bytes.Buffer
	runTriggerUpdateCmdWithWriter(
		[]string{
			"trig-upd-tz",
			"--tz=America/New_York",
		},
		&buf,
	)

	// Positive: timezone should be updated
	entry, err := trigKV.Get("trig-upd-tz")
	if err != nil {
		t.Fatalf("trigger not found after update: %v", err)
	}
	var updated trigger.TriggerDef
	if err := json.Unmarshal(
		entry.Value(), &updated,
	); err != nil {
		t.Fatalf("unmarshal failed: %v", err)
	}
	if updated.Cron.Timezone != "America/New_York" {
		t.Fatalf(
			"expected timezone America/New_York, got %s",
			updated.Cron.Timezone,
		)
	}

	// Negative: expression should remain unchanged
	if updated.Cron.Expression != "0 0 * * *" {
		t.Fatalf(
			"expected cron '0 0 * * *', got %s",
			updated.Cron.Expression,
		)
	}
}

func TestTriggerUpdateNonExistent(t *testing.T) {
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

	// Attempt to update a non-existent trigger via the service
	// directly, since the CLI calls os.Exit on error.
	svc, svcNC := connectService()
	defer svcNC.Close()

	cronExpr := "0 0 * * *"
	err := svc.UpdateTrigger(
		t.Context(), "trig-nonexistent",
		api.TriggerUpdates{CronExpr: &cronExpr},
	)

	// Positive: should return an error
	if err == nil {
		t.Fatal("expected error for non-existent trigger")
	}

	// Negative: error should mention the trigger ID
	if !strings.Contains(
		err.Error(), "trig-nonexistent",
	) {
		t.Fatalf(
			"error should mention trigger ID, got: %s",
			err.Error(),
		)
	}
}

func TestTriggerUpdateJSON(t *testing.T) {
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

	def := trigger.TriggerDef{
		ID:         "trig-upd-json",
		WorkflowID: "wf-test",
		Enabled:    true,
		Cron: &trigger.CronConfig{
			Expression: "* * * * *",
			Timezone:   "UTC",
		},
	}
	data, _ := json.Marshal(def)
	trigKV.Put("trig-upd-json", data)

	var buf bytes.Buffer
	runTriggerUpdateCmdWithWriter(
		[]string{
			"trig-upd-json",
			"--cron=0 0 * * *",
			"--json",
		},
		&buf,
	)

	// Positive: valid JSON with action=updated
	var result triggerActionResult
	if err := json.Unmarshal(
		buf.Bytes(), &result,
	); err != nil {
		t.Fatalf("invalid JSON output: %v", err)
	}
	if result.Action != "updated" {
		t.Fatalf(
			"expected action=updated, got %s", result.Action,
		)
	}

	// Negative: no human text in JSON output
	if bytes.Contains(
		buf.Bytes(), []byte("Trigger updated"),
	) {
		t.Fatal("JSON mode should not contain human text")
	}
}
