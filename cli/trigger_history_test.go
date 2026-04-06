// cli/trigger_history_test.go
// Tests for trigger fire history parsing and display.
// Methodology: publish TriggerFire records to an embedded NATS
// server and verify they can be retrieved and parsed.
package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/internal/trigger"
	"github.com/nats-io/nats.go/jetstream"
)

func TestTriggerHistoryParsesFires(t *testing.T) {
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

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	// Publish a TriggerFire record to the history stream.
	fire := trigger.TriggerFire{
		TriggerID:  "test-trig",
		WorkflowID: "test-wf",
		RunID:      "run-123",
		Source:     "cron",
		FiredAt:    time.Now().UTC().Truncate(time.Second),
	}
	fireBytes, err := json.Marshal(fire)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	_, err = js.Publish(
		context.Background(),
		"trigger.fire.test-trig",
		fireBytes,
	)
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	// Use the API service to retrieve the fire records.
	svc := api.NewService(nc)
	fires, err := svc.ListTriggerFires(
		context.Background(), "test-trig", 10,
	)
	if err != nil {
		t.Fatalf("ListTriggerFires: %v", err)
	}

	// Positive: should retrieve exactly one fire record.
	if len(fires) != 1 {
		t.Fatalf("expected 1 fire, got %d", len(fires))
	}

	// Positive: the fire record should match what we published.
	if fires[0].TriggerID != "test-trig" {
		t.Fatalf(
			"expected trigger_id=test-trig, got %s",
			fires[0].TriggerID,
		)
	}

	// Negative: a non-existent trigger should return zero fires.
	empty, err := svc.ListTriggerFires(
		context.Background(), "no-such-trigger", 10,
	)
	if err != nil {
		t.Fatalf("ListTriggerFires empty: %v", err)
	}
	if len(empty) != 0 {
		t.Fatalf(
			"expected 0 fires for missing trigger, got %d",
			len(empty),
		)
	}

	// Verify JSON output format.
	var buf bytes.Buffer
	runTriggerHistoryCmdWithWriter(
		[]string{"test-trig", "--json"}, &buf, srv.ClientURL(),
	)

	var jsonFires []api.TriggerFireEntry
	if err := json.Unmarshal(
		buf.Bytes(), &jsonFires,
	); err != nil {
		t.Fatalf("invalid JSON output: %v\n%s", err, buf.String())
	}
	if len(jsonFires) != 1 {
		t.Fatalf(
			"expected 1 JSON fire entry, got %d",
			len(jsonFires),
		)
	}
}
