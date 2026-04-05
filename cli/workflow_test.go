// cli/workflow_test.go
// Tests for workflow CLI commands: list, register.
// Methodology: integration tests with embedded NATS. Verify output
// reflects workflow state in KV.
package cli

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestWorkflowRegisterShowsCreatedVsUpdated(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	defer nc.Close()

	// Point CLI at test server
	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	// Write a workflow definition with two steps
	wfJSON := `{
		"name":"test-wf",
		"version":"1.0",
		"steps":[
			{"id":"a","task":"task-a"},
			{"id":"b","task":"task-b","depends_on":["a"]}
		]
	}`
	tmpFile := filepath.Join(t.TempDir(), "wf.json")
	if err := os.WriteFile(
		tmpFile, []byte(wfJSON), 0644,
	); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	// First register: should report "created"
	firstOutput := captureOutput(func() {
		runWorkflowRegisterCmd([]string{tmpFile})
	})

	if !strings.Contains(firstOutput, "created") {
		t.Fatalf(
			"first register should say created, got: %s",
			firstOutput,
		)
	}
	if !strings.Contains(firstOutput, "2 steps") {
		t.Fatalf(
			"first register should show step count, got: %s",
			firstOutput,
		)
	}

	// Negative: first register must not say "updated"
	if strings.Contains(firstOutput, "updated") {
		t.Fatal(
			"first register should not say updated",
		)
	}

	// Second register: should report "updated"
	secondOutput := captureOutput(func() {
		runWorkflowRegisterCmd([]string{tmpFile})
	})

	if !strings.Contains(secondOutput, "updated") {
		t.Fatalf(
			"second register should say updated, got: %s",
			secondOutput,
		)
	}
	if !strings.Contains(secondOutput, "2 steps") {
		t.Fatalf(
			"second register should show step count, got: %s",
			secondOutput,
		)
	}

	// Negative: second register must not say "created"
	if strings.Contains(secondOutput, "created") {
		t.Fatal(
			"second register should not say created",
		)
	}
}

func TestWorkflowRegisterJSON(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	defer nc.Close()

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	wfJSON := `{
		"name":"json-reg",
		"version":"1.0",
		"steps":[
			{"id":"a","task":"task-a"},
			{"id":"b","task":"task-b","depends_on":["a"]}
		]
	}`
	tmpFile := filepath.Join(t.TempDir(), "wf.json")
	if err := os.WriteFile(
		tmpFile, []byte(wfJSON), 0644,
	); err != nil {
		t.Fatalf("write temp file: %v", err)
	}

	output := captureOutput(func() {
		runWorkflowRegisterCmd(
			[]string{tmpFile, "--json"},
		)
	})

	var result workflowRegisterResult
	if err := json.Unmarshal(
		[]byte(output), &result,
	); err != nil {
		t.Fatalf(
			"unmarshal json: %v (output: %s)", err, output,
		)
	}

	// Positive: name and action must be correct.
	if result.Name != "json-reg" {
		t.Fatalf(
			"expected name 'json-reg', got %q", result.Name,
		)
	}
	if result.Action != "created" {
		t.Fatalf(
			"expected action 'created', got %q", result.Action,
		)
	}

	// Positive: step count must match.
	if result.Steps != 2 {
		t.Fatalf("expected 2 steps, got %d", result.Steps)
	}

	// Negative: output must not contain human text.
	if strings.Contains(output, "Workflow") {
		t.Fatal("json output should not contain human text")
	}
}

func TestWorkflowListJSON(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	defer nc.Close()

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	// Register a workflow via the API so list can find it.
	svc := api.NewService(nc, observe.NewNoopTelemetry())
	def := dag.WorkflowDef{
		Name:    "list-json",
		Version: "1.0",
		Timeout: 10 * time.Second,
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task1", Timeout: time.Second},
		},
	}
	err := svc.RegisterWorkflow(context.Background(), def)
	if err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	output := captureOutput(func() {
		runWorkflowListCmd([]string{"--json"})
	})

	var defs []dag.WorkflowDef
	if err := json.Unmarshal(
		[]byte(output), &defs,
	); err != nil {
		t.Fatalf(
			"unmarshal json: %v (output: %s)", err, output,
		)
	}

	// Positive: must contain at least one workflow.
	if len(defs) == 0 {
		t.Fatal("expected at least one workflow in list")
	}

	// Positive: first workflow must have correct name.
	if defs[0].Name != "list-json" {
		t.Fatalf(
			"expected name 'list-json', got %q", defs[0].Name,
		)
	}

	// Negative: output must not contain table headers.
	if strings.Contains(output, "NAME") {
		t.Fatal("json output should not contain table headers")
	}
}
