// cli/workflow_show_test.go
// Integration tests for workflow show command. Uses embedded NATS to
// register a workflow then verifies show output contains expected fields.
// Methodology: register a known workflow, capture stdout from the show
// command, and assert on name, step count, and step IDs in the output.
package cli

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/danmestas/dagnats/api"
	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/natsutil"
	"github.com/danmestas/dagnats/observe"
)

func TestWorkflowShowDisplaysRegisteredWorkflow(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	defer nc.Close()

	// Point CLI at the test NATS server.
	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	// Register a workflow via the API so show can retrieve it.
	svc := api.NewService(nc, observe.NewNoopTelemetry())
	def := dag.WorkflowDef{
		Name:    "show-test",
		Version: "2.0",
		Steps: []dag.StepDef{
			{ID: "greet", Task: "greet", Timeout: time.Second},
			{
				ID:        "uppercase",
				Task:      "uppercase",
				DependsOn: []string{"greet"},
				Timeout:   time.Second,
			},
		},
	}
	err := svc.RegisterWorkflow(context.Background(), def)
	if err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	output := captureOutput(func() {
		runWorkflowShowCmd([]string{"show-test"})
	})

	// Positive: output must contain the workflow name and version.
	if !strings.Contains(output, "show-test") {
		t.Fatalf(
			"expected output to contain workflow name, got: %s",
			output,
		)
	}
	if !strings.Contains(output, "2.0") {
		t.Fatalf(
			"expected output to contain version, got: %s",
			output,
		)
	}

	// Positive: output must contain step count.
	if !strings.Contains(output, "2") {
		t.Fatalf(
			"expected output to contain step count 2, got: %s",
			output,
		)
	}

	// Positive: output must contain both step IDs.
	if !strings.Contains(output, "greet") {
		t.Fatalf(
			"expected output to contain step 'greet', got: %s",
			output,
		)
	}
	if !strings.Contains(output, "uppercase") {
		t.Fatalf(
			"expected output to contain step 'uppercase', got: %s",
			output,
		)
	}

	// Negative: output must not contain unrelated workflows.
	if strings.Contains(output, "unknown-wf") {
		t.Fatal("output should not contain 'unknown-wf'")
	}
}

func TestWorkflowShowDisplaysTimeout(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	defer nc.Close()

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	svc := api.NewService(nc, observe.NewNoopTelemetry())
	def := dag.WorkflowDef{
		Name:    "timeout-test",
		Version: "1.0",
		Timeout: 30 * time.Second,
		Steps: []dag.StepDef{
			{ID: "step1", Task: "task1", Timeout: time.Second},
		},
	}
	err := svc.RegisterWorkflow(context.Background(), def)
	if err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	output := captureOutput(func() {
		runWorkflowShowCmd([]string{"timeout-test"})
	})

	// Positive: output must show the timeout duration.
	if !strings.Contains(output, "30s") {
		t.Fatalf(
			"expected output to contain '30s', got: %s",
			output,
		)
	}

	// Negative: must not say "none" when timeout is set.
	if strings.Contains(output, "none") {
		t.Fatal(
			"output should not contain 'none' when timeout is set",
		)
	}
}

func TestWorkflowShowJSON(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll failed: %v", err)
	}
	defer nc.Close()

	oldURL := os.Getenv("NATS_URL")
	os.Setenv("NATS_URL", srv.ClientURL())
	defer os.Setenv("NATS_URL", oldURL)

	svc := api.NewService(nc, observe.NewNoopTelemetry())
	def := dag.WorkflowDef{
		Name:    "json-show",
		Version: "3.0",
		Steps: []dag.StepDef{
			{ID: "s1", Task: "task1", Timeout: time.Second},
		},
	}
	err := svc.RegisterWorkflow(context.Background(), def)
	if err != nil {
		t.Fatalf("register workflow: %v", err)
	}

	output := captureOutput(func() {
		runWorkflowShowCmd([]string{"json-show", "--json"})
	})

	var got dag.WorkflowDef
	if err := json.Unmarshal([]byte(output), &got); err != nil {
		t.Fatalf(
			"unmarshal json: %v (output: %s)", err, output,
		)
	}

	// Positive: name and version must match.
	if got.Name != "json-show" {
		t.Fatalf("expected name 'json-show', got %q", got.Name)
	}
	if got.Version != "3.0" {
		t.Fatalf("expected version '3.0', got %q", got.Version)
	}

	// Positive: must have the correct step count.
	if len(got.Steps) != 1 {
		t.Fatalf("expected 1 step, got %d", len(got.Steps))
	}

	// Negative: output must not contain table headers.
	if strings.Contains(output, "TASK") {
		t.Fatal("json output should not contain table headers")
	}
}
