// cli/workflow_test.go
// Tests for workflow CLI commands: list, register.
// Methodology: integration tests with embedded NATS. Verify output
// reflects workflow state in KV.
package cli

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/natsutil"
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
