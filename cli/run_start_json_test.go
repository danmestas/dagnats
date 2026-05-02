// cli/run_start_json_test.go
// Regression guard for #143: `dagnats run start <wf> '{}' --json` must emit
// parseable JSON on stdout and nothing else. End-to-end: real NATS, real
// service, real CLI dispatch — captures stdout exactly as a shell pipe would.
package cli

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/danmestas/dagnats/dag"
	"github.com/danmestas/dagnats/internal/api"
	"github.com/danmestas/dagnats/internal/natsutil"
)

func TestRunStart_JSONFlag_StdoutIsParseableJSON(t *testing.T) {
	srv, nc := natsutil.StartTestServer(t)
	if err := natsutil.SetupAll(nc); err != nil {
		t.Fatalf("SetupAll: %v", err)
	}
	t.Setenv("NATS_URL", srv.ClientURL())

	svc := api.NewService(nc)
	wb := dag.NewWorkflow("json-test-wf")
	wb.Task("noop", "noop-task")
	wfDef, err := wb.Build()
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if err := svc.RegisterWorkflow(context.Background(), wfDef); err != nil {
		t.Fatalf("RegisterWorkflow: %v", err)
	}

	stdout := captureOutput(func() {
		runStartCmd([]string{"json-test-wf", "{}", "--json"})
	})

	var parsed map[string]any
	if err := json.Unmarshal([]byte(stdout), &parsed); err != nil {
		t.Fatalf("stdout is not parseable JSON: %v\nGot:\n%s", err, stdout)
	}
	if _, ok := parsed["run_id"].(string); !ok {
		t.Fatalf("expected run_id field in JSON, got: %v", parsed)
	}

	if strings.Contains(stdout, "Started:") {
		t.Fatalf("stdout must not contain human-readable text under --json; got:\n%s", stdout)
	}
	if strings.Contains(stdout, "Hint:") {
		t.Fatalf("stdout must not contain hint under --json; got:\n%s", stdout)
	}
}
